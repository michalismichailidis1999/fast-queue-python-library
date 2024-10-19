import socket
from typing import Any, Dict, Tuple

import OpenSSL
from constants import *
from queue import Queue
import ssl

class Socket(socket.socket):
    pass

def load_p12_and_verify(context: ssl.SSLContext, path: str, password: str = None):
    try:
        # Load the PKCS#12 file
        with open(path, "rb") as f:
            p12 = OpenSSL.crypto.load_pkcs12(
                f.read(), password.encode() if password else None
            )

            # Extract the client certificate and private key
            client_cert = p12.get_certificate()
            private_key = p12.get_privatekey()

            # Check for CA certificates
            ca_certs = p12.get_ca_certificates()

            if not client_cert:
                raise Exception("No client certificate found in PKCS#12 file.")

            # Load the client certificate and private key into the SSL context
            context.load_cert_chain(
                certfile=OpenSSL.crypto.dump_certificate(
                    OpenSSL.crypto.FILETYPE_PEM, client_cert
                ).decode("utf-8"),
                keyfile=OpenSSL.crypto.dump_privatekey(
                    OpenSSL.crypto.FILETYPE_PEM, private_key
                ).decode("utf-8"),
            )

            print("here")

            # Load CA certificates into the context
            if ca_certs:
                for ca_cert in ca_certs:
                    context.load_verify_locations(
                        cadata=OpenSSL.crypto.dump_certificate(
                            OpenSSL.crypto.FILETYPE_PEM, ca_cert
                        ).decode("utf-8")
                    )
            else:
                raise Exception("No CA certificates found in PKCS#12 file.")
    except Exception as e:
        raise Exception(f"Could not load certificate. {e}")


class SocketConnection:

    def __init__(
        self,
        sock: Socket,
        ssl_sock: ssl.SSLSocket,
        ip_address: str,
        port: int,
        timeoutms: int,
        is_connected: bool = True,
    ) -> None:
        self.sock: Socket = sock
        self.ssock: ssl.SSLSocket = ssl_sock
        self.is_connected: bool = is_connected
        self.ip_address: str = ip_address
        self.port: int = port
        self.timeoutms: int = timeoutms
        self.has_ssl_connection = self.ssock is not None

    def reconnect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.ip_address, self.port))

        if self.timeoutms is not None:
            self.sock.settimeout(self.timeoutms / 1000)

        if self.has_ssl_connection:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            self.ssock = context.wrap_socket(self.sock, server_hostname=self.ip_address)

            cert = self.ssock.getpeercert()

            if self.ssock.getpeercert() is None:
                raise ssl.SSLError("Failed to retrieve server certificate")

    def close(self):
        try:
            self.ssock.close()
        except Exception as e:
            print(f"Could not close socket connection. {e}")
        finally:
            self.is_connected = False

    def send_bytes(self, req: bytes):
        self.sock.sendall(req)

    def receive_bytes(self) -> bytes:
        return self.sock.recv(1024)


class SocketClientConf:

    def __init__(
        self, retries: int = 0, timeoutms: int = None, use_https: bool = False
    ) -> None:
        self.retries:int = retries
        self.timeoutms = timeoutms
        self.use_https: bool = use_https


class SocketClient:
    def __init__(self, ip_address:str, port:int, conf:SocketClientConf) -> None:
        self.pool: Queue[SocketConnection] = Queue()
        self.conf = conf
        self.add_connection(ip_address=ip_address, port=port)

    def add_connection(self, ip_address: str, port: int):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip_address, port))

        if self.conf.timeoutms is not None:
            sock.settimeout(self.conf.timeoutms / 1000)

        ssock: ssl.SSLSocket = None

        if self.conf.use_https:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

            context.load_verify_locations(
                "C:\\Users\\Windows\\.ssh\\message_broker_certs\\ca.crt"
            )

            # use for mutual tls
            # context.load_cert_chain(
            #     certfile="C:\\Users\\Windows\\.ssh\\message_broker_certs\\client.crt",
            #     keyfile="C:\\Users\\Windows\\.ssh\\message_broker_certs\\client.key",
            # )

            ssock = context.wrap_socket(sock, server_hostname=ip_address)

            if ssock.getpeercert() is None:
                raise ssl.SSLError("Failed to retrieve server certificate")

        conn: SocketConnection = SocketConnection(
            sock=sock,
            ssl_sock=ssock,
            ip_address=ip_address,
            port=port,
            timeoutms=self.conf.timeoutms,
            is_connected=True,
        )

        self.pool.put(conn)

    def send_request(self, req: bytes) -> bytes:
        conn = self.pool.get()

        if not conn.is_connected:
            reconnected = self.try_reconnect(conn=conn)

            if not reconnected:
                raise Exception("Could not connect to broker")

        retries = 0

        while retries < self.conf.retries:
            try:
                if retries >= 1:
                    print("Request failed. Retrying again.")

                if not conn.is_connected:
                    reconnected = self.try_reconnect(conn=conn)

                    if not reconnected: return [True, "Could not connect to broker", None]

                # Send the message to the server
                conn.send_bytes(req)

                # Wait for the response from the server
                response = conn.receive_bytes()

                if not response:
                    conn.close()
                    self.pool.put(conn)
                    raise Exception("Connection to broker shutdown unexpectedly")

                response_err = self.get_response_error(response)

                if response_err[0] != NO_ERROR:
                    if not self.is_error_retryable(response_err[0]):
                        self.pool.put(conn)
                        raise Exception(response_err[1])

                    retries += 1

                    continue

                self.pool.put(conn)

                return response
            except TimeoutError:
                retries += 1

                if retries >= self.conf.retries:
                    self.pool.put(conn)
                    raise Exception("Request timed out")
            except ConnectionResetError:
                conn.close()
                self.pool.put(conn)
                raise Exception("Connection to broker shut down unexpectedly")
            except Exception as e:
                conn.close()
                self.pool.put(conn)
                raise e

    def try_reconnect(self, conn: SocketConnection) -> bool:
        try:
            conn.reconnect()
            return True
        except:
            return False

    def get_response_error(self, res:bytes) -> Tuple[int,str]:
        return [
            int.from_bytes(bytes=res[0:1], byteorder="big", signed=False),
            res[4:].decode() if len(res) > 4 else "Internal server error",
        ]

    def is_error_retryable(self, error_type: int) -> bool:
        return error_type in [
            NOT_LEADER_FOR_PARTITION, 
            UNKNOWN_QUEUE_OR_PARTITION, 
            REQUEST_TIMED_OUT
        ]

    def close(self):
        while self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.socket.close()
            except Exception as e:
                print(
                    f"Error occured while trying to close connection to the broker. Exception: {e}"
                )
                break
