import socket
from typing import Tuple
from queue import Queue
import ssl
from constants import *

class Socket(socket.socket):
    pass

class SocketConnection:

    def __init__(
        self,
        sock: Socket,
        ssl_sock: ssl.SSLSocket,
        ip_address: str,
        port: int,
        timeoutms: int,
        is_connected: bool = True,
        root_cert: str = None,
        cert: str = None,
        cert_key: str = None,
    ) -> None:
        self.sock: Socket = sock
        self.ssock: ssl.SSLSocket = ssl_sock
        self.is_connected: bool = is_connected
        self.ip_address: str = ip_address
        self.port: int = port
        self.timeoutms: int = timeoutms
        self.has_ssl_connection = self.ssock is not None
        self.root_cert: str = root_cert
        self.cert: str = cert
        self.cert_key: str = cert_key

    def reconnect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.ip_address, self.port))

        if self.timeoutms is not None:
            self.sock.settimeout(self.timeoutms / 1000)

        if self.has_ssl_connection:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

            context.load_verify_locations(self.root_cert)

            if self.cert and self.cert_key:
                context.load_cert_chain(
                    certfile=self.cert,
                    keyfile=self.cert_key,
                )

            self.ssock = context.wrap_socket(self.sock, server_hostname=self.ip_address)

            if self.ssock.getpeercert() is None:
                raise ssl.SSLError("Failed to retrieve server certificate")

    def close(self):
        try:
            (self.sock if not self.has_ssl_connection else self.ssock).close()
        except Exception as e:
            print(f"Could not close socket connection. {e}")
        finally:
            self.is_connected = False

    def send_bytes(self, req: bytes):
        (self.sock if not self.has_ssl_connection else self.ssock).sendall(req)

    def receive_bytes(self) -> bytes:
        return (self.sock if not self.has_ssl_connection else self.ssock).recv(1024)


class SocketClientConf:

    def __init__(
        self,
        retries: int = 0,
        timeoutms: int = None,
        use_https: bool = False,
        root_cert: str = None,
        cert: str = None,
        cert_key: str = None,
    ) -> None:
        self.retries:int = retries
        self.timeoutms = timeoutms
        self.use_https: bool = use_https
        self.root_cert: str = root_cert
        self.cert: str = cert
        self.cert_key: str = cert_key


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

            context.load_verify_locations(self.conf.root_cert)

            if self.conf.cert and self.conf.cert_key:
                context.load_cert_chain(
                    certfile=self.conf.cert,
                    keyfile=self.conf.cert_key,
                )

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
