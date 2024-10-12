import socket
from typing import Tuple
from constants import *
from queue import Queue
import ssl

class Socket(socket.socket):
    pass


def verify_certificate_chain(connection, cert):
    """
    Performs custom certificate verification.

    Args:
        connection: The SSLSocket object.
        cert: The server's certificate.

    Returns:
        True if the certificate is considered valid, False otherwise.
    """

    # 1. Extract certificate details (e.g., issuer, subject, expiry date)
    #    You can use the OpenSSL library to parse the certificate and
    #    access its fields.

    # 2. Implement your verification logic
    #    - Check if the issuer is in your list of trusted CAs.
    #    - Verify the certificate chain (if necessary).
    #    - Validate the certificate's expiry date.
    #    - Perform any other checks required by your application.

    # Example: Check if the issuer is "My Trusted CA"
    print(cert)
    if cert.get_issuer().CN == "My Trusted CA":
        return True
    else:
        return False


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

    def reconnect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.ip_address, self.port))

        if self.timeoutms is not None:
            self.sock.settimeout(self.timeoutms / 1000)

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        self.ssock = context.wrap_socket(self.sock, server_hostname=self.ip_address)

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
    def __init__(self, retries:int = 0, timeoutms:int = None) -> None:
        self.retries:int = retries
        self.timeoutms = timeoutms

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

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        # Set the custom verification callback
        context.verify_mode = ssl.CERT_REQUIRED
        context.verify_flags = ssl.VERIFY_CRL_CHECK_CHAIN  # Optional: Check CRL
        context.check_hostname = True  # Verify hostname (if applicable)
        context.verify_callback = verify_certificate_chain

        ssock = context.wrap_socket(sock, server_hostname=ip_address)

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
