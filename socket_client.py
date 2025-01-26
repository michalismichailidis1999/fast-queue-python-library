import socket
from typing import Tuple
from queue import Queue
import ssl
from constants import *
from exceptions import RetryableException

class SocketClientConf:

    def __init__(
        self,
        timeoutms: int = None,
        ssl_enable: bool = False,
        root_cert: str = None,
        cert: str = None,
        cert_key: str = None,
        sasl_enable: bool = False,
        sasl_auth_method: str = None,
        sasl_username: str = None,
        sasl_password: str = None,
    ) -> None:
        self.timeoutms = timeoutms
        self.ssl_enable: bool = ssl_enable
        self.root_cert: str = root_cert
        self.cert: str = cert
        self.cert_key: str = cert_key
        self.sasl_enable: bool = sasl_enable
        self.sasl_auth_method: str = sasl_auth_method
        self.sasl_username: str = sasl_username
        self.sasl_password: str = sasl_password


class Socket(socket.socket):
    pass

class SocketConnection:

    def __init__(
        self,
        sock: Socket,
        ssl_sock: ssl.SSLSocket,
        address: str,
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
        self.address: str = address
        self.port: int = port
        self.timeoutms: int = timeoutms
        self.has_ssl_connection = self.ssock is not None
        self.root_cert: str = root_cert
        self.cert: str = cert
        self.cert_key: str = cert_key

    def reconnect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.address, self.port))

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

            self.ssock = context.wrap_socket(self.sock, server_hostname=self.address)

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
        res_size = (self.sock if not self.has_ssl_connection else self.ssock).recv(8)

        if res_size <= 0:
            raise Exception("Error occurred while trying to read bytes from socket")

        return (self.sock if not self.has_ssl_connection else self.ssock).recv(res_size)

class SocketClient:

    def __init__(self, address: str, port: int, conf: SocketClientConf) -> None:
        self.pool: Queue[SocketConnection] = Queue()
        self.conf = conf
        self.add_connection(ip_address=address, port=port)

    def add_connection(self, address: str, port: int):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((address, port))

        if self.conf.timeoutms is not None:
            sock.settimeout(self.conf.timeoutms / 1000)

        ssock: ssl.SSLSocket = None

        if self.conf.ssl_enable:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

            context.load_verify_locations(self.conf.root_cert)

            if self.conf.cert and self.conf.cert_key:
                context.load_cert_chain(
                    certfile=self.conf.cert,
                    keyfile=self.conf.cert_key,
                )

            ssock = context.wrap_socket(sock, server_hostname=address)

            if ssock.getpeercert() is None:
                raise ssl.SSLError("Failed to retrieve server certificate")

        conn: SocketConnection = SocketConnection(
            sock=sock,
            ssl_sock=ssock,
            address=address,
            port=port,
            timeoutms=self.conf.timeoutms,
            is_connected=True,
        )

        self.pool.put(conn)

    def send_request(self, req: bytes) -> bytes:
        conn: SocketConnection = None

        try:
            conn = self.pool.get(
                block=True, timeout=int(self.conf.timeoutms / 1000) + 1
            )
        except:
            raise RetryableException(
                error_code=CONNECTION_ERROR,
                error_message="No available connection to send request",
            )

        if not conn.is_connected:
            reconnected = self.try_reconnect(conn=conn)

            if not reconnected:
                raise RetryableException(
                    error_code=CONNECTION_ERROR,
                    error_message="Could not reconnect to broker",
                )

        try:
            # Send the message to the server
            conn.send_bytes(req)

            # Wait for the response from the server
            response = conn.receive_bytes()

            if not response or len(response) == 0:
                conn.close()
                self.pool.put(conn)
                raise RetryableException(
                    error_code=CONNECTION_ERROR,
                    error_message="Could not receive response from the socket connection",
                )

            response_err = self.get_response_error(response)

            if response_err[0] != NO_ERROR:
                self.pool.put(conn)

                if self.__is_error_retryable(response_err[0]):
                    raise RetryableException(
                        error_code=response_err[0], error_message=response_err[1]
                    )

                raise Exception(response_err[1])

            return response
        except TimeoutError:
            self.pool.put(conn)
            raise RetryableException(
                error_code=TIMEOUT_ERROR,
                error_message="Waiting for response timed out",
            )
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

    def __is_error_retryable(self, error_code: int):
        return error_code in [CONNECTION_ERROR, TIMEOUT_ERROR]

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
