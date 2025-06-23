from socket import socket, AF_INET, SOCK_STREAM
import time
from queue import Queue
import ssl
import threading
from typing import Tuple
from constants import *
from exceptions import RetryableException, FastQueueException

class SocketClientConf:

    def __init__(
        self,
        timeoutms: int = 0,
        retries: int = 1,
        retry_wait_ms: int = 0,
        ssl_enable: bool = False,
        root_cert: str = None,
        cert: str = None,
        cert_key: str = None,
        cert_pass: str = None,
        sasl_enable: bool = False,
        sasl_username: str = None,
        sasl_password: str = None,
        max_pool_connections: int = 10
    ) -> None:
        if timeoutms < 0:
            raise ValueError("timeoutms cannot be less than 0")
        
        if retries < 1:
            raise ValueError("retries cannot be less than 1")
        
        if retry_wait_ms < 0:
            raise ValueError("retry_wait_ms cannot be less than 0")
        
        if max_pool_connections < 1:
            raise ValueError("max_pool_connections cannot be less than 1")
        
        self.timeoutms = timeoutms
        self.retries: int = retries
        self.retry_wait_ms: int = retry_wait_ms
        self.ssl_enable: bool = ssl_enable
        self.root_cert: str = root_cert
        self.cert: str = cert
        self.cert_key: str = cert_key
        self.cert_pass: str = cert_pass
        self.sasl_enable: bool = sasl_enable
        self.sasl_username: str = sasl_username
        self.sasl_password: str = sasl_password
        self.max_pool_connections: int = max_pool_connections

class SocketConnection:

    def __init__(
        self,
        sock: socket,
        ssl_sock: ssl.SSLSocket,
        address: str,
        port: int,
        timeoutms: int,
        root_cert: str = None,
        cert: str = None,
        cert_key: str = None,
        cert_pass: str = None,
    ) -> None:
        self.sock: socket = sock
        self.ssock: ssl.SSLSocket = ssl_sock
        self.address: str = address
        self.port: int = port
        self.timeoutms: int = timeoutms
        self.has_ssl_connection = self.ssock is not None
        self.root_cert: str = root_cert
        self.cert: str = cert
        self.cert_key: str = cert_key
        self.cert_pass: str = cert_pass
        self.max_fail_retries: int = 2
        self.remaining_fails: int = self.max_fail_retries

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
        res_size = (self.sock if not self.has_ssl_connection else self.ssock).recv(
            LONG_SIZE
        )

        if len(res_size) <= 0:
            raise Exception("Error occurred while trying to read bytes from socket")

        return (self.sock if not self.has_ssl_connection else self.ssock).recv(
            int.from_bytes(bytes=res_size, byteorder=ENDIAS)
        )
    
    def init_fail_count(self):
        self.remaining_fails = self.max_fail_retries

    def fail_occured(self) -> bool:
        self.remaining_fails -= 1

        if self.remaining_fails <= 0:
            return True
        
        return False

class SocketClient:

    def __init__(self, address: str, port: int, conf: SocketClientConf, max_pool_connections: int = -1) -> None:
        self.address: str = address
        self.port: int = port
        self.pool: Queue[SocketConnection] = Queue()
        self.conf = conf
        self.add_connection(
            address=address if address != "localhost" else "127.0.0.1", port=port
        )

        self.max_pool_connections: int = self.conf.max_pool_connections

        if max_pool_connections > 0:
            self.max_pool_connections = max_pool_connections

        self.__stopped = False
        t = threading.Thread(target=self.__keep_pool_connections_to_maximum, daemon=True)
        t.start()

    def get_connection_info(self) -> Tuple[str, int]:
        return (self.address, self.port)

    def add_connection(self, address: str, port: int):
        # Create a TCP/IP socket
        sock = socket(AF_INET, SOCK_STREAM)
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
                    password=self.conf.cert_pass
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
            root_cert=self.conf.root_cert,
            cert=self.conf.cert,
            cert_key=self.conf.cert_key,
            cert_pass=self.conf.cert_pass
        )

        self.pool.put(conn)

    def send_request(self, req: bytes) -> bytes:
        conn: SocketConnection = None

        retries = self.conf.retries
        err: Exception = None

        while retries > 0:
            try:
                try:
                    conn = self.pool.get(
                        block=True,
                        timeout=(
                            int(self.conf.timeoutms / 1000) + 1
                            if self.conf.timeoutms is not None
                            else None
                        ),
                    )
                except:
                    raise RetryableException(
                        error_code=CONNECTION_ERROR,
                        error_message="No available connection to send request",
                    )
                
                try:
                    # Send the message to the server
                    conn.send_bytes(req)

                    # Wait for the response from the server
                    response = conn.receive_bytes()

                    if not response or len(response) == 0:
                        conn.close()
                        raise RetryableException(
                            error_code=CONNECTION_ERROR,
                            error_message="Could not receive response from the socket connection",
                        )
                    
                    conn.init_fail_count()

                    res_err_code = self.__get_response_error_code(response)

                    if res_err_code != NO_ERROR:
                        self.pool.put(conn)

                        res_err_message = self.__get_response_error(response)

                        if self.__is_error_retryable(res_err_code):
                            err = RetryableException(
                                error_code=res_err_code, error_message=res_err_message
                            )
                            retries -= 1
                            continue

                        raise FastQueueException(res_err_code, res_err_message)

                    self.pool.put(conn)

                    return response
                except TimeoutError:
                    if not conn.fail_occured():
                        self.pool.put(conn)
                    else: conn.close()

                    raise RetryableException(
                        error_code=TIMEOUT_ERROR,
                        error_message="Waiting for response timed out",
                    )
                except Exception as e:
                    conn.close()
                    raise e
            except RetryableException as e:
                retries -= 1
                if retries > 0: time.sleep(self.conf.retry_wait_ms / 1000)
                err = e
            except Exception as e:
                raise Exception(f"{err}")

        if err != None: raise Exception(f"{err}")

        return None
    
    def close(self):
        self.__stopped = True
        while self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.socket.close()
            except Exception as e:
                print(
                    f"Error occured while trying to close connection to the broker. Exception: {e}"
                )
                break

    def __get_response_error_code(self, res: bytes) -> int:
        return int.from_bytes(bytes=res[:INT_SIZE], byteorder=ENDIAS)

    def __get_response_error(self, res: bytes) -> str:
        return (
            res[INT_SIZE:].decode() if len(res) > INT_SIZE else "Internal server error"
        )

    def __is_error_retryable(self, error_code: int):
        return error_code in [CONNECTION_ERROR, TIMEOUT_ERROR]

    def __keep_pool_connections_to_maximum(self):
        while not self.__stopped:
            qsize = self.pool.qsize()
            for _ in range(qsize, self.max_pool_connections + 1):
                self.add_connection(self.address, self.port)

            if self.__stopped: break
            
            time.sleep(5)

    def __del__(self):
        self.close()
