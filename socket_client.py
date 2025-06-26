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
        
        self._timeoutms = timeoutms
        self._retries: int = retries
        self._retry_wait_ms: int = retry_wait_ms
        self._ssl_enable: bool = ssl_enable
        self._root_cert: str = root_cert
        self._cert: str = cert
        self._cert_key: str = cert_key
        self._cert_pass: str = cert_pass
        self._sasl_enable: bool = sasl_enable
        self._sasl_username: str = sasl_username
        self._sasl_password: str = sasl_password
        self._max_pool_connections: int = max_pool_connections

class SocketConnection:

    def __init__(
        self,
        sock: socket,
        ssl_sock: ssl.SSLSocket
    ) -> None:
        self.__sock: socket = sock
        self.__ssock: ssl.SSLSocket = ssl_sock
        self.__has_ssl_connection: bool = self.__ssock is not None
        self.__max_fail_retries: int = 2
        self.__remaining_fails: int = self.__max_fail_retries

    def close(self):
        try:
            (self.__sock if not self.__has_ssl_connection else self.__ssock).close()
        except Exception as e:
            print(f"Could not close socket connection. {e}")

    def send_bytes(self, req: bytes):
        (self.__sock if not self.__has_ssl_connection else self.__ssock).sendall(req)

    def receive_bytes(self) -> bytes:
        res_size = (self.__sock if not self._has_ssl_connection else self.__ssock).recv(
            LONG_SIZE
        )

        if len(res_size) <= 0:
            raise Exception("Error occurred while trying to read bytes from socket")

        return (self.__sock if not self.__has_ssl_connection else self.__ssock).recv(
            int.from_bytes(bytes=res_size, byteorder=ENDIAS)
        )
    
    def init_fail_count(self):
        self.__remaining_fails = self.__max_fail_retries

    def fail_occured(self) -> bool:
        self.__remaining_fails -= 1

        if self.__remaining_fails <= 0:
            return True
        
        return False

class SocketClient:

    def __init__(self, address: str, port: int, conf: SocketClientConf, max_pool_connections: int = -1) -> None:
        self.__address: str = address
        self.__port: int = port
        self.__pool: Queue[SocketConnection] = Queue()
        self.__conf = conf
        self.__add_connection(
            address=address if address != "localhost" else "127.0.0.1", port=port
        )

        self.__max_pool_connections: int = conf._max_pool_connections

        if max_pool_connections > 0:
            self.__max_pool_connections = max_pool_connections

        self.__stopped = False
        t = threading.Thread(target=self.__keep_pool_connections_to_maximum, daemon=True)
        t.start()

    def get_connection_info(self) -> Tuple[str, int]:
        return (self.__address, self.__port)

    def __add_connection(self, address: str, port: int):
        # Create a TCP/IP socket
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, port))

        if self.__conf._timeoutms is not None:
            sock.settimeout(self.conf.timeoutms / 1000)

        ssock: ssl.SSLSocket = None

        if self.__conf._ssl_enable:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

            context.load_verify_locations(self.conf.root_cert)

            if self.conf.cert and self.conf.cert_key:
                context.load_cert_chain(
                    certfile=self.__conf._cert,
                    keyfile=self.__conf._cert_key,
                    password=self.__conf._cert_pass
                )

            ssock = context.wrap_socket(sock, server_hostname=address)

            if ssock.getpeercert() is None:
                raise ssl.SSLError("Failed to retrieve server certificate")

        conn: SocketConnection = SocketConnection(sock=sock, ssl_sock=ssock)

        self.__pool.put(conn)

    def send_request(self, req: bytes) -> bytes:
        conn: SocketConnection = None

        retries = self.__conf._retries
        err: Exception = None

        while retries > 0:
            try:
                try:
                    conn = self.__pool.get(
                        block=True,
                        timeout=(
                            int(self.__conf._timeoutms / 1000) + 1
                            if self.__conf._timeoutms is not None
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
                        self.__pool.put(conn)

                        res_err_message = self.__get_response_error(response)

                        if self.__is_error_retryable(res_err_code):
                            err = RetryableException(
                                error_code=res_err_code, error_message=res_err_message
                            )
                            retries -= 1
                            continue

                        raise FastQueueException(res_err_code, res_err_message)

                    self.__pool.put(conn)

                    return response
                except TimeoutError:
                    if not conn.fail_occured():
                        self.__pool.put(conn)
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
                if retries > 0: time.sleep(self.__conf._retry_wait_ms / 1000)
                err = e
            except Exception as e:
                raise Exception(f"{err}")

        if err != None: raise Exception(f"{err}")

        return None
    
    def close(self):
        self.__stopped = True
        while self.__pool.empty():
            try:
                conn = self.__pool.get_nowait()
                conn.close()
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
            qsize = self.__pool.qsize()
            for _ in range(qsize, self.__max_pool_connections + 1):
                self.__add_connection(self.__address, self.__port)

            if self.__stopped: break
            
            time.sleep(5)

    def __del__(self):
        self.close()