from socket import socket, AF_INET, SOCK_STREAM
import time
from queue import Empty, Queue
import ssl
import threading
from typing import Tuple
from constants import *
from exceptions import RetryableException, FastQueueException
from lock import ReadWriteLock
from conf import SocketClientConf

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

    def send_bytes(self, req: bytearray):
        (self.__sock if not self.__has_ssl_connection else self.__ssock).sendall(req)

    def receive_bytes(self) -> bytes:
        res_size = (self.__sock if not self.__has_ssl_connection else self.__ssock).recv(
            INT_SIZE
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
    
class ConnectionPool:
    def __init__(self, address:str, port: int, conf: SocketClientConf, max_pool_connections: int, raise_on_conn_init: bool):
        self.__pool: Queue[SocketConnection] = Queue()
        self.__address: str = address
        self.__port: int = port
        self.__max_pool_connections: int = max_pool_connections
        self.__conf: SocketClientConf = conf
        self.__connections_count: int = 0
        self.__borrowed_connections: int = 0
        self.__lock: ReadWriteLock = ReadWriteLock()

        self.__wait_seconds_to_add_new_conn: int = 5

        self.__stopped: bool = False

        self.__ping_total_bytes = 9
        self.__ping_bytes: bytes = self.__ping_total_bytes.to_bytes(length=INT_SIZE, byteorder=ENDIAS) + PING.to_bytes(length=INT_SIZE, byteorder=ENDIAS) + True.to_bytes(length=BOOL_SIZE, byteorder=ENDIAS)

        try:
            self.__add_connection(address=address, port=port)
        except Exception as e:
            if raise_on_conn_init:
                raise e

        t = threading.Thread(target=self.__keep_pool_connections_to_maximum, daemon=True)
        t.start()

        t2 = threading.Thread(target=self.__ping_connections, daemon=True)
        t2.start()

    def get_connections_count(self) -> int:
        self.__lock.acquire_read()

        count = self.__connections_count + self.__borrowed_connections

        self.__lock.release_read()

        return count
    
    def get_connection(self, no_wait: bool = False) -> SocketConnection | None:
        if self.get_connections_count() == 0: return None

        conn: SocketConnection | None = None

        try:
            if not no_wait:
                conn = self.__pool.get(
                    block=True,
                    timeout=(
                        int(self.__conf._timeoutms / 1000) + 1
                        if self.__conf._timeoutms is not None and self.__conf._timeoutms > 0
                        else None
                    ),
                )
            else: conn = self.__pool.get_nowait()
        except: return None

        if conn is not None:
            self.__lock.acquire_write()

            self.__connections_count -= 1
            self.__borrowed_connections += 1

            self.__lock.release_write()

        return conn

    def return_connection(self, conn: SocketConnection | None, is_new_conn: bool = False) -> None:
        if self.__stopped: return

        self.__lock.acquire_write()

        if not is_new_conn: self.__borrowed_connections -= 1

        if conn is not None:
            self.__connections_count += 1
            self.__pool.put(conn)

        self.__lock.release_write()

    def __add_connection(self, address: str, port: int):
        # Create a TCP/IP socket
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, port))

        if self.__conf._timeoutms > 0:
            sock.settimeout(self.__conf._timeoutms / 1000)

        ssock: ssl.SSLSocket = None

        if self.__conf._ssl_enable:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

            context.load_verify_locations(self.__conf._root_cert)

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

        self.return_connection(conn=conn, is_new_conn=True)

    def __keep_pool_connections_to_maximum(self):
        conns_diff: int = 0

        while not self.__stopped:
            conns_diff = self.__max_pool_connections - self.get_connections_count()

            if conns_diff == 0:
                time.sleep(self.__wait_seconds_to_add_new_conn)
                continue

            for _ in range(conns_diff):
                self.__add_connection(self.__address, self.__port)

            if self.__stopped: break
            
            time.sleep(self.__wait_seconds_to_add_new_conn)

    def __ping_connections(self):
        while not self.__stopped:
            try:
                time.sleep(self.__conf._connections_ping_time_ms / 1000)

                try:
                    total_connections = self.get_connections_count()

                    for _ in range(total_connections):
                        conn = self.get_connection(no_wait=True)

                        if conn is None: break

                        conn.send_bytes(self.__ping_bytes)

                        res = conn.receive_bytes()

                        if len(res) == BOOL_SIZE and bool.from_bytes(bytes=res, byteorder=ENDIAS):
                            self.return_connection(conn)
                        else:
                            self.return_connection(None)
                            print("Connection ping failed")

                except Exception as e:
                    print(f"Error occured while trying to ping pool socket connections. Reason: {e}")

            except Exception as e:
                print("Something went wrong while trying to ping socket connections")

    def close(self):
        self.__stopped = True

        while not self.__pool.empty():
            try:
                conn = self.__pool.get_nowait()

                if conn is not None:
                    try:
                        conn.close()
                    except:
                        pass
            except Empty: break
            except Exception as e:
                print(
                    f"Error occured while trying to close connection to the broker. Exception: {e}"
                )
                break

class SocketClient:

    def __init__(self, address: str, port: int, conf: SocketClientConf, max_pool_connections: int = -1, raise_on_conn_init: bool = True) -> None:
        self.__address: str = address
        self.__port: int = port
        self.__conf = conf

        self.__pool: ConnectionPool = ConnectionPool(
            address=address,
            port=port,
            conf=conf,
            max_pool_connections=(max_pool_connections if max_pool_connections > 0 else conf._max_pool_connections),
            raise_on_conn_init=raise_on_conn_init
        )

    def get_connection_info(self) -> Tuple[str, int]:
        return (self.__address, self.__port)

    def send_request(self, req: bytearray) -> bytes:
        conn: SocketConnection = None

        retries = self.__conf._retries
        err: Exception = None

        while retries > 0:
            try:
                conn = self.__pool.get_connection() if conn is None else conn

                if conn is None:
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
                        self.__pool.return_connection(None)
                        raise RetryableException(
                            error_code=CONNECTION_ERROR,
                            error_message="Could not receive response from the socket connection",
                        )
                    
                    conn.init_fail_count()

                    res_err_code = self.__get_response_error_code(response)

                    if res_err_code != NO_ERROR:
                        res_err_message = self.__get_response_error(response)

                        if self.__is_error_retryable(res_err_code):
                            err = RetryableException(
                                error_code=res_err_code, error_message=res_err_message
                            )
                            retries -= 1
                            if retries > 0: time.sleep(1.5)
                            continue

                        self.__pool.return_connection(conn)

                        raise FastQueueException(res_err_code, res_err_message)

                    self.__pool.return_connection(conn)

                    return response
                except TimeoutError:
                    if not conn.fail_occured():
                        self.__pool.return_connection(conn)
                    else:
                        conn.close()
                        self.__pool.return_connection(None)

                    raise RetryableException(
                        error_code=TIMEOUT_ERROR,
                        error_message="Waiting for response timed out",
                    )
                except Exception as e:
                    conn.close()
                    self.__pool.return_connection(None)
                    raise e
            except RetryableException as e:
                retries -= 1
                err = e
                if retries > 0: time.sleep(self.__conf._retry_wait_ms / 1000)
            except Exception as e:
                raise Exception(f"{e}")

        if err != None: raise Exception(f"{err}")

        return None
    
    def close(self):
        self.__pool.close()

    def __get_response_error_code(self, res: bytes) -> int:
        return int.from_bytes(bytes=res[:INT_SIZE], byteorder=ENDIAS)

    def __get_response_error(self, res: bytes) -> str:
        return (
            res[(INT_SIZE * 3):].decode() if len(res) > (INT_SIZE * 3) else "Internal server error"
        )

    def __is_error_retryable(self, error_code: int):
        return error_code in [CONNECTION_ERROR, TIMEOUT_ERROR]

    def __del__(self):
        self.close()