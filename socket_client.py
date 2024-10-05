import socket
from typing import Tuple
from constants import *
from queue import Queue


class Socket(socket.socket):
    pass


class SocketConnection:
    def __init__(
        self, socket: Socket, ip_address: str, port: int, is_connected: bool = True
    ) -> None:
        self.socket: Socket = socket
        self.is_connected: bool = is_connected
        self.ip_address: str = ip_address
        self.port: int = port

    def reconnect(self):
        self.socket.connect((self.ip_address, self.port))


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
        socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket.connect((ip_address, port))

        if self.conf.timeoutms is not None:
            socket.settimeout(self.conf.timeoutms / 1000)

        conn: SocketConnection = SocketConnection(
            socket=socket, ip_address=ip_address, port=port, is_connected=True
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
                conn.socket.sendall(req)

                # Wait for the response from the server
                response = conn.socket.recv(1024)

                if not response:
                    conn.socket.close()
                    conn.is_connected = False
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
                conn.socket.close()
                conn.is_connected = False
                self.pool.put(conn)
                raise Exception("Connection to broker shut down unexpectedly")
            except Exception as e:
                conn.socket.close()
                conn.is_connected = False
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
