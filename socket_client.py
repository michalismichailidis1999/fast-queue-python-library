import socket
from typing import Tuple
from constants import *

class SocketClientConf:
    def __init__(self, retries:int = 0, timeoutms:int = None) -> None:
        self.retries:int = retries
        self.timeoutms = timeoutms

class SocketClient:
    def __init__(self, ip_address:str, port:int, conf:SocketClientConf) -> None:
        # Create a TCP/IP socket
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.client_socket.connect((ip_address, port))
        print(f"Connected to server at {ip_address}:{port}")

        self.is_connected = True
        self.conf = conf

        if self.conf.timeoutms is not None:
            self.client_socket.settimeout(self.conf.timeoutms / 1000)

    def send_request(self, req:bytes) -> Tuple[bool, str, bytes]: # (Error Occured, Error Message, response bytes)
        if not self.is_connected:
            reconnected = self.try_reconnect()

            if not reconnected: return [True, "Could not connect to broker", None]

        retries = 0
        
        while retries < self.conf.retries:
            try:
                if not self.is_connected:
                    reconnected = self.try_reconnect()

                    if not reconnected: return [True, "Could not connect to broker", None]

                # Send the message to the server
                self.client_socket.sendall(req)

                # Wait for the response from the server
                response = self.client_socket.recv(1024)

                if not response:
                    self.client_socket.close()
                    self.is_connected = False
                    return [True, "Connection to broker shutdown unexpectedly", None]
                
                response_err = self.get_response_error(response)

                if response_err[0] != -1:
                    retries += 1
                    continue

                return [False, None, response]
            except self.client_socket.timeout:
                retries += 1
            except BlockingIOError as e:
                # Non-blocking socket has no data ready yet, so it's still alive.
                return [True, "Blocking I/O error occured while trying to send request", None]
            except ConnectionResetError:
                # The connection was forcibly closed by the remote host.
                self.client_socket.close()
                self.is_connected = False
                return [True, "Connection to broker shutdown unexpectedly", None]
            except Exception as e:
                # Other errors (like broken pipe, etc.) mean the socket is likely dead.
                self.client_socket.close()
                self.is_connected = False
                return [True, f"Exception occurred: {e}", None]
        
    def try_reconnect(self) -> bool:
        return False
    
    def get_response_error(self, res:bytes) -> Tuple[int,str]:
        return [-1, None]
    
    def is_error_retryable(error_type:int) -> bool:
        return error_type in [
            NOT_LEADER_FOR_PARTITION, 
            UNKNOWN_QUEUE_OR_PARTITION, 
            REQUEST_TIMED_OUT
        ]