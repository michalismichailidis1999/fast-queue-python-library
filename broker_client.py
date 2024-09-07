from socket_client import *
from constants import *

class BrokerClientConf(SocketClientConf):
    def __init__(self, retries: int = 0, timeoutms: int = None) -> None:
        super().__init__(retries, timeoutms)


class BrokerClient(SocketClient):
    def __init__(
        self,
        ip_address: str = "127.0.0.1",
        port: int = 9877,
        conf: BrokerClientConf = None,
    ) -> None:
        if conf is None:
            conf = BrokerClientConf()

        super().__init__(ip_address, port, conf)

    def create_queue(self, queue: str, partitions: int = 1) -> None:
        res = self.send_request(
            CREATE_QUEUE.to_bytes(length=4, byteorder=ENDIAS)
            + queue.encode()
            + partitions.to_bytes(length=4, byteorder=ENDIAS)
        )
        print(res[2].decode())

    def delete_queue(self, queue: str) -> None:
        res = self.send_request(bytes([DELETE_QUEUE]) + queue.encode())

    def list_queues(self) -> list[str]:
        res = self.send_request(bytes([LIST_QUEUES]))
