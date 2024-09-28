from typing import Any
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
            self.create_request(
                CREATE_QUEUE, [(QUEUE_NAME, queue), (PARTITIONS, partitions)]
            )
        )

        if res[0]:
            print(f"Could not create queue {queue}. Reason: {res[1]}")
        elif (
            int.from_bytes(bytes=res[2][4:5], byteorder=ENDIAS, signed=False) - ord("B")
            == 0
        ):
            print(f"Queue {queue} already exists")
        else:
            print(f"Queue {queue} created successfully")

    def delete_queue(self, queue: str) -> None:
        res = self.send_request(
            DELETE_QUEUE.to_bytes(length=4, byteorder=ENDIAS) + queue.encode()
        )

        if res[0]:
            print(f"Could not delete queue {queue}. Reason: {res[1]}")
        elif (
            int.from_bytes(bytes=res[2][4:5], byteorder=ENDIAS, signed=False) - ord("B")
            == 0
        ):
            print(f"Queue {queue} does not exist")
        else:
            print(f"Queue {queue} deleted successfully")

    def list_queues(self) -> list[str]:
        res = self.send_request(LIST_QUEUES.to_bytes(length=4, byteorder=ENDIAS))

        if res[0]:
            print(f"Could not get queues list. Reason: {res[1]}")
        else:
            print("Queues: ", res[2][4:].decode())

    def create_request(self, req_type: int, values: list[Tuple[int, Any]]) -> bytes:
        req_bytes = req_type.to_bytes(length=4, byteorder=ENDIAS)

        for reqValKey, val in values:
            req_bytes += reqValKey.to_bytes(
                length=4, byteorder=ENDIAS
            ) + self.__val_to_bytes(val)

        return req_bytes

    def __val_to_bytes(self, val: Any) -> bytes:
        if isinstance(val, int):
            return val.to_bytes(length=4, byteorder=ENDIAS)
        elif isinstance(val, str):
            return len(val).to_bytes(length=4, byteorder=ENDIAS) + val.encode()
        else:
            raise Exception("Invalid request value of type ", type(val))
