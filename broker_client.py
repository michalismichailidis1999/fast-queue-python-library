from typing import Any
from socket_client import *
from constants import *

class BrokerClientConf(SocketClientConf):

    def __init__(
        self,
        retries: int = 0,
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
        super().__init__(
            retries=retries,
            timeoutms=timeoutms,
            ssl_enable=ssl_enable,
            root_cert=root_cert,
            cert=cert,
            cert_key=cert_key,
            sasl_enable=sasl_enable,
            sasl_auth_method=sasl_auth_method,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
        )


class BrokerClient(SocketClient):

    def __init__(
        self,
        ip_address: str = "localhost",
        port: int = 9877,
        conf: BrokerClientConf = None,
    ) -> None:
        if conf is None:
            conf = BrokerClientConf()

        super().__init__(ip_address, port, conf)

    def create_queue(self, queue: str, partitions: int = 1) -> None:
        if not queue:
            raise ValueError("Empty queue name was passed as argument")

        res = self.send_request(
            self.create_request(
                CREATE_QUEUE, [(QUEUE_NAME, queue), (PARTITIONS, partitions)]
            )
        )

        queue_created: bool = (
            int.from_bytes(bytes=res[4:5], byteorder=ENDIAS, signed=False) - ord("B")
            == 0
        )

        if queue_created:
            print(f"Queue {queue} already exists")
        else:
            print(f"Queue {queue} created successfully")

    def delete_queue(self, queue: str) -> None:
        if not queue:
            raise ValueError("Empty queue name was passed as argument")

        res = self.send_request(
            self.create_request(DELETE_QUEUE, [(QUEUE_NAME, queue)])
        )

        queue_deleted: bool = (
            int.from_bytes(bytes=res[4:5], byteorder=ENDIAS, signed=False) - ord("B")
            == 0
        )

        if queue_deleted:
            print(f"Queue {queue} does not exist")
        else:
            print(f"Queue {queue} deleted successfully")

    def list_queues(self) -> list[str]:
        res = self.send_request(self.create_request(LIST_QUEUES))

        print("Queues: ", res[4:].decode())

    def create_request(
        self, req_type: int, values: list[Tuple[int, Any]] = None
    ) -> bytes:
        req_bytes = req_type.to_bytes(length=4, byteorder=ENDIAS)

        if values != None:
            for reqValKey, val in values:
                if val != None:
                    req_bytes += reqValKey.to_bytes(
                        length=4, byteorder=ENDIAS
                    ) + self.__val_to_bytes(val)

        if self.conf.sasl_enable and self.conf.sasl_auth_method == SASL_BASIC_AUTH:
            req_bytes += (
                USERNAME.to_bytes(length=4, byteorder=ENDIAS)
                + self.__val_to_bytes(self.conf.sasl_username)
                + PASSWORD.to_bytes(length=4, byteorder=ENDIAS)
                + self.__val_to_bytes(self.conf.sasl_password)
            )

        return req_bytes

    def __val_to_bytes(self, val: Any) -> bytes:
        if isinstance(val, int):
            return val.to_bytes(length=4, byteorder=ENDIAS)
        elif isinstance(val, str):
            return len(val).to_bytes(length=4, byteorder=ENDIAS) + val.encode()
        else:
            raise Exception("Invalid request value of type ", type(val))
