from typing import Any, Dict, Tuple
from socket_client import *
from constants import *
from responses import (
    GetControllersConnectionInfoResponse,
    GetLeaderControllerIdResponse,
    CreateQueueResponse,
)
import time

class BrokerClientConf(SocketClientConf):

    def __init__(
        self,
        timeoutms: int = 0,
        ssl_enable: bool = False,
        root_cert: str = None,
        cert: str = None,
        cert_key: str = None,
        sasl_enable: bool = False,
        sasl_username: str = None,
        sasl_password: str = None,
    ) -> None:
        super().__init__(
            timeoutms=timeoutms,
            ssl_enable=ssl_enable,
            root_cert=root_cert,
            cert=cert,
            cert_key=cert_key,
            sasl_enable=sasl_enable,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
        )


class BrokerClient(SocketClient):

    def __init__(
        self,
        controller_node: Tuple[str, int],
        conf: BrokerClientConf = None,
    ) -> None:
        if controller_node is None:
            raise ValueError("Controller node cannot be empty")

        if conf is None:
            conf = BrokerClientConf()

        self.controller_nodes: Dict[int, SocketClient] = {}
        self.controller_leader: int = -1

        controller_conn = SocketClient(
            address=controller_node[0], port=controller_node[1], conf=conf
        )

        retries = 5

        controllers_connection_res: GetControllersConnectionInfoResponse = None

        while retries > 0:
            try:
                res = (
                    GetControllersConnectionInfoResponse(
                        controller_conn.send_request(
                            self.create_request(GET_CONTROLLERS_CONNECTION_INFO)
                        )
                    )
                    if controllers_connection_res is None
                    else controllers_connection_res
                )

                if controllers_connection_res is None:
                    controllers_connection_res = res

                    for controller in res.controller_nodes:
                        if (
                            controller.address == controller_node[0]
                            and controller.port == controller_node[1]
                        ):
                            self.controller_nodes[controller.node_id] = controller_conn
                        elif controller.node_id in self.controller_nodes:
                            continue
                        else:
                            self.controller_nodes[controller.node_id] = SocketClient(
                                controller.address, controller.port, conf
                            )

                    print("Initialized controller nodes connection info:")

                    for node in res.controller_nodes:
                        print(node)

                if res.leader_id in self.controller_nodes:
                    self.controller_leader = res.leader_id
                    break

                leader_res = GetLeaderControllerIdResponse(
                    controller_conn.send_request(
                        self.create_request(GET_CONTROLLER_LEADER_ID)
                    )
                )

                controllers_connection_res.leader_id = leader_res.leader_id

                if res.leader_id not in self.controller_nodes:
                    print("Leader not elected yet")

                retries -= 1
                time.sleep(3)
            except Exception as e:
                raise Exception(f"{e}")

        print(
            f"Connected successfully to controller quorum leader {self.controller_leader}"
        )

    def create_queue(
        self, queue: str, partitions: int = 1, replication_factor: int = 1
    ) -> None:
        if not queue:
            raise ValueError("Empty queue name was passed as argument")

        leader = self.controller_nodes[self.controller_leader]

        res = CreateQueueResponse(
            leader.send_request(
                self.create_request(
                    CREATE_QUEUE,
                    [
                        (QUEUE_NAME, queue),
                        (PARTITIONS, partitions),
                        (REPLICATION_FACTOR, replication_factor),
                    ],
                )
            )
        )

        if res.success:
            print(f"Queue {queue} created")
        else:
            print(f"Creation of queue {queue} failed")

    def delete_queue(self, queue: str) -> None:
        if not queue:
            raise ValueError("Empty queue name was passed as argument")

        res = self.send_request(
            self.create_request(DELETE_QUEUE, [(QUEUE_NAME, queue)])
        )

        queue_deleted: bool = (
            int.from_bytes(bytes=res[4:5], byteorder=ENDIAS) - ord("B") == 0
        )

        if queue_deleted:
            print(f"Queue {queue} does not exist")
        else:
            print(f"Queue {queue} deleted successfully")

    def create_request(
        self, req_type: int, values: list[Tuple[int, Any]] = None
    ) -> bytes:
        req_bytes = req_type.to_bytes(length=INT_SIZE, byteorder=ENDIAS)

        if values != None:
            for reqValKey, val in values:
                if val != None:
                    req_bytes += reqValKey.to_bytes(
                        length=4, byteorder=ENDIAS
                    ) + self.__val_to_bytes(val)

        # if self.conf.sasl_enable:
        #     req_bytes += (
        #         USERNAME.to_bytes(length=4, byteorder=ENDIAS)
        #         + self.__val_to_bytes(self.conf.sasl_username)
        #         + PASSWORD.to_bytes(length=4, byteorder=ENDIAS)
        #         + self.__val_to_bytes(self.conf.sasl_password)
        #     )

        return (LONG_SIZE + len(req_bytes)).to_bytes(
            length=LONG_SIZE, byteorder=ENDIAS
        ) + req_bytes

    def __val_to_bytes(self, val: Any) -> bytes:
        if isinstance(val, int):
            return val.to_bytes(length=4, byteorder=ENDIAS)
        elif isinstance(val, str):
            return len(val).to_bytes(length=4, byteorder=ENDIAS) + val.encode()
        else:
            raise Exception("Invalid request value of type ", type(val))
