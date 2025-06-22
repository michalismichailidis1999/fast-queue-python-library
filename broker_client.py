from typing import Any, Dict, Tuple
from socket_client import *
from constants import *
from responses import (
    GetControllersConnectionInfoResponse,
    GetLeaderControllerIdResponse,
    CreateQueueResponse,
)
import time
import threading

class BrokerClientConf(SocketClientConf):

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
        super().__init__(
            timeoutms=timeoutms,
            retries=retries,
            retry_wait_ms=retry_wait_ms,
            ssl_enable=ssl_enable,
            root_cert=root_cert,
            cert=cert,
            cert_key=cert_key,
            cert_pass=cert_pass,
            sasl_enable=sasl_enable,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            max_pool_connections=max_pool_connections
        )


class BrokerClient:

    def __init__(
        self,
        controller_node: Tuple[str, int],
        conf: BrokerClientConf = None,
    ) -> None:
        if controller_node is None:
            raise ValueError("Controller node cannot be empty")

        if conf is None:
            conf = BrokerClientConf()

        self.controllers_mut: threading.Lock = threading.Lock()

        self.controller_nodes: Dict[int, SocketClient] = {}
        self.controller_leader: int = -1

        self.conf = conf

        # initialize the leader
        self.__check_for_leader_update(controller_node=controller_node, retries=5, called_from_contructor=True)

        print(
            f"Connected successfully to controller quorum leader {self.controller_leader}"
        )

        self.__stopped = False

        # check periodically for leader change
        t = threading.Thread(target=self.__check_for_leader_update, daemon=True, args=[controller_node, 1, False])
        t.start()

    def __check_for_leader_update(self, controller_node: Tuple[str, int], retries: int, called_from_contructor: bool = False):
        controller_conn = SocketClient(
            address=controller_node[0], port=controller_node[1], conf=self.conf
        )

        while not self.__stopped:
            controllers_connection_res: GetControllersConnectionInfoResponse = None

            self.controllers_mut.acquire()

            try:
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

                            # TODO: Update this logic when functionality to update controllers in configuration mid running is added
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

                            if called_from_contructor:
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

                        if called_from_contructor and res.leader_id not in self.controller_nodes:
                            print("Leader not elected yet")

                        retries -= 1

                        if retries > 0: time.sleep(3)
                    except Exception as e:
                        raise Exception(f"{e}")
            except Exception as e:
                if called_from_contructor: raise e
                print(f"Error occured while trying to fetch controller nodes updates. {e}")
            finally:
                self.controllers_mut.release()

            if called_from_contructor: return

            if self.__stopped: break

            time.sleep(10)

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
    
    def close(self):
        self.__stopped = True

        self.controllers_mut.acquire()

        try:
            for node_socket_client in self.controller_nodes.values():
                node_socket_client.close()
        except Exception as e:
            print(f"Error occured while closing client. {e}")
        finally:
            self.controllers_mut.release()

    def __val_to_bytes(self, val: Any) -> bytes:
        if isinstance(val, int):
            return val.to_bytes(length=4, byteorder=ENDIAS)
        elif isinstance(val, str):
            return len(val).to_bytes(length=4, byteorder=ENDIAS) + val.encode()
        else:
            raise Exception("Invalid request value of type ", type(val))
        
    def __del__(self):
        self.close()
