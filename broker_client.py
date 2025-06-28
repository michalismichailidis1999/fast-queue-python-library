from typing import Any, Dict, Set, Tuple
from socket_client import *
from constants import *
from responses import (
    GetControllersConnectionInfoResponse,
    GetLeaderControllerIdResponse,
    CreateQueueResponse,
)
import time
import threading
from lock import ReadWriteLock

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
        authentication_enable: bool = False,
        username: str = None,
        password: str = None,
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
            max_pool_connections=max_pool_connections
        )

        self._authentication_enable: bool = authentication_enable
        self._username: str = username
        self._password: str = password


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

        self.__lock: ReadWriteLock = ReadWriteLock()

        self.__controller_nodes: Dict[int, SocketClient] = {}
        self.__leader_node_id: int = -1

        self._conf = conf

        # initialize the leader
        self.__check_for_leader_update(controller_node=controller_node, retries=5, called_from_contructor=True)

        print(
            f"Connected successfully to controller quorum leader {self.__leader_node_id}"
        )

        self.__stopped = False

        # check periodically for leader change
        t = threading.Thread(target=self.__check_for_leader_update, daemon=True, args=[controller_node, 1, False])
        t.start()

    def __check_for_leader_update(self, controller_node: Tuple[str, int], retries: int, called_from_contructor: bool = False):
        controller_conn = SocketClient(address=controller_node[0], port=controller_node[1], conf=self._conf, max_pool_connections=1)
    
        while not self.__stopped:
            while retries > 0:
                try:
                    res = GetControllersConnectionInfoResponse(
                        controller_conn.send_request(
                            self._create_request(GET_CONTROLLERS_CONNECTION_INFO, None, False)
                        )
                    )

                    to_keep = set()

                    for controller in res.controller_nodes:
                        conn_info: Tuple[str, int] | None = self.__get_controller_node_conenction_info(controller.node_id)

                        if conn_info is not None and conn_info[0] == controller.address and conn_info[1] == controller.port:
                            continue
                        
                        self.__replace_controller_node_socket_client(
                            node_id=controller.node_id, 
                            new_address=controller.address, 
                            new_port=controller.port,
                        )

                        to_keep.add(controller.node_id)


                    self.__remove_unused_controller_nodes(to_keep)

                    if called_from_contructor:
                        print("Initialized controller nodes connection info:")

                        for node in res.controller_nodes:
                            print(node)

                    if res.leader_id in self.__leader_node_id:
                        self.__leader_node_id = res.leader_id
                        break

                    leader_res = GetLeaderControllerIdResponse(
                        controller_conn.send_request(
                            self._create_request(GET_CONTROLLER_LEADER_ID, None, False)
                        )
                    )

                    res.leader_id = leader_res.leader_id

                    if called_from_contructor and res.leader_id not in self.__controller_nodes:
                        print("Leader not elected yet")

                    retries -= 1

                    if retries > 0: time.sleep(3)
                except Exception as e:
                    if called_from_contructor:
                        controller_conn.close()
                        raise Exception(f"{e}")
                    else:
                        print(f"Error occured while trying to fetch leader controller update. {e}")
                        break

            if called_from_contructor:
                controller_conn.close()
                return

            if self.__stopped:
                controller_conn.close()
                break

            time.sleep(10)

    def create_queue(
        self, queue: str, partitions: int = 1, replication_factor: int = 1
    ) -> None:
        if not queue:
            raise ValueError("Empty queue name was passed as argument")

        try:
            leader = self._get_leader_node_socket_client()

            if leader is None:
                raise Exception("No elected controller leader yet")

            res = CreateQueueResponse(
                leader.send_request(
                    self._create_request(
                        CREATE_QUEUE,
                        [
                            (QUEUE_NAME, queue),
                            (PARTITIONS, partitions),
                            (REPLICATION_FACTOR, replication_factor),
                        ],
                        True,
                    )
                )
            )

            if res.success:
                print(f"Queue {queue} created")
            else:
                print(f"Creation of queue {queue} failed")
        except Exception as e:
            print(f"Error occured while trying to create queue {queue}. {e}")

    def delete_queue(self, queue: str) -> None:
        if not queue:
            raise ValueError("Empty queue name was passed as argument")
        
        try:
            leader = self._get_leader_node_socket_client()

            if leader is None:
                raise Exception("No elected controller leader yet")

            res = CreateQueueResponse(
                leader.send_request(
                    self._create_request(
                        CREATE_QUEUE,
                        [
                            (QUEUE_NAME, queue),
                        ],
                        True
                    )
                )
            )

            if res.success:
                print(f"Queue {queue} deleted")
            else:
                print(f"Deletion of queue {queue} failed")
        except Exception as e:
            print(f"Error occured while trying to delete queue {queue}. {e}")

    def _create_request(
        self, req_type: int, values: list[Tuple[int, Any]] = None, request_needs_authentication: bool = False
    ) -> bytes:
        req_bytes = req_type.to_bytes(length=INT_SIZE, byteorder=ENDIAS)

        if values != None:
            for reqValKey, val in values:
                if val != None:
                    req_bytes += self.__val_to_bytes(reqValKey) + self.__val_to_bytes(val)

        if self._conf._authentication_enable and request_needs_authentication:
            req_bytes += (
                self.__val_to_bytes(USERNAME)
                + self.__val_to_bytes(self._conf._username)
                + self.__val_to_bytes(PASSWORD)
                + self.__val_to_bytes(self._conf._password)
            )

        return self.__val_to_bytes(LONG_SIZE + len(req_bytes), LONG_SIZE) + req_bytes
    
    def close(self):
        self.__stopped = True

        self.__lock.acquire_write()

        try:
            for node_socket_client in self.__controller_nodes.values():
                node_socket_client.close()
        except Exception as e:
            print(f"Error occured while closing client. {e}")
        finally:
            self.__lock.release_write()

    def _get_leader_node_socket_client(self) -> SocketClient | None:
        self.__lock.acquire_read()

        socket_client = self.__controller_nodes[self.__leader_node_id] if self.__leader_node_id in self.__controller_nodes else None

        self.__lock.release_read()

        return socket_client
    
    def __replace_controller_node_socket_client(self, node_id: int, new_address: str, new_port: int):
        self.__lock.acquire_write()

        try:
            if node_id in self.__controller_nodes: del self.__controller_nodes[node_id]
            self.__controller_nodes[node_id] = SocketClient(address=new_address, port=new_port, conf=self._conf)
        except Exception as e:
            pass
        finally:
            self.__lock.release_write()

    def __get_controller_node_conenction_info(self, node_id: int) -> Tuple[str, int] | None:
        self.__lock.acquire_read()

        conn_info: Tuple[str, int] | None = None

        if node_id in self.__controller_nodes:
            conn_info = self.__controller_nodes[node_id].get_connection_info()

        self.__lock.release_read()

        return conn_info
    
    def __remove_unused_controller_nodes(self, to_keep: Set[int]):
        self.__lock.acquire_write()

        try:
            node_ids = list(self.__controller_nodes.keys())
            for node_id in node_ids:
                if node_id not in to_keep:
                    del self.__controller_nodes[node_id]
        except Exception as e:
            print(f"Error occured while trying to remove unused node connection. {e}")
        finally:
            self.__lock.release_write()

    def __val_to_bytes(self, val: Any, numeric_size: int | None = None) -> bytes:
        if isinstance(val, int):
            return val.to_bytes(length=4 if numeric_size is None else numeric_size, byteorder=ENDIAS)
        elif isinstance(val, str):
            return len(val).to_bytes(length=4, byteorder=ENDIAS) + val.encode()
        else:
            raise Exception("Invalid request value of type ", type(val))
        
    def __del__(self):
        self.close()
