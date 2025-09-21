from typing import Any, Dict, Set, Tuple
from socket_client import *
from constants import *
from responses import (
    DeleteQueueResponse,
    GetControllersConnectionInfoResponse,
    GetLeaderControllerIdResponse,
    CreateQueueResponse,
)
import time
import threading
from lock import ReadWriteLock
from conf import BrokerClientConf

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

        self._conf: BrokerClientConf = conf

        self.__stopped: bool = False

        # initialize the leader
        while self.__leader_node_id <= 0:
            self.__check_for_leader_update(controller_node=controller_node, retries=10, time_to_wait=2, called_from_contructor=True)

        print(
            f"Connected successfully to controller quorum leader {self.__leader_node_id}"
        )

        # check periodically for leader change
        t = threading.Thread(target=self.__check_for_leader_update, daemon=True, args=[controller_node, 1, 15, False])
        t.start()

    def __check_for_leader_update(self, controller_node: Tuple[str, int], retries: int, time_to_wait: int, called_from_contructor: bool = False):
        controller_conn: SocketClient = SocketClient(address=controller_node[0], port=controller_node[1], conf=self._conf, max_pool_connections=2)

        initial_retries: int = retries

        success: bool = False
    
        while not self.__stopped:
            retries = initial_retries
            success = False

            while retries > 0:
                try:
                    res = GetControllersConnectionInfoResponse(
                        controller_conn.send_request(
                            self._create_request(GET_CONTROLLERS_CONNECTION_INFO, None, False)
                        )
                    )

                    to_keep = set()

                    for controller in res.controller_nodes:
                        to_keep.add(controller.node_id)

                        conn_info: Tuple[str, int] | None = self.__get_controller_node_conenction_info(controller.node_id)

                        if conn_info is not None and conn_info[0] == controller.address and conn_info[1] == controller.port:
                            continue
                        
                        self.__replace_controller_node_socket_client(
                            node_id=controller.node_id, 
                            new_address=controller.address, 
                            new_port=controller.port,
                        )

                    self.__remove_unused_controller_nodes(to_keep)

                    if called_from_contructor:
                        print("Initialized controller nodes connection info:")

                        for node in res.controller_nodes:
                            print(node)

                    if res.leader_id in self.__controller_nodes:
                        self.__leader_node_id = res.leader_id
                        retries -= 1
                        success = True
                        break

                    leader_res = GetLeaderControllerIdResponse(
                        controller_conn.send_request(
                            self._create_request(GET_CONTROLLER_LEADER_ID, None, False)
                        )
                    )

                    res.leader_id = leader_res.leader_id

                    retries -= 1

                    if res.leader_id in self.__controller_nodes:
                        success = True
                        break
                except Exception as e:
                    retries -= 1

                    if retries <= 0:
                        if called_from_contructor:
                            controller_conn.close()
                            raise Exception(f"Error occured while trying to fetch leader controller update. {e}")
                        else:
                            print(f"Error occured while trying to fetch leader controller update. {e}")
                finally:
                    if retries > 0 and not success: time.sleep(time_to_wait)

            if called_from_contructor:
                controller_conn.close()
                if self.__leader_node_id not in self.__controller_nodes: raise Exception("Could not connect to leader node")
                return
            
            if self.__stopped:
                controller_conn.close()
                break

            time.sleep(time_to_wait)

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
                            (QUEUE_NAME, queue, None),
                            (PARTITIONS, partitions, None),
                            (REPLICATION_FACTOR, replication_factor, None),
                        ],
                        True,
                    )
                )
            )

            if res.success:
                if res.created:
                    print(f"Queue {queue} created")
                    time.sleep(15)
                else: print(f"Queue {queue} already exists")
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

            res = DeleteQueueResponse(
                leader.send_request(
                    self._create_request(
                        CREATE_QUEUE,
                        [
                            (QUEUE_NAME, queue, None),
                        ],
                        True
                    )
                )
            )

            if res.success:
                if res.deleted: print(f"Queue {queue} deleted")
                else: print(f"Queue {queue} does not exist")
            else:
                print(f"Deletion of queue {queue} failed")
        except Exception as e:
            print(f"Error occured while trying to delete queue {queue}. {e}")

    def _create_request(
        self, req_type: int, values: list[Tuple[int, Any, int | None]] = None, request_needs_authentication: bool = False
    ) -> bytearray:
        total_bytes: int = 8

        if self._conf._authentication_enable and request_needs_authentication:
            total_bytes += 4 * INT_SIZE + len(self._conf._username) + len(self._conf._password)

        messages_total_bytes = 0

        if values != None:
            for reqValKey, val, val_bytes in values:
                total_bytes += INT_SIZE

                if val != None and reqValKey != MESSAGES:
                    total_bytes += self.__get_val_bytes(val=val) if val_bytes is None else val_bytes
                elif val != None and reqValKey == MESSAGES:
                    total_bytes += INT_SIZE
                    
                    for message, key in val:
                        message_bytes = 2 * INT_SIZE + len(message) + (len(key) if key is not None else 0)
                        total_bytes += message_bytes
                        messages_total_bytes += message_bytes

        req_bytes = bytearray(total_bytes)

        req_bytes[0:4] = self.__val_to_bytes(total_bytes)
        req_bytes[4:8] = self.__val_to_bytes(req_type)

        pos = 8

        val_bytes = 0

        if values != None:
            for reqValKey, val, val_bytes_size in values:
                req_bytes[pos:(pos + INT_SIZE)] = self.__val_to_bytes(reqValKey)
                pos += INT_SIZE
                
                if val != None and reqValKey != MESSAGES:
                    val_bytes = self.__get_val_bytes(val) if val_bytes_size is None else val_bytes_size

                    req_bytes[pos:(pos + val_bytes)] = self.__val_to_bytes(val, val_bytes_size)
                    pos += val_bytes
                elif val != None and reqValKey == MESSAGES:
                    req_bytes[pos:(pos + INT_SIZE)] = self.__val_to_bytes(messages_total_bytes)
                    pos += INT_SIZE

                    for message, key in val:
                        key_len = len(key) if key is not None else 0
                        message_len = len(message)

                        req_bytes[pos:(pos + INT_SIZE)] = self.__val_to_bytes(key_len)
                        pos += INT_SIZE

                        req_bytes[pos:(pos + INT_SIZE)] = self.__val_to_bytes(message_len)
                        pos += INT_SIZE

                        if key_len > 0:
                            req_bytes[pos:(pos + key_len)] = key
                            pos += key_len

                        req_bytes[pos:(pos + message_len)] = message
                        pos += message_len

        if self._conf._authentication_enable and request_needs_authentication:
            req_bytes[pos:(pos + INT_SIZE)] = self.__val_to_bytes(USERNAME)
            pos += INT_SIZE

            req_bytes[pos:(pos + INT_SIZE + len(self._conf._username))]
            pos += INT_SIZE + len(self._conf._username)

            req_bytes += (
                self.__val_to_bytes(USERNAME)
                + self.__val_to_bytes(self._conf._username)
                + self.__val_to_bytes(PASSWORD)
                + self.__val_to_bytes(self._conf._password)
            )

        return req_bytes
    
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
            print(f"Error occured while trying to connect to controlelr node {node_id}. {e}")
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

    def __get_val_bytes(self, val: Any) -> int:
        if isinstance(val, bool):
            return BOOL_SIZE
        elif isinstance(val, int):
            return INT_SIZE
        elif isinstance(val, str):
            return INT_SIZE + len(val)
        else: return 0

    def __val_to_bytes(self, val: Any, length: int | None = None) -> bytes:
        if isinstance(val, bool):
            return val.to_bytes(length=BOOL_SIZE, byteorder=ENDIAS)
        elif isinstance(val, int):
            return val.to_bytes(length=(INT_SIZE if length is None else length), byteorder=ENDIAS)
        elif isinstance(val, str):
            return len(val).to_bytes(length=INT_SIZE, byteorder=ENDIAS) + val.encode()
        else:
            raise Exception("Invalid request value of type ", type(val))
        
    def __del__(self):
        self.close()
