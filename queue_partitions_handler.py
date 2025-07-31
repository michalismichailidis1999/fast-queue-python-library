from conf import ConsumerConf, ProducerConf
from socket_client import SocketClient
from broker_client import BrokerClient
from lock import ReadWriteLock
from typing import Dict, Tuple, Set
import time
from responses import GetQueuePartitionInfoResponse, GetLeaderControllerIdResponse
from constants import *

class QueuePartitionsHandler:
    def __init__(self, client: BrokerClient, producer_conf: ProducerConf | None = None, consumer_conf: ConsumerConf | None = None):
        if (producer_conf is None and consumer_conf is None) or (producer_conf is not None and consumer_conf is not None):
            raise ValueError("Passed configuration must be either for producer or consumer")
        
        self._client: BrokerClient = client
        self._conf: ProducerConf | ConsumerConf = producer_conf if producer_conf is not None else consumer_conf

        self._partitions_lock: ReadWriteLock = ReadWriteLock()
        self._total_partitions = 0
        self._partition_clients: Dict[int, SocketClient | None] = {} # (Node Id - SocketClient) Pair
        self._partitions_nodes: Dict[int, int] = {} # (Partition Id - Node Id) Pair

        self.__fetch_info_wait_time_sec: int = 10

        self._stopped: bool = False

    def _retrieve_queue_partitions_info(self, retries: int = 1, called_from_contructor: bool = False):
        initial_retries: int = retries
        success: bool = False

        while not self._stopped:
            retries = initial_retries
            success = False

            while retries > 0:
                try:
                    leader_socket = self._client._get_leader_node_socket_client()
                
                    if leader_socket == None:
                        raise Exception("Leader controller didn't elected yet")
                    
                    res = GetQueuePartitionInfoResponse(
                        leader_socket.send_request(
                            self._client._create_request(GET_QUEUE_PARTITIONS_INFO, [(QUEUE_NAME, self._conf.queue)], False)
                        )
                    )

                    for partition_id in range(res.total_partitions):
                        self.__set_partition_node(partition_id=partition_id, node_id=-1, if_not_exists_only=True)

                    self.__set_total_partitions(res.total_partitions)

                    to_keep = set()
                    partitions_to_keep = set()

                    for partition_leader in res.partition_leader_nodes:
                        to_keep.add(partition_leader.node_id)
                        partitions_to_keep.add(partition_leader.partition_id)

                        conn_info: Tuple[str, int] | None = self.__get_leader_node_conenction_info(partition_leader.node_id)

                        if conn_info is not None and conn_info[0] == partition_leader.address and conn_info[1] == partition_leader.port:
                            self.__set_partition_node(partition_id=partition_leader.partition_id, node_id=partition_leader.node_id)
                            continue
                        
                        self.__replace_leader_node_socket_client(
                            node_id=partition_leader.node_id, 
                            new_address=partition_leader.address, 
                            new_port=partition_leader.port,
                        )

                        self.__set_partition_node(partition_id=partition_leader.partition_id, node_id=partition_leader.node_id)

                    self.__remove_partition_nodes(partitions_to_keep)
                    self.__remove_unused_controller_nodes(to_keep)

                    if called_from_contructor:
                        print(f"Initialized queue's {self._conf.queue} partition leader nodes")

                        for node in res.partition_leader_nodes:
                            print(node)
                            
                    retries -= 1

                    if len(list(filter(lambda x: x is not None, self._partitions_nodes.items()))) == self._total_partitions:
                        success = True
                        break

                    print("Not all partitions have assigned leader yet")
                except Exception as e:
                    retries -= 1

                    if called_from_contructor and retries <= 0:
                        raise Exception(f"Error occured while trying to retrieve queue's {self._conf.queue} partitions info. {e}")
                    
                    print(f"Error occured while trying to retrieve queue's {self._conf.queue} partitions info. {e}")
                finally:
                    if retries > 0 and not success: time.sleep(self.__fetch_info_wait_time_sec)

            if called_from_contructor: return

            time.sleep(self.__fetch_info_wait_time_sec)

    def _get_leader_node_socket_client(self, partition_id: int) -> SocketClient | None:
        self._partitions_lock.acquire_read()

        node_id: int = self._partitions_nodes[partition_id] if partition_id in self._partitions_nodes else -1

        socket_client = self._partition_clients[node_id] if node_id is not None and node_id in self._partition_clients else None

        self._partitions_lock.release_read()

        return socket_client

    def __set_partition_node(self, partition_id: int, node_id: int, if_not_exists_only: bool = False) -> None:
        self._partitions_lock.acquire_write()

        if (if_not_exists_only and partition_id not in self._partitions_nodes) or True:
            self._partitions_nodes[partition_id] = node_id if node_id > 0 else None

        self._partitions_lock.release_write()

    def __set_total_partitions(self, total_partitions: int) -> None:
        self._partitions_lock.acquire_write()

        if self._total_partitions == 0: self._total_partitions = total_partitions

        self._partitions_lock.release_write()

    def __replace_leader_node_socket_client(self, node_id: int, new_address: str, new_port: int):
        self._partitions_lock.acquire_write()

        try:
            if node_id in self._partition_clients: del self._partition_clients[node_id]
            self._partition_clients[node_id] = SocketClient(address=new_address, port=new_port, conf=self._client._conf)
        except Exception as e:
            pass
        finally:
            self._partitions_lock.release_write()

    def __get_leader_node_conenction_info(self, node_id: int) -> Tuple[str, int] | None:
        self._partitions_lock.acquire_read()

        conn_info: Tuple[str, int] | None = None

        if node_id in self._partition_clients and self._partition_clients[node_id] is not None:
            conn_info = self._partition_clients[node_id].get_connection_info()

        self._partitions_lock.release_read()

        return conn_info
    
    def __remove_partition_nodes(self, to_keep: Set[int]) -> None:
        self._partitions_lock.acquire_write()

        partition_ids = [i for i in range(self._total_partitions)]

        for partition_id in partition_ids:
            if partition_id not in to_keep:
                self._partitions_nodes[partition_id] = None

        self._partitions_lock.release_write()
    
    def __remove_unused_controller_nodes(self, to_keep: Set[int]):
        self._partitions_lock.acquire_write()

        try:
            node_ids = list(self._partition_clients.keys())
            for node_id in node_ids:
                if node_id not in to_keep and node_id is not None:
                    del self._partition_clients[node_id]
        except Exception as e:
            print(f"Error occured while trying to remove unused node connection. {e}")
        finally:
            self._partitions_lock.release_write()