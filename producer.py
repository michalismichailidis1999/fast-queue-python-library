from typing import Callable, Dict
from broker_client import *
from constants import *
from responses import GetQueuePartitionInfoResponse
import threading
import time
import mmh3
import random
from socket_client import SocketClient
from lock import ReadWriteLock

class ProducerConf:

    """
    :param str queue: Name of the queue in which the producer will produce messages to.
    :param int wait_ms: Milliseconds to wait before sending the messages batch.
    :param int max_batch_size: Maximum batch size in bytes producer can hold locally before sending it to broker (if wait_ms > 0) (default value 16KB).
    :raise ValueError: If invalid argument is passed.
    """

    def __init__(self, queue: str, wait_ms: int = None, max_batch_size: int = 16384) -> None:
        if wait_ms is not None and wait_ms < 0:
            raise ValueError("wait_ms cannot be less than 0")
        
        if max_batch_size is not None and max_batch_size < 0:
            raise ValueError("max_batch_size cannot be less than 0")
        
        self.queue: str = queue
   
        self.max_batch_size: int = max_batch_size # default 16KB
        self.wait_ms: int = 0 if wait_ms is None else wait_ms

class Producer:

    def __init__(self, client: BrokerClient, conf: ProducerConf) -> None:
        if client._create_queue_command_run: time.sleep(5)

        self.__client: BrokerClient = client
        self.__conf: ProducerConf = conf

        self.__partitions: Dict[
            int, list[Tuple[bytes, bytes | None, Callable[[bytes, bytes | None, Exception], None]]]
        ] = {}
        self.__total_bytes_cached = 0

        self.__messages_lock: ReadWriteLock = ReadWriteLock()
        self.__partitions_lock: ReadWriteLock = ReadWriteLock()

        self.__send_messages_in_batches: bool = self.__conf.wait_ms > 0
        self.__prev_partition_sent: int = -1
        self.__seed = random.randint(100, 1000)

        self.__total_partitions = 0
        self.__partitions_clients: Dict[int, SocketClient | None] = {}
        self.__partitions_nodes: Dict[int, int] = {}

        self.__stopped: bool = False

        self.__fetch_info_wait_time_sec: int = 10

        self.__retrieve_queue_partitions_info(5, True)

        t1 = threading.Thread(target=self.__retrieve_queue_partitions_info, args=[1, False], daemon=True)
        t1.start()

        if self.__conf.wait_ms > 0:
            t2 = threading.Thread(target=self.__flush_messages_batch, daemon=True)
            t2.start()

        print(
            f"Producer initialized for queue {self.__conf.queue}"
        )

    def __retrieve_queue_partitions_info(self, retries: int = 1, called_from_contructor: bool = False):
        initial_retries: int = retries
        success: bool = False

        while not self.__stopped:
            retries = initial_retries
            success = False

            while retries > 0:
                try:
                    leader_socket = self.__client._get_leader_node_socket_client()
                
                    if leader_socket == None:
                        raise Exception("Leader controller didn't elected yet")
                    
                    res = GetQueuePartitionInfoResponse(
                        leader_socket.send_request(
                            self.__client._create_request(GET_QUEUE_PARTITIONS_INFO, [(QUEUE_NAME, self.__conf.queue)], False)
                        )
                    )

                    for partition_id in range(res.total_partitions):
                        if partition_id not in self.__partitions_clients:
                            self.__partitions_nodes[partition_id] = None

                    self.__total_partitions = res.total_partitions

                    to_keep = set()

                    for partition_leader in res.partition_leader_nodes:
                        self.__partitions_nodes[partition_leader.partition_id] = partition_leader.node_id
                        to_keep.add(partition_leader.node_id)

                        conn_info: Tuple[str, int] | None = self.__get_leader_node_conenction_info(partition_leader.node_id)

                        if conn_info is not None and conn_info[0] == partition_leader.address and conn_info[1] == partition_leader.port:
                            continue
                        
                        self.__replace_leader_node_socket_client(
                            node_id=partition_leader.node_id, 
                            new_address=partition_leader.address, 
                            new_port=partition_leader.port,
                        )

                    self.__remove_unused_controller_nodes(to_keep)

                    if called_from_contructor:
                        print(f"Initialized queue's {self.__conf.queue} partition leader nodes")

                        for node in res.partition_leader_nodes:
                            print(node)
                            
                    retries -= 1

                    if len(list(filter(lambda x: x is not None, self.__partitions_nodes.items()))) == self.__total_partitions:
                        success = True
                        break

                    print("Not all partitions have assigned leader yet")
                except Exception as e:
                    retries -= 1

                    if called_from_contructor and retries <= 0:
                        raise Exception(f"Error occured while trying to retrieve queue's {self.__conf.queue} partitions info. {e}")
                    
                    print(f"Error occured while trying to retrieve queue's {self.__conf.queue} partitions info. {e}")
                finally:
                    if retries > 0 and not success: time.sleep(self.__fetch_info_wait_time_sec)

            if called_from_contructor: return

            time.sleep(self.__fetch_info_wait_time_sec)

    def produce(
        self,
        message: str,
        key: str = None,
        on_delivery: Callable[[bytes, bytes | None, Exception], None] = None,
    ) -> None:
        if message is None or message == "":
            raise ValueError("Message was empty")
        
        self.__produce(message=message.encode(), key=(key.encode() if key is not None and key != "" else None), on_delivery=on_delivery)

    def produce(
        self,
        message: bytes,
        key: bytes | None, 
        on_delivery: Callable[[bytes, bytes | None, Exception], None] = None,
    ) -> None:
        if message == None or len(message) == 0:
            raise ValueError("Message was empty")

        ex: Exception = None

        self.__messages_lock.acquire_write()

        try:
            partition: int = self.__get_message_partition(key)

            if partition not in self.__partitions:
                self.__partitions[partition] = []

            self.__partitions[partition].append((message, key, on_delivery))
            self.__total_bytes_cached += len(message)
            self.__prev_partition_sent = partition
        except Exception as e:
            ex = e
        finally:
            self.__messages_lock.release_write()

        if ex != None:
            raise ex

        if (
            self.__conf.max_batch_size <= self.__total_bytes_cached
            or not self.__send_messages_in_batches
        ):
            self.flush()

    def flush(self):
        ex: Exception = None

        self.__messages_lock.acquire_write()

        try:
            if len(self.__partitions.keys()) > 0:
                print("Flushing messages...")

                for partition in self.__partitions.keys():
                    if len(self.__partitions[partition]) == 0:
                        continue

                    partition_ex: Exception | None = None
                    flushed_bytes: int = 0

                    try:
                        partition_client = self._get_leader_node_socket_client(partition_id=partition)

                        if partition_client == None:
                            raise Exception(f"No leader node for partition {partition} elected yet")
                        
                        try:
                            partition_client.send_request(
                                self.__client._create_request(
                                    PRODUCE,
                                    [
                                        (QUEUE_NAME, self.__conf.queue),
                                        (PARTITION, partition),
                                    ]
                                    + [
                                        (MESSAGE, str(message))
                                        for message, _ in self.__partitions[partition]
                                    ],
                                )
                            )
                        except Exception as e:
                            partition_ex = e
                        finally:
                            for message, key, cb in self.__partitions[partition]:
                                flushed_bytes += len(message)
                                if cb != None: cb(message, key, partition_ex)
                    except Exception as e:
                        raise e

                    print(f"Flushed {flushed_bytes} bytes from partition {partition}")
                    self.__total_bytes_cached -= flushed_bytes
                    del self.__partitions[partition]
        except Exception as e:
            ex = e
        finally:
            self.__messages_lock.release_write()

        if ex is not None: raise e

    def close(self):
        self.__stopped = True

        try:
            if self.__total_bytes_cached > 0:
                print("Trying to flush remaining messages before shutdown..")
                self.flush()
        except Exception as e:
            print(f"Could not flush remaining messages. Reason: {e}")

        self.close()
        self.__client.close()

        print("Producer closed")

    def __flush_messages_batch(self):
        while not self.__stopped:
            time.sleep(self.__conf.wait_ms / 1000)
            try:
                self.flush()
            except Exception as e:
                print(f"Error occured while flushing messages periodically. {e}")

    def __get_message_partition(self, key: bytes = None) -> int:
        if key == None:
            ex: Exception = None

            self.__partitions_lock.acquire()

            partition: int = self.__prev_partition_sent

            try:
                if partition == -1:
                    partition = 0
                else:
                    partition = (partition + 1) % self.__total_partitions
            except Exception as e:
                ex = e
            finally:
                self.__partitions_lock.release()

            if ex != None:
                raise ex

            return partition
        else:
            return mmh3.hash(key=key, seed=self.__seed) % self.__total_partitions
        
    def __replace_leader_node_socket_client(self, node_id: int, new_address: str, new_port: int):
        self.__partitions_lock.acquire_write()

        try:
            if node_id in self.__partitions_clients: del self.__partitions_clients[node_id]
            self.__partitions_clients[node_id] = SocketClient(address=new_address, port=new_port, conf=self.__client._conf)
        except Exception as e:
            pass
        finally:
            self.__partitions_lock.release_write()

    def __get_leader_node_conenction_info(self, node_id: int) -> Tuple[str, int] | None:
        self.__partitions_lock.acquire_read()

        conn_info: Tuple[str, int] | None = None

        if node_id in self.__partitions_clients and self.__partitions_clients[node_id] is not None:
            conn_info = self.__partitions_clients[node_id].get_connection_info()

        self.__partitions_lock.release_read()

        return conn_info
    
    def __remove_unused_controller_nodes(self, to_keep: Set[int]):
        self.__partitions_lock.acquire_write()

        try:
            node_ids = list(self.__partitions_clients.keys())
            for node_id in node_ids:
                if node_id not in to_keep and node_id is not None:
                    del self.__partitions_clients[node_id]
        except Exception as e:
            print(f"Error occured while trying to remove unused node connection. {e}")
        finally:
            self.__partitions_lock.release_write()

    def _get_leader_node_socket_client(self, partition_id: int) -> SocketClient | None:
        self.__partitions_lock.acquire_read()

        node_id: int = self.__partitions_nodes[partition_id] if partition_id in self.__partitions_nodes else -1

        socket_client = self.__partitions_clients[node_id] if node_id in self.__partitions_clients else None

        self.__partitions_lock.release_read()

        return socket_client
        
    def __del__(self):
        self.close()