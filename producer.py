from typing import Callable, Dict
from broker_client import *
from constants import *
from responses import GetQueuePartitionInfoResponse
import threading
import time
import mmh3
import random
from socket_client import SocketClient

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
        self.client: BrokerClient = client
        self.conf: ProducerConf = conf

        self.partitions: Dict[
            int, list[(bytes, bytes, Callable[[Exception, bytes, bytes, int], None])]
        ] = {}
        self.total_bytes_cached = 0

        self.messages_mut: threading.Lock = threading.Lock()
        self.partitions_mut: threading.Lock = threading.Lock()
        self.partitions_info_mut: threading.Lock = threading.Lock()

        self.__send_messages_in_batches: bool = self.conf.wait_ms > 0
        self.__prev_partition_sent: int = -1
        self.__seed = random.randint(100, 1000)

        self.__total_partitions = 0
        self.__partitions_clients: Dict[int, SocketClient | None] = {}
        self.__partitions_nodes: Dict[int, int] = {}

        self.__stopped: bool = False

        self.__retrieve_queue_partitions_info(5, True)

        t1 = threading.Thread(target=self.__retrieve_queue_partitions_info, args=[1, False], daemon=True)
        t1.start()

        if self.conf.wait_ms > 0:
            t2 = threading.Thread(target=self.__flush_messages_batch, daemon=True)
            t2.start()

        print(
            f"Producer initialized for queue {self.conf.queue}"
        )

    def __retrieve_queue_partitions_info(self, retries: int = 1, called_from_contructor: bool = False):
        while not self.__stopped:
            self.partitions_info_mut.acquire()
            
            try:
                while retries > 0:
                    leader_socket = self.client.get_leader_socket()
                    
                    if leader_socket == None:
                        raise Exception("Leader controller didn't elected yet")
                    
                    res = GetQueuePartitionInfoResponse(
                        leader_socket.send_request(
                            self.create_request(GET_QUEUE_PARTITIONS_INFO, [], False)
                        )
                    )

                    for partition_id in range(res.total_partitions):
                        if partition_id not in self.__partitions_clients:
                            self.__partitions_clients[partition_id] = None

                    to_keep = set()

                    for partition_leader in res.partition_leader_nodes:
                        if partition_leader.node_id in self.__partitions_clients:
                            conn_info = self.__partitions_clients[partition_leader.node_id].get_connection_info()
                            if conn_info[0] != partition_leader.address and conn_info[1] != partition_leader.port:
                                del self.__partitions_clients[partition_leader.node_id]
                                self.__partitions_clients[partition_leader.node_id] = SocketClient(
                                    partition_leader.address, partition_leader.port, self.client.conf
                                )
                        else:
                            self.__partitions_clients[partition_leader.node_id] = SocketClient(
                                partition_leader.address, partition_leader.port, self.client.conf
                            )

                        self.__partitions_nodes[partition_leader.partition_id] = partition_leader.node_id

                        to_keep.add(partition_leader.node_id)

                    for node_id in self.__partitions_clients.keys():
                        if node_id not in to_keep:
                            del self.__partitions_clients[node_id]

                    if called_from_contructor:
                        print(f"Initialized queue's {self.conf.queue} partition leader nodes")

                        for node in res.partition_leader_nodes:
                            print(node)

                    not_initialized_leaders = []

                    if len(filter(lambda x: x is not None, self.__partitions_clients.items())) == res.total_partitions: break

                    print("Not all partitions have assigned leader yet")

                    retries -= 1
                    
                    if retries > 0: time.sleep(3)
            except Exception as e:
                if called_from_contructor: raise e
                print(f"Error occured while trying to fetch queue's {self.conf.queue} partitions info. {e}")
            finally:
                self.partitions_info_mut.release()

            if called_from_contructor: return

            time.sleep(10)

    def produce(
        self,
        message: str,
        key: str = None,
        on_delivery: Callable[[Exception, bytes, int], None] = None,
    ) -> None:
        if message is None or message == "":
            raise ValueError("Message was empty")
        
        self.__produce(message=message.encode(), key=(key.encode() if key is not None and key != "" else None), on_delivery=on_delivery)

    def produce(
        self,
        message: bytes,
        key: bytes, 
        on_delivery: Callable[[Exception, bytes, int], None] = None,
    ) -> None:
        if message == None or len(message) == 0:
            raise ValueError("Message was empty")

        ex: Exception = None

        self.messages_mut.acquire()

        try:
            partition: int = self.__get_message_partition(key)

            if partition not in self.partitions:
                self.partitions[partition] = []

            self.partitions[partition].append((message, key, on_delivery))
            self.total_bytes_cached += message.get_total_bytes()
            self.__prev_partition_sent = partition
        except Exception as e:
            ex = e
        finally:
            self.messages_mut.release()

        if ex != None:
            raise ex

        if (
            self.conf.max_batch_size <= self.total_bytes_cached
            or not self.__send_messages_in_batches
        ):
            self.flush()

    def flush(self):
        ex: Exception = None

        self.messages_mut.acquire()

        try:
            if len(self.partitions.keys()) > 0:
                print("Flushing messages...")

                for partition in self.partitions.keys():
                    if len(self.partitions[partition]) == 0:
                        continue

                    partition_ex: Exception = None
                    flushed_bytes: int = 0

                    self.partitions_info_mut.acquire()

                    try:
                        if partition not in self.__partitions_clients:
                            raise Exception(f"Error in partition {partition} connection pool retrieval")
                        
                        partition_client = self.__partitions_clients[partition]

                        if partition_client == None:
                            raise Exception(f"No leader node for partition {partition} elected yet")

                        partition_client.send_request(
                            self.client.create_request(
                                PRODUCE,
                                [
                                    (QUEUE_NAME, self.conf.queue),
                                    (PARTITION, partition),
                                ]
                                + [
                                    (MESSAGE, str(message))
                                    for message, _ in self.partitions[partition]
                                ],
                            )
                        )
                    except Exception as e:
                        partition_ex = e
                    finally:
                        self.partitions_info_mut.release()

                        for message, key, cb in self.partitions[partition]:
                            try:
                                message_bytes: int = message.get_total_bytes()
                                flushed_bytes += message_bytes
                                if cb != None:
                                    cb(partition_ex, message, key, message_bytes)
                            except Exception as e:
                                print(f"Error occured while executing message callback. {e}")

                    print(f"Flushed {flushed_bytes} bytes from partition {partition}")

                    self.total_bytes_cached -= flushed_bytes
                    del self.partitions[partition]

                    print("Messages flushed")
        except Exception as e:
            ex = e
        finally:
            self.messages_mut.release()

    def close(self):
        self.__stopped = True

        try:
            if self.total_bytes_cached > 0:
                print("Trying to flush remaining messages before shutdown..")
                self.flush()
        except Exception as e:
            print(f"Could not flush remaining messages. Reason: {e}")

        self.client.close()

        print("Producer closed")

    def __flush_messages_batch(self):
        while not self.__stopped:
            time.sleep(self.conf.wait_ms / 1000)
            self.flush()

    def __get_message_partition(self, key: bytes = None) -> int:
        if key == None:
            ex: Exception = None

            self.partitions_mut.acquire()

            partition: int = self.__prev_partition_sent

            try:
                if partition == -1:
                    partition = 0
                else:
                    partition = (partition + 1) % self.__total_partitions
            except Exception as e:
                ex = e
            finally:
                self.partitions_mut.release()

            if ex != None:
                raise ex

            return partition
        else:
            return mmh3.hash(key=key, seed=self.__seed) % self.__total_partitions
        
    def __del__(self):
        self.close()