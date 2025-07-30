from typing import Callable, Dict, Generator
from broker_client import *
from constants import *
from responses import GetQueuePartitionInfoResponse, ProduceMessagesResponse
import threading
import time
import mmh3
import random
from socket_client import SocketClient
from lock import ReadWriteLock
import asyncio

class ProducerConf:

    """
    :param str queue: Name of the queue in which the producer will produce messages to.
    :param int wait_ms: Milliseconds to wait before sending the messages batch.
    :param int max_batch_size: Maximum batch size in bytes producer can hold locally before sending it to broker (if wait_ms > 0) (default value 16KB).
    :raise ValueError: If invalid argument is passed.
    """

    def __init__(self, queue: str, wait_ms: int = None, max_batch_size: int = 16384, max_produce_request_bytes: int = 10_000_000) -> None:
        if wait_ms is not None and wait_ms < 0:
            raise ValueError("wait_ms cannot be less than 0")
        
        if max_batch_size is not None and max_batch_size < 0:
            raise ValueError("max_batch_size cannot be less than 0")
        
        if max_produce_request_bytes  < 0:
            raise ValueError("max_produce_request_bytes cannot be less than 0")
        
        self.queue: str = queue
   
        self.max_batch_size: int = max_batch_size # default 16KB
        self.wait_ms: int = 0 if wait_ms is None else wait_ms
        self.max_produce_request_bytes = max_produce_request_bytes

class PartitionMessagesDoubleBuffer:
    def __init__(self):
        self.__messages: list[Dict[int, list[Tuple[bytes, bytes | None]]]] = [{}, {}]
        self.__write_pos: int = 0
        self.__read_pos: int = 1
        self.write_lock: ReadWriteLock = ReadWriteLock()
        self.read_lock: ReadWriteLock = ReadWriteLock()

    def append_partition_message(self, partition: int, message: Tuple[bytes, bytes | None]):
        buff = self.__messages[self.__write_pos]

        if partition not in buff:
            buff[partition] = []

        buff[partition].append(message)

    def read_partition_messages(self, partition: int) -> Generator[Tuple[bytes, bytes | None], None, None]:
        if partition in self.__messages[self.__read_pos]:
            for message_key_pair in self.__messages[self.__read_pos][partition]:
                yield message_key_pair

    def clear_read_buffer(self):
        self.__messages[self.__read_pos] = {}

    def swap_buffers(self):
        self.write_lock.acquire_write()
        self.read_lock.acquire_write()

        temp = self.__read_pos
        self.__read_pos = self.__write_pos
        self.__write_pos = temp

        self.read_lock.release_write()
        self.write_lock.release_write()

class Producer:

    def __init__(self, client: BrokerClient, conf: ProducerConf, on_delivery_callabck: Callable[[bytes, bytes | None, Exception], None] = None) -> None:
        if client._create_queue_command_run: time.sleep(5)

        self.__client: BrokerClient = client
        self.__conf: ProducerConf = conf

        self.__on_message_delivery_callback: Callable[[bytes, bytes | None, Exception], None] = on_delivery_callabck

        self.__messages: PartitionMessagesDoubleBuffer = PartitionMessagesDoubleBuffer()
        self.__produce_lock: ReadWriteLock = ReadWriteLock()

        self.__total_bytes_cached = 0
        self.__cached_bytes_lock: ReadWriteLock = ReadWriteLock()

        self.__send_messages_in_batches: bool = self.__conf.wait_ms > 0
        self.__prev_partition_sent: int = -1
        self.__seed = random.randint(100, 1000)

        self.__partitions_lock: ReadWriteLock = ReadWriteLock()
        self.__total_partitions = 0
        self.__partitions_clients: Dict[int, SocketClient | None] = {} # (Node Id - SocketClient) Pair
        self.__partitions_nodes: Dict[int, int] = {} # (Partition Id - Node Id) Pair

        self.__can_flush: bool = True
        self.__flush_lock: ReadWriteLock = ReadWriteLock()

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

    async def produce(
        self,
        message: str,
        key: str = None,
    ) -> None:
        if message is None or message == "":
            raise ValueError("Message was empty")
        
        if not self.__send_messages_in_batches:
            self.__produce_lock.acquire_write()

        ex: Exception | None = None

        try:
            await self.__produce(message=message.encode(), key=(key.encode() if key is not None and key != "" else None))
        except Exception as e:
            ex = e
        finally:
            if not self.__send_messages_in_batches:
                self.__produce_lock.release_write()

        if ex is not None: raise ex

    async def __produce(
        self,
        message: bytes,
        key: bytes | None,
    ) -> None:
        if message == None or len(message) == 0:
            raise ValueError("Message was empty")
        
        partition: int = self.__get_message_partition(key)
        self.__prev_partition_sent = partition

        self.__messages.write_lock.acquire_write()
        self.__messages.append_partition_message(partition=partition, message=(message, key))
        self.__messages.write_lock.release_write()

        total_cached_bytes = self.__increment_cached_bytes(len(message))

        if self.__conf.max_batch_size <= total_cached_bytes or not self.__send_messages_in_batches:
            await self.flush()

    async def flush(self):
        self.__flush_lock.acquire_write()

        if not self.__can_flush:
            self.__flush_lock.release_write()
            return
        
        self.__can_flush = False

        self.__flush_lock.release_write()

        ex: Exception = None

        self.__init_cached_bytes()

        self.__messages.swap_buffers()

        self.__messages.read_lock.acquire_write()

        try:
            tasks = [
                self.__flush_partition_messages(partition=partition)
                for partition in range(self.__total_partitions)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    print(f"Error occured while flushing partition messages. {result}")
                    
            self.__messages.clear_read_buffer()

        except Exception as e:
            ex = e
        finally:
            self.__messages.read_lock.release_write()

        self.__flush_lock.acquire_write()

        self.__can_flush = True

        self.__flush_lock.release_write()

        if ex is not None: raise ex

    async def __flush_partition_messages(self, partition: int):
        try:
            partition_client = self.__get_leader_node_socket_client(partition_id=partition)

            if partition_client == None:
                raise Exception(f"No leader node for partition {partition} elected yet")
            
            remaining_bytes: int = self.__conf.max_produce_request_bytes

            to_send = []

            for message, key in self.__messages.read_partition_messages(partition=partition):
                if remaining_bytes - len(message) - len(key) < 0:
                    self.__send_messages_to_partition_leader(
                        partition_client=partition_client, 
                        partition=partition, 
                        messages=to_send,
                    )
                    
                    remaining_bytes: int = self.__conf.max_produce_request_bytes
                    to_send.clear()
                else: 
                    to_send.append((message, key))
                    remaining_bytes -= (len(message) + len(key))

            if len(to_send) > 0:
                self.__send_messages_to_partition_leader(
                    partition_client=partition_client, 
                    partition=partition, 
                    messages=to_send,
                )

                to_send.clear()
        except Exception as e:
            raise e
        
    def __send_messages_to_partition_leader(self, partition_client: SocketClient, partition: int, messages: list[Tuple[bytes, bytes | None]]):
        partition_ex: Exception | None = None

        try:
            ProduceMessagesResponse(
                partition_client.send_request(
                    self.__client._create_request(
                        PRODUCE,
                        [
                            (QUEUE_NAME, self.__conf.queue),
                            (PARTITION, partition),
                            (MESSAGES, messages)
                        ]
                    )
                )
            )
        except Exception as e:
            partition_ex = e
        
        if self.__on_message_delivery_callback != None:
            for message, key in messages:
                self.__on_message_delivery_callback(message, key, partition_ex)

    def close(self):
        self.__stopped = True

        try:
            if self.__total_bytes_cached > 0:
                print("Trying to flush remaining messages before shutdown..")
                asyncio.run(self.flush())
        except Exception as e:
            print(f"Could not flush remaining messages. Reason: {e}")

        self.__client.close()

        print("Producer closed")

    def __flush_messages_batch(self):
        while not self.__stopped:
            time.sleep(self.__conf.wait_ms / 1000)
            try:
                asyncio.run(self.flush())
            except Exception as e:
                print(f"Error occured while flushing messages periodically. {e}")

    def __get_message_partition(self, key: bytes = None) -> int:
        if self.__total_partitions == 0: return -1
        
        if key == None:
            partition: int = self.__prev_partition_sent

            if partition == -1: partition = 0
            else: partition = (partition + 1) % self.__total_partitions

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
    
    def __remove_partition_nodes(self, to_keep: Set[int]) -> None:
        self.__partitions_lock.acquire_write()

        partition_ids = [i for i in range(self.__total_partitions)]

        for partition_id in partition_ids:
            if partition_id not in to_keep:
                self.__partitions_nodes[partition_id] = None

        self.__partitions_lock.release_write()
    
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

    def __get_leader_node_socket_client(self, partition_id: int) -> SocketClient | None:
        self.__partitions_lock.acquire_read()

        node_id: int = self.__partitions_nodes[partition_id] if partition_id in self.__partitions_nodes else -1

        socket_client = self.__partitions_clients[node_id] if node_id is not None and node_id in self.__partitions_clients else None

        self.__partitions_lock.release_read()

        return socket_client

    def __increment_cached_bytes(self, inc: int) -> int:
        self.__cached_bytes_lock.acquire_write()

        self.__total_bytes_cached += inc
        total_cached_bytes = self.__total_bytes_cached

        self.__cached_bytes_lock.release_write()

        return total_cached_bytes

    def __init_cached_bytes(self) -> None:
        self.__partitions_lock.acquire_write()

        self.__total_bytes_cached = 0

        self.__partitions_lock.release_write()

    def __set_partition_node(self, partition_id: int, node_id: int, if_not_exists_only: bool = False) -> None:
        self.__partitions_lock.acquire_write()

        if (if_not_exists_only and partition_id not in self.__partitions_nodes) or True:
            self.__partitions_nodes[partition_id] = node_id if node_id > 0 else None

        self.__partitions_lock.release_write()

    def __set_total_partitions(self, total_partitions: int) -> None:
        self.__partitions_lock.acquire_write()

        if self.__total_partitions == 0: self.__total_partitions = total_partitions

        self.__partitions_lock.release_write()

    def __del__(self):
        self.close()