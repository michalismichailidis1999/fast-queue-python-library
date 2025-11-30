from typing import Callable, Dict, Generator
from .broker_client import *
from .constants import *
from .responses import ProduceMessagesResponse
import threading
import time
import mmh3
import random
from .socket_client import SocketClient
from .lock import ReadWriteLock
from .conf import ProducerConf
from .queue_partitions_handler import QueuePartitionsHandler

class PartitionMessagesDoubleBuffer:
    def __init__(self):
        self.__messages: list[Dict[int, list[Tuple[bytes, bytes | None, int]]]] = [{}, {}]
        self.__write_pos: int = 0
        self.__read_pos: int = 1
        self.write_lock: ReadWriteLock = ReadWriteLock()
        self.read_lock: ReadWriteLock = ReadWriteLock()

    def append_partition_message(self, partition: int, message: Tuple[bytes, bytes | None, int]):
        buff = self.__messages[self.__write_pos]

        if partition not in buff:
            buff[partition] = []

        buff[partition].append(message)

    def read_partition_messages(self, partition: int) -> Generator[Tuple[bytes, bytes | None, int], None, None]:
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

class Producer(QueuePartitionsHandler):

    def __init__(self, client: BrokerClient, conf: ProducerConf, on_delivery_callback: Callable[[bytes, bytes | None, Exception], None] = None) -> None:
        super().__init__(client=client, producer_conf=conf)

        self.__on_message_delivery_callback: Callable[[bytes, bytes | None, Exception], None] = on_delivery_callback

        self.__messages: PartitionMessagesDoubleBuffer = PartitionMessagesDoubleBuffer()
        self.__produce_lock: ReadWriteLock = ReadWriteLock()

        self.__total_bytes_cached: int = 0
        self.__cached_bytes_lock: ReadWriteLock = ReadWriteLock()

        self.__send_messages_in_batches: bool = self._conf.wait_ms > 0
        self.__prev_partition_sent: int = -1
        self.__seed = random.randint(100, 1000)

        self.__can_flush: bool = True
        self.__flush_lock: ReadWriteLock = ReadWriteLock()

        self.__transaction_group_id_retrieval_cb: Callable[[], int] | None = None

        while not self._all_partition_leaders_found():
            self._retrieve_queue_partitions_info(10, 2, True)

        t1 = threading.Thread(target=self._retrieve_queue_partitions_info, args=[1, 15, False], daemon=True)
        t1.start()

        if self._conf.wait_ms > 0:
            t2 = threading.Thread(target=self.__flush_messages_batch, daemon=True)
            t2.start()

        print(
            f"Producer initialized for queue {self._conf.queue}"
        )

    def produce(
        self,
        message: str,
        key: str = None,
        transaction_id: int = 0
    ) -> None:
        if message is None or message == "":
            raise ValueError("Message was empty")
        
        if not self.__send_messages_in_batches:
            self.__produce_lock.acquire_write()

        ex: Exception | None = None

        try:
            self.__produce(message=message.encode(), key=(key.encode() if key is not None and key != "" else None), transaction_id=transaction_id)
        except Exception as e:
            ex = e
        finally:
            if not self.__send_messages_in_batches:
                self.__produce_lock.release_write()

        if ex is not None: raise ex

    def __produce(
        self,
        message: bytes,
        key: bytes | None,
        transaction_id: int = 0
    ) -> None:
        if message == None or len(message) == 0:
            raise ValueError("Message was empty")
        
        partition: int = self.__get_message_partition(key)
        self.__prev_partition_sent = partition

        if self._get_leader_node_socket_client(partition_id=partition) is None:
            raise Exception(f"Leader for partition {partition} has not been elected yet")

        self.__messages.write_lock.acquire_write()
        self.__messages.append_partition_message(partition=partition, message=(message, key, transaction_id))
        self.__messages.write_lock.release_write()

        total_cached_bytes = self.__increment_cached_bytes(len(message))

        if self._conf.max_batch_size <= total_cached_bytes or not self.__send_messages_in_batches:
            self.flush()

    def flush(self):
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
            for partition in range(self._total_partitions):
                try:
                    self.__flush_partition_messages(partition=partition)
                except Exception as e:
                    print(f"Error occured while flushing partition {partition} messages. Reason: {e}")
                    
            self.__messages.clear_read_buffer()

        except Exception as e:
            ex = e
        finally:
            self.__messages.read_lock.release_write()

        self.__flush_lock.acquire_write()

        self.__can_flush = True

        self.__flush_lock.release_write()

        if ex is not None: raise ex

    def __flush_partition_messages(self, partition: int):
        try:
            partition_client = self._get_leader_node_socket_client(partition_id=partition)

            if partition_client == None:
                raise Exception(f"No leader node for partition {partition} elected yet")
            
            remaining_bytes: int = self._conf.max_produce_request_bytes

            to_send: list[Tuple[bytes, bytes | None, int]] = []

            for message, key, transaction_id in self.__messages.read_partition_messages(partition=partition):
                if remaining_bytes - len(message) - (len(key) if key is not None else 0) - LONG_LONG_SIZE < 0:
                    self.__send_messages_to_partition_leader(
                        partition_client=partition_client, 
                        partition=partition, 
                        messages=to_send,
                    )
                    
                    remaining_bytes: int = self._conf.max_produce_request_bytes
                    to_send.clear()
                else: 
                    to_send.append((message, key, transaction_id))
                    remaining_bytes -= (len(message) + (len(key) if key is not None else 0) + LONG_LONG_SIZE)

            if len(to_send) > 0:
                self.__send_messages_to_partition_leader(
                    partition_client=partition_client, 
                    partition=partition, 
                    messages=to_send,
                )

                to_send.clear()
        except Exception as e:
            raise e
        
    def __send_messages_to_partition_leader(self, partition_client: SocketClient, partition: int, messages: list[Tuple[bytes, bytes | None, int]]):
        partition_ex: Exception | None = None

        transaction_group_id: int = 0

        if self.__transaction_group_id_retrieval_cb is not None:
            transaction_group_id = self.__transaction_group_id_retrieval_cb()

            if transaction_group_id <= 0:
                raise Exception(f"Cannot flush partition's {partition} messages. Invalid transaction group id {transaction_group_id}")

        try:
            ProduceMessagesResponse(
                partition_client.send_request(
                    self._client._create_request(
                        PRODUCE,
                        [
                            (QUEUE_NAME, self._conf.queue, None),
                            (PARTITION, partition, None),
                            (TRANSACTION_GROUP_ID, transaction_group_id, LONG_LONG_SIZE),
                            (MESSAGES, messages, None)
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
        self._stopped = True

        try:
            if self.__total_bytes_cached > 0:
                print("Trying to flush remaining messages before shutdown..")
                self.flush()
        except Exception as e:
            print(f"Could not flush remaining messages. Reason: {e}")

        self._client.close()

        print("Producer closed")

    def __flush_messages_batch(self):
        while not self._stopped:
            time.sleep(self._conf.wait_ms / 1000)
            try:
                self.flush()
            except Exception as e:
                print(f"Error occured while flushing messages periodically. {e}")

    def __get_message_partition(self, key: bytes = None) -> int:
        if key == None:
            partition: int = self.__prev_partition_sent

            if partition == -1: partition = 0
            else: partition = (partition + 1) % self._total_partitions

            return partition
        else:
            return mmh3.hash(key=key, seed=self.__seed) % self._total_partitions

    def __increment_cached_bytes(self, inc: int) -> int:
        self.__cached_bytes_lock.acquire_write()

        self.__total_bytes_cached += inc
        total_cached_bytes = self.__total_bytes_cached

        self.__cached_bytes_lock.release_write()

        return total_cached_bytes

    def __init_cached_bytes(self) -> None:
        self._partitions_lock.acquire_write()

        self.__total_bytes_cached = 0

        self._partitions_lock.release_write()

    def __del__(self):
        self.close()

    def _set_transaction_group_id_retrieval_cb(self, cb: Callable[[], int]) -> None:
        self.__transaction_group_id_retrieval_cb = cb