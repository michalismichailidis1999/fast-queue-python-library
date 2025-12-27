import threading
from typing import Dict, List, Set
from queue import Queue
from .exceptions import FastQueueException
from .lock import ReadWriteLock
from .queue_partitions_handler import QueuePartitionsHandler
from .broker_client import BrokerClient
from .conf import ConsumerConf
from .responses import ConsumeMessagesResponse, GetConsumerAssignedPartitions, Message, RegisterConsumerResponse
from .constants import *
import time

from .socket_client import SocketClient

class Consumer(QueuePartitionsHandler):
    def __init__(self, client: BrokerClient, conf: ConsumerConf):
        super().__init__(client=client, consumer_conf=conf)

        self.__id: int = -1
        self.__assigned_partitions: list[int] = []

        self.__last_consumed_partition_index: int = -1

        self.__lock: ReadWriteLock = ReadWriteLock()
        self.__auto_commit_lock: ReadWriteLock = ReadWriteLock()

        self.__auto_commits: Dict[int, int] = {}

        self.__consume_condition_lock: threading.Condition = threading.Condition(threading.Lock())
        self.__messages_for_consumption: Queue[Message] = Queue()
        self.__total_consumed_messages: int = 0
        self.__total_consumed_messages_bytes: int = 0

        while not self._all_partition_leaders_found():
            self._retrieve_queue_partitions_info(10, 2, True)

        while self.__get_consumer_id() <= 0:
            try:
                self.__register_consumer(10, 2, True)
            except:
                print("Consumer registration failed. Retrying again...")
            
            if self.__get_consumer_id() <= 0:
                time.sleep(10)

        t1 = threading.Thread(target=self._retrieve_queue_partitions_info, args=[1, 15, False], daemon=True)
        t1.start()

        t2 = threading.Thread(target=self.__retrieve_assigned_partitions, args=[1, 15, False], daemon=True)
        t2.start()

        t3 = threading.Thread(target=self.__register_consumer, args=[1, 15, False], daemon=True)
        t3.start()

        if self._conf.auto_commit:
            t4 = threading.Thread(target=self.__auto_commit, daemon=True)
            t4.start()

        t5 = threading.Thread(target=self.__retrieve_messages_from_all_assigned_partitions, daemon=True)
        t5.start()

    def __register_consumer(self, initial_retries: int, time_to_wait: int, called_from_constructor: bool = False):
        while not self._stopped:
            if self.__get_consumer_id() > 0:
                if called_from_constructor: break

                time.sleep(time_to_wait)
                continue

            retries: int = initial_retries

            try:
                while retries > 0:
                    try:
                        leader_node = self._client._get_leader_node_socket_client()

                        if leader_node is None:
                            raise Exception("No controller leader elected yet")

                        res = RegisterConsumerResponse(
                            leader_node.send_request(
                                self._client._create_request(
                                    REGISTER_CONSUMER,
                                    [
                                        (QUEUE_NAME, self._conf.queue, None),
                                        (CONSUMER_GROUP_ID, self._conf.group_id, None),
                                        (CONSUME_FROM, True if self._conf.consume_from == CONSUME_EARLIEST else False, None)
                                    ]
                                )
                            )
                        )

                        if not res.success or res.consumer_id <= 0:
                            raise Exception("Unsuccessfull consumer registration")

                        self.__set_consumer_id(res.consumer_id)

                        break
                    except Exception as e:
                        retries -= 1

                        if retries <= 0: raise e
                        else:
                            time.sleep(time_to_wait)
            except Exception as e:
                if called_from_constructor: raise e

                print(f"Error occured while trying to register consumer. Reason: {e}")
            
            if called_from_constructor: break

            time.sleep(time_to_wait)

    def __retrieve_assigned_partitions(self, initial_retries: int, time_to_wait, called_from_constructor: bool = False):
        while not self._stopped:
            if self.__get_consumer_id() <= 0 and not called_from_constructor:
                time.sleep(time_to_wait)
                continue

            retries: int = initial_retries

            try:
                while retries > 0:
                    try:
                        leader_node = self._client._get_leader_node_socket_client()

                        if leader_node is None:
                            raise Exception("No controller leader elected yet")
                
                        res = GetConsumerAssignedPartitions(
                            leader_node.send_request(
                                self._client._create_request(
                                    GET_CONSUMER_ASSIGNED_PARTITIONS,
                                    [
                                        (QUEUE_NAME, self._conf.queue, None),
                                        (CONSUMER_GROUP_ID, self._conf.group_id, None),
                                        (CONSUMER_ID, self.__get_consumer_id(), LONG_LONG_SIZE)
                                    ]
                                )
                            )
                        )

                        self.__set_assigned_partitions(res.assigned_partitions)

                        if len(res.assigned_partitions) == 0:
                            retries -= 1
                            time.sleep(time_to_wait)
                            continue

                        break
                    except Exception as e:
                        retries -= 1

                        if retries <= 0: raise e
                        else:
                            time.sleep(time_to_wait)
            except Exception as e:
                if called_from_constructor: raise e

                print(f"Error occured while trying to retrieve assigned consumer partitions. Reason: {e}")

            if called_from_constructor: break

            time.sleep(time_to_wait)

    def poll(self, timeout: int = 2, max_messages_to_receive: int = 50) -> List[Message] | None:
        with self.__consume_condition_lock:
            self.__consume_condition_lock.wait_for(predicate=lambda: self.__messages_for_consumption.qsize() == 0, timeout=timeout)

            messages: List[Message] = []

            while max_messages_to_receive > 0 and self.__messages_for_consumption.qsize() > 0:
                message: Message | None = None

                try:
                    message = self.__messages_for_consumption.get()
                except:
                    message = None
                
                if message is not None:
                    messages.append(message)
                else:
                    break

            return messages if len(messages) > 0 else None
        
    def fetch_messages_from(self, partition: int, offset: int) -> List[Message] | None:
        return self.__fetch_messages_from(partition=partition, offset=offset, single_message=False)

    def fetch_message_from(self, partition: int, offset: int) -> Message | None:
        messages = self.__fetch_messages_from(partition=partition, offset=offset, single_message=True)

        if not messages: return None

        return messages[0]

    def __fetch_messages_from(self, partition: int, offset: int, single_message: bool = False) -> List[Message] | None:
        return self.__consume_messages_from_node(partition=partition, offset=offset, single_message=single_message)
        
    def ack(self, offset: int, partition: int) -> None:
        if self.__get_consumer_id() <= 0: return None

        partition_client = self._get_leader_node_socket_client(partition_id=partition)

        if partition_client is None:
            raise Exception("Could not commit message offset, no leader has been elected for partition yet")
        
        try:
            partition_client.send_request(
                self._client._create_request(
                    ACK,
                    [
                        (QUEUE_NAME, self._conf.queue, None),
                        (CONSUMER_GROUP_ID, self._conf.group_id, None),
                        (CONSUMER_ID, self.__get_consumer_id(), LONG_LONG_SIZE),
                        (PARTITION, partition, None),
                        (MESSAGE_OFFSET, offset, LONG_LONG_SIZE)
                    ]
                )
            )
        except FastQueueException as e:
            if e.error_code == CONSUMER_UNREGISTERED:
                self.__reset_consumer()
                return

            raise e
        except Exception as e:
            raise e

    def __get_total_assigned_partitions(self):
        self.__lock.acquire_write()

        total_assigned_partitions: int = len(self.__assigned_partitions)

        self.__lock.release_write()

        return total_assigned_partitions

    def __get_partition_to_poll_from(self) -> int:
        self.__lock.acquire_write()

        if len(self.__assigned_partitions) == 0: return -1

        partition_index_to_fetch: int = self.__last_consumed_partition_index + 1

        if partition_index_to_fetch >= len(self.__assigned_partitions):
            partition_index_to_fetch = 0

        self.__last_consumed_partition_index = partition_index_to_fetch

        self.__lock.release_write()

        return partition_index_to_fetch

    def __auto_commit(self):
        while not self._stopped:
            self.__auto_commit_lock.acquire_write()

            to_remove: Set[int] = set()

            try:
                for partition, offset in self.__auto_commits.items():
                    self.ack(offset=offset, partition=partition)
                    to_remove.add(partition)
            except Exception as e:
                print(f"Error occured while auto commiting offsets. Reason: {e}")

            for partition in to_remove:
                del self.__auto_commits[partition]

            self.__auto_commit_lock.release_write()

            time.sleep(self._conf.auto_commit_interval_ms / 1000)

    def __get_consumer_id(self) -> int:
        self.__lock.acquire_write()

        id: int = self.__id

        self.__lock.release_write()

        return id

    def __reset_consumer(self) -> None:
        self.__lock.acquire_write()

        self.__id = -1
        self.__assigned_partitions = []

        self.__lock.release_write()

    def close(self) -> None:
        self.stopped = True

        self._client.close()

        print("Consumer closed")

    def __del__(self) -> None:
        self.close()

    def __get_consumer_id(self) -> int:
        self.__lock.acquire_read()

        consumer_id: int = self.__id

        self.__lock.release_read()

        return consumer_id
    
    def __set_consumer_id(self, consumer_id: int) -> None:
        self.__lock.acquire_write()

        self.__id = consumer_id

        self.__lock.release_write()
    
    def __set_assigned_partitions(self, assigned_partitions: List[int]) -> None:
        self.__lock.acquire_write()

        self.__assigned_partitions = assigned_partitions

        self.__lock.release_write()

    def __retrieve_messages_from_all_assigned_partitions(self):
        while not self._stopped:
            self.__lock.acquire_read()
            first_skipped_partition = -1

            try:
                for partition in self.__assigned_partitions:
                    if self._conf.max_queued_messages > 0 and self.__total_consumed_messages >= self._conf.max_queued_messages:
                        first_skipped_partition = partition if first_skipped_partition == -1 else first_skipped_partition
                        continue

                    if self._conf.max_queued_messages_bytes > 0 and self.__total_consumed_messages_bytes >= self._conf.max_queued_messages_bytes:
                        first_skipped_partition = partition if first_skipped_partition == -1 else first_skipped_partition
                        continue

                    messages = self.__consume_messages_from_node(partition=partition, offset=0, single_message=False)

                    if messages:
                        if self._conf.auto_commit: self.__auto_commit_lock.acquire_write()

                        for message in messages:
                            message.partition = partition

                            self.__messages_for_consumption.put(message)
                            self.__total_consumed_messages += 1
                            self.__total_consumed_messages_bytes += message.get_message_total_bytes()

                            if self._conf.auto_commit:
                                self.__auto_commits[message.partition] = message.offset

                        if self._conf.auto_commit: self.__auto_commit_lock.release_write()

                        self.__consume_condition_lock.notify_all()
            except Exception as e:
                print(f"Failed to retrieve messages from assigned partitions. Reason: {e}")
            finally:
                self.__lock.release_read()

            if first_skipped_partition != -1:
                self.__lock.acquire_write()

                index_to_rotate = 0

                for i in range(len(self.__assigned_partitions)):
                    if self.__assigned_partitions[i] == first_skipped_partition:
                        index_to_rotate = i
                        break
                
                if index_to_rotate > 0:
                    part_a = self.__assigned_partitions[index_to_rotate:]
                    part_b = self.__assigned_partitions[:index_to_rotate]

                    self.__assigned_partitions = part_a + part_b

                self.__lock.release_write()
            
            time.sleep(self._conf.poll_frequency_ms / 1000)

    def __consume_messages_from_node(self, partition: int, offset: int, single_message: bool) -> List[Message] | None:
        partition_client = self._get_leader_node_socket_client(partition_id=partition)

        if partition_client is None:
            return None

        return ConsumeMessagesResponse(
            partition_client.send_request(
                self._client._create_request(
                    CONSUME,
                    [
                        (QUEUE_NAME, self._conf.queue, None),
                        (CONSUMER_GROUP_ID, self._conf.group_id, None),
                        (CONSUMER_ID, self.__get_consumer_id(), LONG_LONG_SIZE),
                        (PARTITION, partition, None),
                        (MESSAGE_OFFSET, offset, LONG_LONG_SIZE),
                        (READ_SINGLE_OFFSET_ONLY, single_message, None)
                    ]
                )
            )
        ).messages