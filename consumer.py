import threading
from typing import Dict, List, Set
from exceptions import FastQueueException
from lock import ReadWriteLock
from queue_partitions_handler import QueuePartitionsHandler
from broker_client import BrokerClient
from conf import ConsumerConf
from responses import ConsumeMessagesResponse, GetConsumerAssignedPartitions, Message, RegisterConsumerResponse
from constants import *
import time

from socket_client import SocketClient

class Consumer(QueuePartitionsHandler):
    def __init__(self, client: BrokerClient, conf: ConsumerConf):
        super().__init__(client=client, consumer_conf=conf)

        self.__id: int = -1
        self.__assigned_partitions: list[int] = []

        self.__last_consumed_partition_index: int = -1

        self.__lock: ReadWriteLock = ReadWriteLock()
        self.__auto_commit_lock: ReadWriteLock = ReadWriteLock()

        self.__auto_commits: Dict[int, int] = {}

        self.__fetch_info_wait_time_sec: int = 15

        while True:
            try:
                self.__register_consumer(5, True)
            except:
                print("Consumer registration failed. Retrying again")
            finally:
                if self.__id > 0:
                    print(f"Consumer registered with id {self.__id}")
                    break

        self.__retrieve_assigned_partitions(5, True)

        self._retrieve_queue_partitions_info(5, True)

        t1 = threading.Thread(target=self._retrieve_queue_partitions_info, args=[1, False], daemon=True)
        t1.start()

        t2 = threading.Thread(target=self.__retrieve_assigned_partitions, args=[1, False], daemon=True)
        t2.start()

        t3 = threading.Thread(target=self.__register_consumer, args=[1, False], daemon=True)
        t3.start()

        if self._conf.auto_commit:
            t4 = threading.Thread(target=self.__auto_commit, daemon=True)
            t4.start()

    def __register_consumer(self, initial_retries: int = 3, called_from_constructor: bool = False):
        while not self._stopped:
            if self.__id > 0:
                time.sleep(self.__fetch_info_wait_time_sec)
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
                            time.sleep(self.__fetch_info_wait_time_sec / 3)
                            continue

                        self.__lock.acquire_write()

                        self.__id = res.consumer_id

                        self.__lock.release_write()

                        break
                    except Exception as e:
                        retries -= 1

                        time.sleep(self.__fetch_info_wait_time_sec / 3)

                        if retries <= 0:
                            raise e
            except Exception as e:
                if called_from_constructor: raise e

                print(f"Error occured while trying to register consumer. Reason: {e}")
            
            if called_from_constructor: break

            time.sleep(self.__fetch_info_wait_time_sec)

    def __retrieve_assigned_partitions(self, initial_retries: int = 3, called_from_constructor: bool = False):
        while not self._stopped:
            if self.__get_consumer_id() <= 0:
                time.sleep(self.__fetch_info_wait_time_sec)
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

                        self.__lock.acquire_write()

                        self.__assigned_partitions = res.assigned_partitions

                        self.__lock.release_write()

                        if len(res.assigned_partitions) == 0:
                            time.sleep(self.__fetch_info_wait_time_sec / 3)
                            continue

                        break
                    except Exception as e:
                        retries -= 1

                        time.sleep(self.__fetch_info_wait_time_sec / 3)

                        if retries <= 0:
                            raise e
            except Exception as e:
                if called_from_constructor: raise e

                print(f"Error occured while trying to retrieve assigned consumer partitions. Reason: {e}")

            if called_from_constructor: break

            time.sleep(self.__fetch_info_wait_time_sec)

    def poll_message(self, offset: int | None = None) -> Message | None:
        if self.__get_consumer_id() <= 0: return None

        try:
            messages: List[Message] | None = self.__poll_messages(offset=offset, only_one=True)

            return messages[0] if messages is not None and len(messages) > 0 else None
        except FastQueueException as e:
            if e.error_code == CONSUMER_UNREGISTERED:
                self.__reset_consumer()
            return None
        except:
            return None
    
    def poll_messages(self, offset: int | None = None) -> List[Message] | None:
        if self.__get_consumer_id() <= 0: return None

        try:
            return self.__poll_messages(offset=offset)
        except FastQueueException as e:
            if e.error_code == CONSUMER_UNREGISTERED:
                self.__reset_consumer()
            return None
        except:
            return None

    def __poll_messages(self, offset: int | None = None, only_one: bool = False) -> List[Message] | None:
        partition_index_to_fetch: int = -1
        partition_client: SocketClient | None = None

        total_assigned_partitions: int = self.__get_total_assigned_partitions()

        for i in range(total_assigned_partitions):
            partition_index_to_fetch = self.__get_partition_to_poll_from()

            if partition_index_to_fetch == -1: return None

            partition_client = self._get_leader_node_socket_client(partition_id=partition_index_to_fetch)

            if partition_client is not None: break

        if partition_client is None: return None

        messages = ConsumeMessagesResponse(
            partition_client.send_request(
                self._client._create_request(
                    CONSUME,
                    [
                        (QUEUE_NAME, self._conf.queue, None),
                        (CONSUMER_GROUP_ID, self._conf.group_id, None),
                        (CONSUMER_ID, self.__get_consumer_id(), LONG_LONG_SIZE),
                        (PARTITION, partition_index_to_fetch, None),
                        (MESSAGE_OFFSET, offset if offset else 0, LONG_LONG_SIZE),
                        (READ_SINGLE_OFFSET_ONLY, only_one, None)
                    ]
                )
            )
        ).messages

        if messages:
            if self._conf.auto_commit: self.__auto_commit_lock.acquire_write()

            for message in messages:
                message.partition = partition_index_to_fetch
                if self._conf.auto_commit:
                    self.__auto_commits[message.partition] = message.offset

            if self._conf.auto_commit: self.__auto_commit_lock.release_write()

        return messages
        
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

        self.client.close()

        print("Consumer closed")

    def __del__(self) -> None:
        self.close()