import threading
from typing import Dict, List
from lock import ReadWriteLock
from queue_partitions_handler import QueuePartitionsHandler
from broker_client import BrokerClient
from conf import ConsumerConf
import asyncio
from responses import ConsumeMessagesResponse, GetConsumerAssignedPartitions, Message, RegisterConsumerResponse
from constants import *
import time

class Consumer(QueuePartitionsHandler):
    def __init__(self, client: BrokerClient, conf: ConsumerConf):
        if client._create_queue_command_run: time.sleep(10)

        super().__init__(client=client, consumer_conf=conf)

        self.__id: int = -1
        self.__assigned_partitions: list[int] = []

        self.__last_consumed_partition_index: int = -1

        self.__lock: ReadWriteLock = ReadWriteLock()
        self.__auto_commit_lock: ReadWriteLock = ReadWriteLock()

        self.__auto_commits: Dict[int, int] = {}

        self.__register_consumer()

        self._retrieve_queue_partitions_info(5, True)

        t1 = threading.Thread(target=self._retrieve_queue_partitions_info, args=[1, False], daemon=True)
        t1.start()

        t2 = threading.Thread(target=self.__retrieve_assigned_partitions, daemon=True)
        t2.start()

    def __register_consumer(self):
        while True:
            try:
                leader_node = self._client._get_leader_node_socket_client()

                if leader_node is None:
                    raise Exception("No controller leader elected yet")

                res = RegisterConsumerResponse(
                    leader_node.send_request(
                        self._client._create_request(
                            REGISTER_CONSUMER,
                            [
                                (QUEUE_NAME, self._conf.queue),
                                (CONSUMER_GROUP_ID, self._conf.group_id),
                                (CONSUME_FROM, True if self._conf.consume_from == CONSUME_EARLIEST else False)
                            ]
                        )
                    )
                )

                if not res.success or res.consumer_id <= 0:
                    raise Exception("Could not register consumer")

                self.__lock.acquire_write()

                self.__id = res.consumer_id

                self.__lock.release_write()

                break
            except Exception as e:
                print(f"Error occured while trying to register consumer. Reason: {e}")
                time.sleep(3)

    def __retrieve_assigned_partitions(self):
        retries: int = 3

        while not self._stopped:
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
                                        (QUEUE_NAME, self._conf.queue),
                                        (CONSUMER_GROUP_ID, self._conf.group_id),
                                        (CONSUMER_ID, self.__id)
                                    ]
                                )
                            )
                        )

                        self.__lock.acquire_write()

                        self.__assigned_partitions = res.assigned_partitions

                        self.__lock.release_write()

                        if len(res.assigned_partitions):
                            print("No assigned partitions yet")
                            time.sleep(2)
                    except Exception as e:
                        retries -= 1

                        if retries <= 0: raise e
            except Exception as e:
                print(f"Error occured while trying to retrieve assigned consumer partitions. Reason: {e}")

    def poll_message(self, offset: int | None = None) -> Message | None:
        messages: List[Message] | None = self.__poll_messages(offset=offset, only_one=True)

        return messages[0] if messages is not None and len(messages) > 0 else None
    
    def poll_messages(self, offset: int | None = None) -> List[Message] | None:
        return self.__poll_messages(offset=offset)

    def __poll_messages(self, offset: int | None = None, only_one: bool = False) -> List[Message] | None:
        partition_index_to_fetch: int = self.__get_partition_to_poll_from()

        partition_client = self._get_leader_node_socket_client(partition_id=partition_index_to_fetch)

        if partition_client is None:
            raise Exception("No leader has been elected for partition yet")
        
        messages = ConsumeMessagesResponse(partition_client.send_request(
            self._client._create_request(
                CONSUME,
                [
                    (QUEUE_NAME, self._conf.queue),
                    (CONSUMER_GROUP_ID, self._conf.group_id),
                    (CONSUMER_ID, self.__id),
                    (PARTITION, partition_index_to_fetch),
                    (MESSAGE_OFFSET, offset if offset else 0),
                    (READ_SINGLE_OFFSET_ONLY, only_one)
                ]
            )
        )).messages

        if messages:
            for message in messages:
                message.partition = partition_index_to_fetch

        return messages
        
    async def ack(self, offset: int, partition: int) -> None:
        partition_client = self._get_leader_node_socket_client(partition_id=partition)

        if partition_client is None:
            raise Exception("No leader has been elected for partition yet")
        
        partition_client.send_request(
            self._client._create_request(
                ACK,
                [
                    (QUEUE_NAME, self._conf.queue),
                    (CONSUMER_GROUP_ID, self._conf.group_id),
                    (CONSUMER_ID, self.__id),
                    (PARTITION, partition),
                    (MESSAGE_OFFSET, offset)
                ]
            )
        )

    def __get_partition_to_poll_from(self) -> int:
        self.__lock.acquire_write()

        partition_index_to_fetch: int = self.__last_consumed_partition_index + 1

        if partition_index_to_fetch >= len(self.__assigned_partitions):
            partition_index_to_fetch = 0

        self.__last_consumed_partition_index = partition_index_to_fetch

        self.__lock.release_write()

    def __auto_commit(self):
        while not self._stopped:
            self.__auto_commit_lock.acquire_write()

            try:
                for partition, offset in self.__auto_commits.values():
                    self.ack(offset=offset, partition=partition)
            except Exception as e:
                print(f"Error occured while auto commiting offsets. Reason: {e}")

            self.__auto_commit_lock.release_write()

            time.sleep(self._conf.auto_commit_interval_ms / 1000)

    def close(self):
        self.stopped = True

        self.client.close()

        print("Consumer closed")

    def __del__(self):
        self.close()