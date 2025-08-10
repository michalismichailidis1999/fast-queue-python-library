import threading
from typing import Callable, List
from lock import ReadWriteLock
from queue_partitions_handler import QueuePartitionsHandler
from broker_client import BrokerClient
from conf import ConsumerConf
import asyncio
from responses import GetConsumerAssignedPartitions, RegisterConsumerResponse
from constants import *
import time

class Message:
    def __init__(self):
        pass

class Consumer(QueuePartitionsHandler):
    def __init__(self, client: BrokerClient, conf: ConsumerConf):
        super().__init__(client=client, consumer_conf=conf)

        self.__id: int = -1
        self.__assigned_partitions: list[int] = []

        self.__lock: ReadWriteLock = ReadWriteLock()

        self.__register_consumer()

        self._retrieve_queue_partitions_info(5, True)

        t1 = threading.Thread(target=self._retrieve_queue_partitions_info, args=[1, False], daemon=True)
        t1.start()

        t2 = threading.Thread(target=self.__retrieve_assigned_partitions, daemon=True)
        t2.start()

    def __register_consumer(self):
        retries: int = 3

        while retries > 0:
            try:
                res = RegisterConsumerResponse(
                    self._client._create_request(
                        REGISTER_CONSUMER,
                        [
                            (QUEUE_NAME, self._conf.queue),
                            (CONSUMER_GROUP_ID, self._conf.group_id),
                            (CONSUME_FROM, self._conf.consume_from == CONSUME_EARLIEST)
                        ]
                    )
                )

                if not res.success or res.consumer_id <= 0:
                    raise Exception("Could not register consumer")

                self.__lock.acquire_write()

                self.__id = res.consumer_id

                self.__lock.release_write()

                break
            except Exception as e:
                retries -= 1

                if retries <= 0: raise e

    def __retrieve_assigned_partitions(self):
        retries: int = 3

        while not self._stopped:
            try:
                while retries > 0:
                    try:
                        res = GetConsumerAssignedPartitions(
                            GET_CONSUMER_ASSIGNED_PARTITIONS,
                            [
                                (QUEUE_NAME, self._conf.queue),
                                (CONSUMER_GROUP_ID, self._conf.group_id),
                                (CONSUMER_ID, self.__id)
                            ]
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

    def consume(self) -> List[Message] | None:
        return None
        
    async def ack(self, offset: int) -> None:
        pass

    def close(self):
        self.stopped = True

        self.client.close()

        print("Consumer closed")

    def __del__(self):
        self.close()