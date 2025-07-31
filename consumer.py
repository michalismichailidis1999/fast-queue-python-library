import threading
from typing import Callable
from queue_partitions_handler import QueuePartitionsHandler
from broker_client import BrokerClient
from conf import ConsumerConf
import asyncio
from responses import RegisterConsumerResponse
from constants import *

class Consumer(QueuePartitionsHandler):
    def __init__(self, client: BrokerClient, conf: ConsumerConf):
        super().__init__(client=client, consumer_conf=conf)

        self.__id: int = -1

        self.__register_consumer()

        self.retrieve_queue_partitions_info(5, True)

        t1 = threading.Thread(target=self.retrieve_queue_partitions_info, args=[1, False], daemon=True)
        t1.start()

    def __register_consumer(self):
        res = RegisterConsumerResponse(
            self._client._create_request(
                REGISTER_CONSUMER,
                [
                    (QUEUE_NAME, self._conf.queue),
                    (CONSUMER_GROUP_ID, self._conf.group_id)
                ]
            )
        )

        if not res.success:
            raise Exception("Could not register consumer")

        self.__id = res.consumer_id

    async def consume(self, callback: Callable[[bytes, bytes], None]) -> None:
        if callback is None:
            raise ValueError("callback function cannot be null")
        
    async def ack(self, offset: int) -> None:
        pass

    def close(self):
        self.stopped = True

        self.client.close()

        print("Consumer closed")

    def __del__(self):
        self.close()