from typing import Dict
from broker_client import *
from constants import *
from uuid import uuid4
import threading
import time

class ProducerConf:

    """
    :param str queue: Name of the queue in which the producer will produce messages to.
    :param int wait_ms: Milliseconds to wait before sending the messages batch.
    :param int max_batch_size: Maximum batch size in bytes producer can hold locally before sending it to broker.
    :raise BadValueError: If invalid argument is passed or incorrect value type is passed.
    """

    def __init__(self, queue: str, **kwargs) -> None:
        self.queue: str = queue
        self.wait_ms: int = (
            0  # send immediatelly to broker, do not wait for batch to be accumulated
        )
        self.max_batch_size: int = 16384  # 16KB

        for key, value in kwargs.items():
            if key == "wait_ms":
                if not isinstance(value, int):
                    raise ValueError(f"{key} must be an integer")

                self.wait_ms: int = value
            elif key == "max_batch_size":
                if not isinstance(value, int):
                    raise ValueError(f"{key} must be an integer")

                self.max_batch_size: int = value
            else:
                raise ValueError(f"Invalid argument {key}")


class Producer:

    def __init__(self, client: BrokerClient, conf: ProducerConf) -> None:
        self.client: BrokerClient = client
        self.conf: ProducerConf = conf
        self.transactional_id = str(uuid4())

        self.partitions: Dict[int, list[str]] = {}
        self.total_bytes_cached = 0

        self.open_transactions: Dict[int, int] = {}

        self.__connect()

        self.messages_mut = threading.Lock()
        self.transactions_mut = threading.Lock()

        if self.conf.wait_ms > 0:
            t = threading.Thread(target=self.__flush_messages_batch)
            t.start()

        print(
            f"Producer with transactional_id {self.transactional_id} initialized for queue {self.conf.queue}"
        )

    def produce(self, message: str) -> None:
        if message == None or message == "":
            raise ValueError("Message was empty")

        self.produce_many([message])

        print(f"Produced message {message.decode()}")

    def produce_many(self, messages: list[str]):
        ex: Exception = None
        total_bytes_cached: int = 0

        self.messages_mut.acquire()

        try:
            for message in messages:
                partition: int = self.__get_message_partition(message)

                if partition not in self.partitions:
                    self.partitions[partition] = []

                self.partitions[partition].append(message)
                self.total_bytes_cached += len(message)

            messages.clear()

            total_bytes_cached = self.total_bytes_cached
        except Exception as e:
            ex = e
        finally:
            self.messages_mut.release()

        if ex != None:
            raise ex

        if self.conf.max_batch_size <= total_bytes_cached:
            self.flush()

    def flush(self):
        print("Flushing messages...")

        ex: Exception = None

        self.transactions_mut.acquire()
        self.messages_mut.acquire()

        try:
            thread_id = threading.get_ident()

            for partition in self.partitions.keys():
                self.client.send_request(
                    self.client.create_request(
                        PRODUCE,
                        [
                            (QUEUE_NAME, self.conf.queue),
                            (TRANSACTIONAL_ID, self.transactional_id),
                            (
                                TRANSACTION_ID,
                                (
                                    self.open_transactions[thread_id]
                                    if thread_id in self.open_transactions
                                    else None
                                ),
                            ),
                            (PRODUCER_ID, self.id),
                            (PRODUCER_EPOCH, self.epoch),
                            (PARTITION, partition),
                        ]
                        + [
                            (MESSAGE, message) for message in self.partitions[partition]
                        ],
                    )
                )

                flushed_bytes = sum(
                    [len(message) for message in self.partitions[partition]]
                )

                print(f"Flushed {flushed_bytes} bytes from partition {partition}")

                self.total_bytes_cached -= flushed_bytes
                del self.partitions[partition]
        except Exception as e:
            ex = e
        finally:
            self.transactions_mut.release()
            self.messages_mut.release()

        print("Messages flushed")

    def begin_transaction(self):
        ex: Exception = None

        self.transactions_mut.acquire()

        try:
            res = self.client.send_request(
                self.client.create_request(
                    BEGIN_TRANSACTION, [(TRANSACTIONAL_ID, self.transactional_id)]
                )
            )

            thread_id = threading.get_ident()

            self.open_transactions[thread_id] = int.from_bytes(
                bytes=res[4:8], byteorder=ENDIAS, signed=False
            )
        except Exception as e:
            ex = e
        finally:
            self.transactions_mut.release()

        if ex != None:
            raise ex

    def commit_transaction(self, transaction_id: int):
        ex: Exception = None

        self.transactions_mut.acquire()

        try:
            thread_id = threading.get_ident()

            if thread_id not in self.open_transactions:
                raise ValueError("Transaction cannot be None")

            transaction_id: int = self.open_transactions[thread_id]

            self.client.send_request(
                self.client.create_request(
                    COMMIT_TRANSACTION,
                    [
                        (TRANSACTIONAL_ID, self.transactional_id),
                        (TRANSACTION_ID, transaction_id),
                    ],
                )
            )
        except Exception as e:
            ex = e
        finally:
            self.transactions_mut.release()

        if ex != None:
            raise ex

    def close(self):
        try:
            if self.total_bytes_cached > 0:
                print("Trying to flush remaining messages before shutdown..")
                self.flush()
        except Exception as e:
            print(f"Could not flush remaining messages. Reason: {e}")

        self.client.close()

        print("Producer closed")

    def __flush_messages_batch(self):
        while True:
            self.flush()
            time.sleep(self.conf.wait_ms / 1000)

    def __get_message_partition(message: str) -> int:
        return 0

    def __connect(self):
        # if successfull receives back [producer_id, epoch]
        res = self.client.send_request(
            self.client.create_request(
                PRODUCER_CONNECT,
                [
                    (TRANSACTIONAL_ID, self.transactional_id),
                    (QUEUE_NAME, self.conf.queue),
                ],
            )
        )

        self.id: int = int.from_bytes(bytes=res[4:8], byteorder=ENDIAS, signed=False)
        self.epoch: int = int.from_bytes(
            bytes=res[8:12], byteorder=ENDIAS, signed=False
        )
