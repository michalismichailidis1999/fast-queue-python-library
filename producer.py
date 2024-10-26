from typing import Callable, Dict
from broker_client import *
from constants import *
from uuid import uuid4
import threading
import time
from models import Message
import mmh3
import random

class ProducerConf:

    """
    :param str queue: Name of the queue in which the producer will produce messages to.
    :param int wait_ms: Milliseconds to wait before sending the messages batch.
    :param int max_batch_size: Maximum batch size in bytes producer can hold locally before sending it to broker (if wait_ms > 0).
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

        self.partitions: Dict[
            int, list[(Message, Callable[[Exception, Message, int], None])]
        ] = {}
        self.total_bytes_cached = 0

        self.open_transactions: Dict[int, int] = {}

        self.__connect()

        self.messages_mut: threading.Lock = threading.Lock()
        self.transactions_mut: threading.Lock = threading.Lock()
        self.partitions_mut: threading.Lock = threading.Lock()

        self.__send_messages_in_batches: bool = self.conf.wait_ms > 0
        self.__prev_partition_sent: int = -1
        self.__seed = random.randint(100, 1000)

        if self.conf.wait_ms > 0:
            t = threading.Thread(target=self.__flush_messages_batch, daemon=True)
            t.start()

        print(
            f"Producer with transactional_id {self.transactional_id} initialized for queue {self.conf.queue}"
        )

    def produce(
        self,
        message: Message,
        on_delivery: Callable[[Exception, Message, int], None] = None,
    ) -> None:
        if message.payload == None or message.payload == "":
            raise ValueError("Message was empty")

        ex: Exception = None

        self.messages_mut.acquire()

        try:
            partition: int = self.__get_message_partition(message)

            if partition not in self.partitions:
                self.partitions[partition] = []

            self.partitions[partition].append((message, on_delivery))
            self.total_bytes_cached += message.get_total_bytes()
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

        self.transactions_mut.acquire()
        self.messages_mut.acquire()

        try:
            if len(self.partitions.keys()) > 0:
                print("Flushing messages...")

                thread_id = threading.get_ident()

                for partition in self.partitions.keys():
                    if len(self.partitions[partition]) == 0:
                        continue

                    partition_ex: Exception = None
                    flushed_bytes: int = 0

                    try:
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
                                    (MESSAGE, str(message))
                                    for message, _ in self.partitions[partition]
                                ],
                            )
                        )
                    except Exception as e:
                        partition_ex = e
                    finally:
                        for message, cb in self.partitions[partition]:
                            try:
                                message_bytes: int = message.get_total_bytes()
                                flushed_bytes += message_bytes
                                if cb != None:
                                    cb(partition_ex, message, message_bytes)
                            except Exception as e:
                                print(f"Error occured flushing messages. {e}")

                    print(f"Flushed {flushed_bytes} bytes from partition {partition}")

                    self.total_bytes_cached -= flushed_bytes
                    del self.partitions[partition]

                    print("Messages flushed")
        except Exception as e:
            ex = e
        finally:
            self.transactions_mut.release()
            self.messages_mut.release()

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
            time.sleep(self.conf.wait_ms / 1000)
            self.flush()

    def __get_message_partition(self, message: Message, key: Any = None) -> int:
        if key == None:
            ex: Exception = None

            self.partitions_mut.acquire()

            partition: int = self.__prev_partition_sent

            try:
                if partition == -1:
                    partition = 0
                else:
                    partition = (partition + 1) % self.__partitions
            except Exception as e:
                ex = e
            finally:
                self.partitions_mut.release()

            if ex != None:
                raise ex

            return partition
        else:
            return mmh3.hash(key=str(key), seed=self.__seed) % self.partitions

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

        self.__partitions: int = int.from_bytes(
            bytes=res[12:16], byteorder=ENDIAS, signed=False
        )
