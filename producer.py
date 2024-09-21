from broker_client import *
from constants import *
from uuid import uuid4

class ProducerConf:

    def __init__(self, queue: str) -> None:
        self.queue = queue


class Producer:
    def __init__(self, client: BrokerClient, conf: ProducerConf) -> None:
        self.client: BrokerClient = client
        self.conf: ProducerConf = conf
        self.transactional_id = str(uuid4())
        self.queue = conf.queue

        # if successfull receives back [producer_id]
        res = self.client.send_request(
            PRODUCER_CONNECT.to_bytes(length=4, byteorder=ENDIAS)
            + (len(self.transactional_id)).to_bytes(length=4, byteorder=ENDIAS)
            + (len(self.conf.queue)).to_bytes(length=4, byteorder=ENDIAS)
            + self.transactional_id.encode()
            + self.conf.queue.encode()
        )

        if res[0]:
            raise Exception(res[1])

        self.id: int = int.from_bytes(bytes=res[2][4:8], byteorder=ENDIAS, signed=False)

        print(
            f"Producer with transactional_id {self.transactional_id} initialized for queue {self.conf.queue}"
        )

    def produce(self, message: bytes) -> None:
        partition: int = 1
        total_messages_sent = 1

        res = self.client.send_request(
            PRODUCE.to_bytes(length=4, byteorder=ENDIAS)
            + len(self.queue).to_bytes(length=4, byteorder=ENDIAS)
            + self.conf.queue.encode()
            + self.id.to_bytes(length=4, byteorder=ENDIAS)
            + partition.to_bytes(length=4, byteorder=ENDIAS)
            + total_messages_sent.to_bytes(length=4, byteorder=ENDIAS)
            + self.__get_message_sizes_to_bytes([message])
            + self.__convert_messages_to_bytes([message])
        )

        print(f"Produced message {message.decode()} successfully")

    def __get_message_sizes_to_bytes(self, messages: list[str]) -> bytes:
        if len(messages) == 0:
            raise Exception("Cannot convert empty messages array into bytes")

        _bytes = len(messages[0]).to_bytes(length=4, byteorder=ENDIAS)

        for i in range(1, len(messages)):
            _bytes += len(messages[i]).to_bytes(length=4, byteorder=ENDIAS)

        return _bytes

    def __convert_messages_to_bytes(self, messages: list[str]) -> bytes:
        if len(messages) == 0:
            raise Exception("Cannot convert empty messages array into bytes")

        _bytes = messages[0].encode()

        for i in range(1, len(messages)):
            _bytes += messages[i].encode()

        return _bytes
