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
            f"Producer with transactional_id {self.transactional_id} and id {self.id} initialized for queue {self.conf.queue}"
        )

    def produce(self, message: bytes) -> None:
        print(f"Produced message {message.decode()} successfully")
