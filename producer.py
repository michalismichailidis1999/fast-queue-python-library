from broker_client import *

class ProducerConf:
    def __init__(self) -> None:
        pass

class Producer():
    def __init__(self, client:BrokerClient, conf:ProducerConf) -> None:
        self.client = client
        self.conf = conf

    def produce(message:bytes) -> None:
        pass