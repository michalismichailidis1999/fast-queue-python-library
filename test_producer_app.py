from broker_client import BrokerClient, BrokerClientConf
from producer import Producer, ProducerConf
from models import Message

broker_conf = BrokerClientConf(retries=3, timeoutms=None)

client = BrokerClient(conf=broker_conf)

queue_name = input("Enter the queue you want to produce messages to: ")

client.create_queue(queue=queue_name, partitions=1)

producer_conf = ProducerConf(queue=queue_name, wait_ms=1000, max_batch_size=16384)

producer = Producer(client=client, conf=producer_conf)

while True:
    message = input("Enter a message: ")
    producer.produce(Message(payload=message))

producer.close()
