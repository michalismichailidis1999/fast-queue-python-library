from broker_client import BrokerClient, BrokerClientConf
from producer import Producer
from conf import ProducerConf
from constants import *

def on_delivery_callback(message: bytes, key: bytes | None, exception: Exception | None):
    if exception != None:
        print(f"Could not produce message `{(message.decode())}`. Reason: {exception}")
    else:
        print(f"Message `{(message.decode())}` produced successfully")


broker_conf = BrokerClientConf()

client = BrokerClient(conf=broker_conf, controller_node=["127.0.0.1", 9877])

queue_name = input("Enter the queue you want to produce messages to: ")

# client.create_queue(queue=queue_name, partitions=3, replication_factor=2)

client.create_queue(queue=queue_name, partitions=1, replication_factor=1)

producer_conf = ProducerConf(queue=queue_name)#, wait_ms=15000, max_batch_size=1638400)

producer = Producer(client=client, conf=producer_conf, on_delivery_callback=on_delivery_callback)

while True:
    message = input("Enter a message (type 'exit' to stop): ")

    if not message:
        print("Cannot produce empty message")
        continue

    if message == "exit": break

    key = input("Enter a key for your message (Optional): ")

    producer.produce(message=message, key=key)

producer.close()