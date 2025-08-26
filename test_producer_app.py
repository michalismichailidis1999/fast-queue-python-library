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

client.create_queue(queue=queue_name, partitions=2, replication_factor=2)

producer_conf = ProducerConf(queue=queue_name)

producer = Producer(client=client, conf=producer_conf, on_delivery_callback=on_delivery_callback)

while True:
    try:
        message = input("Enter a message (type 'exit' to stop): ")

        if not message:
            print("Cannot produce empty message")
            continue

        if message == "exit": break

        key = input("Enter a key for your message (Optional): ")

        producer.produce(message=message, key=key)
    except Exception as e:
        print(f"Could not produce message '{message}'. Reason: {e}")

producer.close()