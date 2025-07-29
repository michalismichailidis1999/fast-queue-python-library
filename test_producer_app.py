from broker_client import BrokerClient, BrokerClientConf
from producer import Producer, ProducerConf
from constants import *
import time
import asyncio

def on_delivery_callback(message: bytes, key: bytes | None, exception: Exception | None):
    if exception != None:
        print(f"Could not produce message `{(message.decode())}`. Reason: {exception}")
    else:
        print(f"Message `{(message.decode())}` produced successfully")

broker_conf = BrokerClientConf(
    # timeoutms=None,
    # ssl_enable=False,
    # root_cert="C:\\Users\\Windows\\.ssh\\message_broker_certs\\ca.crt",
    # cert="C:\\Users\\Windows\\.ssh\\message_broker_certs\\client.crt",
    # cert_key="C:\\Users\\Windows\\.ssh\\message_broker_certs\\client.key",
    # cert_pass = None,
    # authentication_enabled=True,
    # username="test",
    # password="test",
)

client = BrokerClient(conf=broker_conf, controller_node=["127.0.0.1", 9877])

queue_name = input("Enter the queue you want to produce messages to: ")

client.create_queue(queue=queue_name, partitions=3, replication_factor=1)

producer_conf = ProducerConf(queue=queue_name, wait_ms=3000, max_batch_size=163840)

producer = Producer(client=client, conf=producer_conf)

start = time.time()

while True:
    message = input("Enter a message (type 'exit' to stop): ")

    if not message:
        print("Cannot produce empty message")
        continue

    if message == "exit": break

    key = input("Enter a key for your message (Optional): ")

    for i in range(100000):
        asyncio.run(producer.produce(message=f"{message} {i}", key=f"{key} {i}"))#, on_delivery=on_delivery_callback))
        if i % 10000 == 0 and i > 0:
            end = time.time()
            duration = end - start

            print(f"Iteration: {i}, Duration: {duration:.3f} seconds")
            start = time.time()

    break

producer.close()