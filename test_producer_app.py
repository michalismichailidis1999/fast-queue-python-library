from broker_client import BrokerClient, BrokerClientConf
from producer import Producer, ProducerConf
from constants import *

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

client.create_queue(queue=queue_name, partitions=5, replication_factor=1)

producer_conf = ProducerConf(queue=queue_name, wait_ms=1000, max_batch_size=16384)

producer = Producer(client=client, conf=producer_conf)

while True:
    message = input("Enter a message (type 'exit' to stop): ")

    if not message:
        print("Cannot produce empty message")
        continue

    if message == "exit": break

    producer.produce(message, on_delivery=on_delivery_callback)

producer.close()
