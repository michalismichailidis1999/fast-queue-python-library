from broker_client import BrokerClient, BrokerClientConf
from producer import Producer, ProducerConf
from models import Message

broker_conf = BrokerClientConf(
    retries=3,
    timeoutms=None,
    # use_https=True,
    # root_cert="C:\\Users\\Windows\\.ssh\\message_broker_certs\\ca.crt",
    # cert="C:\\Users\\Windows\\.ssh\\message_broker_certs\\client.crt",
    # cert_key="C:\\Users\\Windows\\.ssh\\message_broker_certs\\client.key",
)

client = BrokerClient(conf=broker_conf)

queue_name = input("Enter the queue you want to produce messages to: ")

client.create_queue(queue=queue_name, partitions=1)

producer_conf = ProducerConf(queue=queue_name, wait_ms=1000, max_batch_size=16384)

producer = Producer(client=client, conf=producer_conf)

while True:
    message = input("Enter a message (type 'exit' to stop): ")

    if not message:
        print("Cannot produce empty message")
        continue

    if message == "exit":
        break

    producer.produce(Message(payload=message))

producer.close()
