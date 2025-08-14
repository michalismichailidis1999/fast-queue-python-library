import time
from broker_client import BrokerClient, BrokerClientConf
from consumer import Consumer
from conf import ConsumerConf
from constants import *
from responses import Message

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

queue_name = input("Enter the queue you want to consume messages from: ")

client.create_queue(queue=queue_name, partitions=3, replication_factor=1)

consumer_conf = ConsumerConf(queue=queue_name, group_id="test", consume_from=CONSUME_EARLIEST, auto_commit=False)

consumer = Consumer(client=client, conf=consumer_conf)

def handle_message(message: Message) -> None:
    try:
        print(f"Offset: {message.offset}, Timestamp: {message.timestamp}")
        consumer.ack(offset=message.offset, partition=message.partition)
    except Exception as e:
        print(f"Something went wrong while processing message with offset {message.offset}. Reason: {e}")

while True:
    try:
        messages = consumer.poll_messages()

        if messages is None:
            time.sleep(2)

        for message in messages: handle_message(message)

    except Exception as e:
        print(f"{e}")