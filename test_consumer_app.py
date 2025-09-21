import time
from broker_client import BrokerClient, BrokerClientConf
from consumer import Consumer
from conf import ConsumerConf
from constants import *
from responses import Message

broker_conf = BrokerClientConf()

client = BrokerClient(conf=broker_conf, controller_node=["127.0.0.1", 9877])

queue_name = "test"

client.create_queue(queue=queue_name, partitions=5, replication_factor=1)

consumer_conf = ConsumerConf(queue=queue_name, group_id="test", consume_from=CONSUME_EARLIEST, auto_commit=True, auto_commit_interval_ms=5000)

consumer = Consumer(client=client, conf=consumer_conf)

def handle_message(message: Message) -> None:
    try:
        print(message)
    except Exception as e:
        print(f"Something went wrong while processing message with offset {message.offset}. Reason: {e}")

while True:
    try:
        messages = consumer.poll_messages()

        if messages is None:
            time.sleep(2)
            continue

        for message in messages:
            handle_message(message)

    except Exception as e:
        print(f"{e}")