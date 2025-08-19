import asyncio
import time
from broker_client import BrokerClient, BrokerClientConf
from consumer import Consumer
from conf import ConsumerConf
from constants import *
from responses import Message

broker_conf = BrokerClientConf()

client = BrokerClient(conf=broker_conf, controller_node=["127.0.0.1", 9877])

queue_name = input("Enter the queue you want to consume messages from: ")

client.create_queue(queue=queue_name, partitions=3, replication_factor=1)

group_id = input("Enter a group id you want to use for your consumer: ")

consumer_conf = ConsumerConf(queue=queue_name, group_id=group_id, consume_from=CONSUME_EARLIEST, auto_commit=True, auto_commit_interval_ms=5000)

consumer = Consumer(client=client, conf=consumer_conf)

async def handle_message(message: Message) -> None:
    try:
        print(f"Offset: {message.offset}, Timestamp: {message.timestamp}")
        await consumer.ack(offset=message.offset, partition=message.partition)
    except Exception as e:
        print(f"Something went wrong while processing message with offset {message.offset}. Reason: {e}")

async def consume_messages():
    while True:
        try:
            await messages = consumer.poll_messages()

            if messages is None:
                time.sleep(2)
                continue

            for message in messages: handle_message(message)

        except Exception as e:
            print(f"{e}")

asyncio.run(consume_messages)