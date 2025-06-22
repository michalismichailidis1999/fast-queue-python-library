from broker_client import BrokerClient, BrokerClientConf
from producer import Producer, ProducerConf
from constants import *


def on_delivery_callback(exception: Exception, message: bytes, key: bytes, message_bytes: int):
    if exception != None:
        pass
        # print(
        #     f"Could not produce message {message} with size {message_bytes} bytes. Reason: {exception}"
        # )
    else:
        pass
        # print(
        #     f"Message {message} with size {message_bytes} bytes produced successfully"
        # )


broker_conf = BrokerClientConf(
    # timeoutms=None,
    # ssl_enable=False,
    # root_cert="C:\\Users\\Windows\\.ssh\\message_broker_certs\\ca.crt",
    # cert="C:\\Users\\Windows\\.ssh\\message_broker_certs\\client.crt",
    # cert_key="C:\\Users\\Windows\\.ssh\\message_broker_certs\\client.key",
    # sasl_enabled=True,
    # sasl_auth_method=SASL_BASIC_AUTH,
    # sasl_username="test",
    # sasl_password="test",
)

client = BrokerClient(conf=broker_conf, controller_node=["127.0.0.1", 9877])

queue_name = input("Enter the queue you want to produce messages to: ")

client.create_queue(queue=queue_name, partitions=1, replication_factor=1)

# producer_conf = ProducerConf(queue=queue_name, wait_ms=1000, max_batch_size=16384)

# producer = Producer(client=client, conf=producer_conf)

# while True:
#     message = input("Enter a message (type 'exit' to stop): ")

#     if not message:
#         print("Cannot produce empty message")
#         continue

#     if message == "exit":
#         break

#     producer.produce(Message(payload=message), on_delivery=on_delivery_callback)

# producer.close()
