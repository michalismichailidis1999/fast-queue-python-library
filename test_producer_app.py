from broker_client import BrokerClient, BrokerClientConf
from producer import Producer, ProducerConf

broker_conf = BrokerClientConf(retries=3, timeoutms=None)

client = BrokerClient(conf=broker_conf)

queue_name = input("Enter a queue name: ")

client.create_queue(queue_name, 3)

client.list_queues()

producer_conf = ProducerConf(queue=queue_name)

producer = Producer(client=client, conf=producer_conf)

message = input("Enter a message to send to broker: ")

producer.produce(message.encode())
