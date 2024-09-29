from broker_client import BrokerClient, BrokerClientConf
from producer import Producer, ProducerConf

broker_conf = BrokerClientConf(retries=3, timeoutms=None)

client = BrokerClient(conf=broker_conf)

queue_name = input("Enter a queue name: ")

client.create_queue(queue=queue_name, partitions=1)

producer_conf = ProducerConf(queue=queue_name, wait_ms=100, max_batch_size=16384)

producer = Producer(client=client, conf=producer_conf)

message = input("Enter a message to send to broker 1.000.000 times: ")

producer.produce(message)
