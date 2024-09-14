from broker_client import BrokerClient, BrokerClientConf

# from producer import Producer, ProducerConf

broker_conf = BrokerClientConf(retries=3, timeoutms=None)

client = BrokerClient(conf=broker_conf)

queue_name = input("Enter a queue name: ")

client.create_queue(queue_name, 3)
client.create_queue(queue_name + "2", 3)
client.create_queue(queue_name + "3", 3)
client.create_queue(queue_name + "4", 3)

client.list_queues()

client.delete_queue(queue_name)

# producer_conf = ProducerConf()

# producer = Producer(client=client, conf=producer_conf)

# producer.produce("Hello world".encode())
