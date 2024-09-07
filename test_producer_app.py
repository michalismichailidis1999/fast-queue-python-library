from broker_client import BrokerClient, BrokerClientConf

# from producer import Producer, ProducerConf

broker_conf = BrokerClientConf(retries=3, timeoutms=3000)

client = BrokerClient(conf=broker_conf)

client.create_queue("my-fucking-test-queue")

# producer_conf = ProducerConf()

# producer = Producer(client=client, conf=producer_conf)

# producer.produce("Hello world".encode())
