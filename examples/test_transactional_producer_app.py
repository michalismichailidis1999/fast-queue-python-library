from fast_queue import BrokerClient, BrokerClientConf, TransactionalProducer, ProducerConf

def on_delivery_callback(message: bytes, key: bytes | None, exception: Exception | None):
    if exception != None:
        print(f"Could not produce message `{(message.decode())}`. Reason: {exception}")
    else:
        print(f"Message `{(message.decode())}` produced successfully")

broker_conf = BrokerClientConf()

client = BrokerClient(conf=broker_conf, controller_node=["127.0.0.1", 9877])

queue_1 = "test-1"
queue_2 = "test-2"
queue_3 = "test-3"

client.create_queue(queue=queue_1, partitions=3, replication_factor=1)
client.create_queue(queue=queue_2, partitions=3, replication_factor=1)
client.create_queue(queue=queue_3, partitions=3, replication_factor=1)

transactional_producer = TransactionalProducer(
    client=client,
    producers_info=[
        (ProducerConf(queue=queue_1, wait_ms=5000, max_batch_size=(16384 * 4)), on_delivery_callback),
        (ProducerConf(queue=queue_2, wait_ms=5000, max_batch_size=(16384 * 4)), on_delivery_callback),
        (ProducerConf(queue=queue_3, wait_ms=5000, max_batch_size=(16384 * 4)), on_delivery_callback)
    ]
)

while True:
    try:
        message = input("Enter a message (type 'exit' to stop): ")

        if not message:
            print("Cannot produce empty message")
            continue

        if message == "exit": break

        key = input("Enter a key for your message (Optional): ")

        transactional_producer.produce(message=message, key=key)
    except Exception as e:
        print(f"Could not produce message '{message}'. Reason: {e}")

transactional_producer.close()