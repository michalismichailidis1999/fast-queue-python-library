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

client.create_queue(queue=queue_1, partitions=1, replication_factor=1)
client.create_queue(queue=queue_2, partitions=1, replication_factor=1)

transactional_producer = TransactionalProducer(
    client=client,
    producers_info=[
        (ProducerConf(queue=queue_1, wait_ms=5000, max_batch_size=(16384 * 4)), on_delivery_callback),
        (ProducerConf(queue=queue_2, wait_ms=5000, max_batch_size=(16384 * 4)), on_delivery_callback)
    ]
)

tx_id: int = 0

while True:
    try:
        do_you_want_to_exit = input("Do you want to exit ? (y/n): ")

        if do_you_want_to_exit == "y": break

        message = input("Enter a message: ")

        while not message or message == "":
            print("Cannot produce empty message")
            message = input("Enter a message: ")

        key = input("Enter a key for your message (Optional): ")

        tx_id = transactional_producer.begin_transaction()

        transactional_producer.produce(queue_name=queue_1, message=f"| {queue_1} | {message} |", key=key, transaction_id=tx_id)
        transactional_producer.produce(queue_name=queue_2, message=f"| {queue_2} | {message} |", key=key, transaction_id=tx_id)

        transactional_producer.commit_transaction(tx_id)

        tx_id = 0
    except Exception as e:
        print(f"Could not produce message '{message}'. Reason: {e}")

        if tx_id > 0:
            transactional_producer.abort_transaction(tx_id)

transactional_producer.close()