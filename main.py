import logging
import random
import string
import time
import pyarrow as pa
from confluent_kafka import Producer, Consumer, KafkaError
import pyarrow.ipc as ipc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Kafka configuration
broker = "localhost:9092"
topic_name = "arrow-topic"

# Serialization function for Apache Arrow
def serialize(batch):
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()

# Deserialization function for Apache Arrow
def deserialize(buffer):
    with ipc.open_stream(buffer) as reader:
        return reader.read_pandas()

def get_random_string(length):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))

# Function to produce messages to Kafka
def produce():
    logger.info("Producing....")
    data = [
        pa.array([1, 2, 3, 4]),
        pa.array(["foo", "bar", "baz", None]),
        pa.array([True, None, False, True]),
    ]
    batch = pa.record_batch(data, names=["f0", "f1", "f2"])

    producer = Producer({'bootstrap.servers': broker})
    serialized_value = serialize(batch)
    producer.produce(topic=topic_name, value=serialized_value)
    logger.info("Produced a batch to Kafka")
    producer.flush()  # Ensure the message is sent

# Function to consume messages from Kafka
def consume():
    logger.info("Consuming....")
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': f"arrow-group-{get_random_string(5)}",
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic_name])

    start_time = time.time()
    while time.time() - start_time < 5.0:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Kafka error: {msg.error()}")
                break

        buffer = pa.py_buffer(msg.value())
        dataframe = deserialize(buffer)
        logger.info(f"Consumed and converted to Pandas DataFrame:\n{dataframe}")

    consumer.close()

if __name__ == "__main__":
    logger.info("Starting production and consumption...")
    produce()
    consume()
