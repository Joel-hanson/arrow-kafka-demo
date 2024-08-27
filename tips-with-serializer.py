import logging
import time
import pyarrow as pa
import pandas as pd
import pyarrow.ipc as ipc
from pyarrow import csv
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.serialization import Serializer, Deserializer

# Set up logging for better visibility of the process
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Kafka configuration
broker = "localhost:9092"
topic_name = "tips"
received_dataframes = []

# Custom Serializer for Apache Arrow
class ArrowSerializer(Serializer):

    def __call__(self, record_batch, ctx):
        if record_batch is None:
            return None

        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, record_batch.schema) as writer:
            writer.write_batch(record_batch)
        return sink.getvalue().to_pybytes()

# Custom Deserializer for Apache Arrow
class ArrowDeserializer(Deserializer):
    def __init__(self, schema=None):
        self.schema = schema

    def __call__(self, value, ctx):
        if value is None:
            return None

        reader = ipc.open_stream(value)
        return reader.read_all()

# Function to produce Apache Arrow serialized data to Kafka
def produce_data():
    table = csv.read_csv("tips.csv")  # Read the CSV into an Arrow Table
    batches = table.to_batches(max_chunksize=5)

    producer = Producer({'bootstrap.servers': broker})
    serializer = ArrowSerializer()

    for batch in batches:
        serialized_value = serializer(batch, None)
        producer.produce(topic=topic_name, value=serialized_value)
        logger.info("Produced a batch to Kafka")
        producer.flush()  # Ensure the message is sent

# Function to consume Apache Arrow serialized data from Kafka and convert to Pandas
def consume_data(timeout=5.0):
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'arrow-group',
        'auto.offset.reset': 'earliest'
    })

    deserializer = ArrowDeserializer()
    consumer.subscribe([topic_name])

    start_time = time.time()
    while time.time() - start_time < timeout:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Kafka error: {msg.error()}")
                break

        # Deserialize the message to an Arrow RecordBatch
        record_batch = deserializer(msg.value(), None)
        if record_batch:
            df = record_batch.to_pandas()
            logger.info(f"Consumed and converted to Pandas DataFrame:\n{df}")
            received_dataframes.append(df)

    consumer.close()

if __name__ == "__main__":
    try:
        logger.info("Starting production and consumption...")
        produce_data()
        consume_data()

        if received_dataframes:
            combined_df = pd.concat(received_dataframes, ignore_index=True)
            logger.info(f"Combined DataFrame for analysis:\n{combined_df}")
        else:
            logger.info("No data was received for analysis.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info("Process completed")
