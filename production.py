import logging
import time
import pyarrow as pa
import pandas as pd
import pyarrow.ipc as ipc
from pyarrow import csv
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.serialization import Serializer, Deserializer
from concurrent.futures import ThreadPoolExecutor
import socket
import random
import string

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# Kafka configuration
BROKER = "localhost:9092"
TOPIC_NAME = "tips"
ARROW_BATCH_SIZE = 10000 # The batch size of the record_batch that is produced to kafka
CONSUMER_TIMEOUT = 60.0 # The wait time for the consumer to close
RECEIVED_DATAFRAMES = []


class ArrowSerializer(Serializer):
    def __call__(self, record_batch, ctx):
        if record_batch is None:
            return None
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, record_batch.schema) as writer:
            writer.write_batch(record_batch)
        return sink.getvalue().to_pybytes()


class ArrowDeserializer(Deserializer):
    def __call__(self, value, ctx):
        if value is None:
            return None
        reader = ipc.open_stream(value)
        return reader.read_all()


def produce_data(producer, serializer, table):
    batches = table.to_batches(max_chunksize=ARROW_BATCH_SIZE)  # Increased batch size
    for batch in batches:
        serialized_value = serializer(batch, None)
        producer.produce(topic=TOPIC_NAME, value=serialized_value)
        logger.info(f"Produced a batch of {batch.num_rows} rows to Kafka")
    producer.flush()


def consume_data(consumer, deserializer, timeout=CONSUMER_TIMEOUT):
    start_time = time.time()
    while time.time() - start_time < timeout:
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Kafka error: {msg.error()}")
                break

        record_batch = deserializer(msg.value(), None)
        if record_batch:
            df = record_batch.to_pandas()
            logger.info(f"Consumed batch of {len(df)} rows")
            RECEIVED_DATAFRAMES.append(df)

    consumer.close()


def get_random_string(length):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


def main():
    try:
        # Kafka configuration with better defaults
        producer_conf = {
            "bootstrap.servers": BROKER,
            "client.id": socket.gethostname(),
            "acks": "all",
            "retries": 5,
            "linger.ms": 5,
            "compression.type": "lz4",
            "batch.size": 65536,
            "max.in.flight.requests.per.connection": 5,
        }

        consumer_conf = {
            "bootstrap.servers": BROKER,
            "group.id": f"arrow-group-{get_random_string(5)}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
        }

        serializer = ArrowSerializer()
        deserializer = ArrowDeserializer()

        # Read data
        table = csv.read_csv("tips.csv")

        # Produce data
        produce_data(Producer(producer_conf), serializer, table)

        # Consume data
        with ThreadPoolExecutor(max_workers=3) as executor:
            consumers = [Consumer(consumer_conf) for _ in range(3)]
            for consumer in consumers:
                consumer.subscribe([TOPIC_NAME])
            futures = [
                executor.submit(consume_data, consumer, deserializer)
                for consumer in consumers
            ]

            for future in futures:
                future.result()

        if RECEIVED_DATAFRAMES:
            combined_df = pd.concat(RECEIVED_DATAFRAMES, ignore_index=True)
            logger.info(f"Combined DataFrame: {len(combined_df)} rows")
        else:
            logger.info("No data was received for analysis.")

    except Exception as e:
        logger.exception(f"An error occurred: {e}")
    finally:
        logger.info("Process completed")


if __name__ == "__main__":
    main()
