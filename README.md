# Arrow-Kafka Integration Example

This repository demonstrates how to use Apache Arrow with Kafka for efficient data streaming and processing. It includes examples of producing and consuming Apache Arrow data with Kafka, along with code to serialize and deserialize Arrow data.
<!--
## Advantages of Using Apache Arrow Data

Integrating Apache Arrow provides several benefits for data processing and streaming applications:

1. Efficient serialization/deserialization: Arrow provides zero-copy reads and efficient memory layout, which can significantly speed up serialization and deserialization processes.

1. Reduced data size: Arrow's columnar format can often lead to smaller data sizes compared to row-based formats, potentially reducing network transfer times and storage requirements.

1. Interoperability: Arrow provides a standard in-memory format that can be easily shared between different systems and languages without data conversion overhead.

1. Performance: Arrow's columnar format allows for vectorized operations, which can significantly speed up data processing tasks.

## Disclaimer

### Benchmarking and Performance

Please note that the advantages described in this repository regarding the use of Apache Arrow with Kafka are based on studies and general observations of Apache Arrow’s capabilities and benefits. **Benchmarking and performance testing specific to this integration have not been conducted.**

The listed advantages are derived from documented features and benefits of Apache Arrow and Kafka, and may vary depending on your specific use case, data size, and environment. For accurate performance assessments, we recommend conducting your own benchmarking tailored to your particular scenario. -->

## Prerequisites

Make sure you have the following installed:

- [Apache Kafka](https://kafka.apache.org/quickstart): Make sure Kafka is running on `localhost:9092` or update the configuration accordingly.
- [Python 3.x](https://www.python.org/downloads/)
- [pip](https://pip.pypa.io/en/stable/): Python package installer

## How It Works

### Producing Arrow Data

1. **Serialization**: The `serialize` function converts an Apache Arrow `RecordBatch` into a byte array using the Arrow IPC (Inter-Process Communication) stream.

   ```python
   # Serialization function for Apache Arrow
   def serialize(batch):
       sink = pa.BufferOutputStream()
       with ipc.new_stream(sink, batch.schema) as writer:
           writer.write_batch(batch)
       return sink.getvalue().to_pybytes()
   ```

2. **Production Script**: The `produce` function creates sample Arrow data, serializes it, and sends it to the Kafka topic.

   ```python
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
   ```

### Consuming Arrow Data

1. **Deserialization**: The `deserialize` function converts a byte array back into a Pandas DataFrame.

   ```python
   # Deserialization function for Apache Arrow
   def deserialize(buffer):
       with ipc.open_stream(buffer) as reader:
           return reader.read_pandas()
   ```

2. **Consumption Script**: The `consume` function listens for messages from the Kafka topic, deserializes them, and logs the resulting Pandas DataFrame.

   ```python
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
   ```

## Installation

1. Clone the repository:

```sh
git clone https://github.com/Joel-hanson/arrow-kafka-demo.git
cd arrow-kafka-demo
```

1. Install the required Python packages:

   ```sh
   pip install -r requirements.txt
   ```

## Usage

### Producing and Consuming Arrow Data

1. **Run `main.py`**:

   This script demonstrates the basic functionality of producing and consuming Apache Arrow data with Kafka.

   ```sh
   python main.py
   ```

2. **Run `production.py`** (optional):

   This script is tailored for production scenarios and may include optimizations or additional configurations.

   ```sh
   python production.py
   ```

3. **Using `tips-with-serializer.py`**:

   This script shows how to use a custom Arrow serializer and deserializer with Kafka.

   ```sh
   python tips-with-serializer.py
   ```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [Apache Arrow](https://arrow.apache.org/)
- [Apache Kafka](https://kafka.apache.org/)
- [Confluent Kafka Python Client](https://docs.confluent.io/platform/current/clients/python.html)
