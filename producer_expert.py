import pandas as pd
from confluent_kafka import Producer
import json
import time


# Database and File Configuration
DUCKDB_FILE = "cleaned_data.duckdb"  # Path to DuckDB file where cleaned data will be stored

# Kafka & KSQL Setup
# Start Zookeeper server: bin/zookeeper-server-start etc/kafka/zookeeper.properties
# Start Kafka server: bin/kafka-server-start etc/kafka/server.properties
# Start KSQL DB server: bin/ksql-server-start etc/ksqldb/ksql-server.properties

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Address of the Kafka broker to connect to
TOPIC_RAW_DATA = 'parquet_stream'  # Kafka topic for streaming raw data

# Expert Analytics Section for Flattened Data
TOPIC_FLAT_TABLES = 'flattened'  # Kafka topic for flattened data tables
FLAT_COLUMNS = ['instance_id', 'query_id', 'write_table_ids', 'read_table_ids', 'arrival_timestamp', 'query_type']  # Columns for flattened data



def send_to_kafka(producer, topic, chunk):
    """
    Send data from a DataFrame chunk to a Kafka topic.

    This function converts each record in the provided DataFrame chunk into a dictionary,
    serializes it into JSON format, and sends it to the specified Kafka topic using the provided 
    Kafka producer. Timestamps are converted to ISO 8601 format before sending.

    Args:
        producer (KafkaProducer): The Kafka producer instance used to send data.
        topic (str): The name of the Kafka topic to send the data to.
        chunk (pandas.DataFrame): A DataFrame containing the data to send. Each row in the DataFrame
                                  represents a record to be sent to Kafka.

    Returns:
        None: This function does not return any value, it sends data to Kafka and flushes the producer.
    """
    # Iterate over each record (row) in the DataFrame chunk
    for record in chunk.to_dict(orient='records'):
        # Convert any Timestamp objects in the record to ISO 8601 string format
        record = {k: v.isoformat() if isinstance(v, pd.Timestamp) else v for k, v in record.items()}

        # Send the record to the specified Kafka topic in JSON format
        producer.produce(topic, key=None, value=json.dumps(record))

    # Ensure all records are sent before the function ends
    producer.flush()


def stream_parquet_to_kafka(parquet_file, batch_size):
    """
    Stream the specified Parquet file to Kafka in batches.

    This function reads a Parquet file, processes the data in batches, and streams it 
    to a Kafka topic. Each record is sent in the specified batch size. It also writes 
    different types of data (like leaderboard, query metrics, etc.) to their respective 
    Kafka topics.

    Args:
        parquet_file (str): Path to the Parquet file to be streamed.
        batch_size (int): The batch size to use for sending data to Kafka.
    """
    # Configure Kafka producer with necessary settings
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,  # Kafka broker to connect to
        'linger.ms': 10,  # Delay in milliseconds before sending data (helps to group small batches)
    }
    producer = Producer(producer_config)  # Kafka producer instance

    print(f"Streaming Parquet file '{parquet_file}' to Kafka topic '{TOPIC_RAW_DATA}' with batch size {batch_size}...")
    
    # Read the Parquet file into a pandas DataFrame
    df = pd.read_parquet(parquet_file)
    
    # Sort the DataFrame by 'arrival_timestamp' to process in chronological order
    df = df.sort_values(by='arrival_timestamp').reset_index(drop=True)
    
    # Convert 'arrival_timestamp' column to datetime format
    df['arrival_timestamp'] = pd.to_datetime(df['arrival_timestamp'])
    
    # Assign a 'batch_id' to each record based on the batch size
    df['batch_id'] = (df.index // batch_size)  # Group data into batches
    
    # Type cast columns if necessary (you can define this function based on the need)
    type_cast_batch(df)
    
    # Iterate over the DataFrame grouped by 'batch_id'
    for batch_id, batch in df.groupby('batch_id'):
        try:
            # Send the batch to Kafka topic 'TOPIC_RAW_DATA'
            send_to_kafka(producer, TOPIC_RAW_DATA, batch)
            print(f"Batch {batch_id} sent to Kafka successfully.")
            
            # TESTING: Send flattened tables to the respective Kafka topic
            write_to_topic(batch, TOPIC_FLAT_TABLES, producer, FLAT_COLUMNS)
            
            # Ensure the producer sends the data to Kafka before proceeding
            producer.flush()
            
            # Optional delay between batches for simulating real-time streaming
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")
        
        print("Finished streaming data to Kafka.")
        
        # Add delay based on time differences between batches for real-time simulation
        # if batch_id < df['batch_id'].max():
            # Uncomment below to use delay based on the time difference between batches
            #delay_stream(curr_batch_end, next_batch_start)

        # Ensure that all data has been sent before moving to the next batch
        producer.flush()

    print("Batch over")  # Indicate that the batch processing is done


def delay_stream(batch_start, next_batch_start):
    """
    Simulate real-time streaming by introducing a delay based on the time difference 
    between the start of the current and next batch.

    The delay is scaled using a factor to compress the time between batches, simulating 
    a real-time stream for testing or processing purposes.

    Args:
        batch_start (pd.Timestamp): The timestamp indicating the start of the current batch.
        next_batch_start (pd.Timestamp): The timestamp indicating the start of the next batch.

    Returns:
        None: The function introduces a delay but does not return any value.
    """
    # Scaling factor to compress 3 months of data into 20 minutes of real-time simulation
    scaling_factor = 6480  # Example scaling factor for time compression (can be adjusted as needed)
    
    # Calculate the time difference between the current batch and the next batch in seconds
    time_diff = (next_batch_start - batch_start).total_seconds()
    
    # Calculate the delay by dividing the time difference by the scaling factor
    delay = time_diff / scaling_factor
    
    # Set a minimum delay of 1 second to avoid excessive delays for small time differences
    min_delay = 1
    
    # Sleep for the calculated delay, ensuring that it is at least the minimum delay
    time.sleep(max(delay, min_delay))


def type_cast_batch(batch):
    """
    Type casts the columns of a Pandas DataFrame according to a predefined schema.

    This function ensures that each column in the DataFrame is converted to the correct data type
    as specified in the dtype_mapping. If any invalid timestamps are encountered during conversion,
    they will be coerced to `NaT` (Not a Time).

    Args:
        batch (pandas.DataFrame): The input DataFrame containing columns that need to be type-cast.

    Returns:
        pandas.DataFrame: The type-cast DataFrame with the columns converted to the specified data types.
    """
    # Define a dictionary that maps column names to their expected data types
    dtype_mapping = {
        "instance_id": "Int64",  # Integer type for instance_id
        "cluster_size": "float64",  # Float type for cluster_size
        "user_id": "Int64",  # Integer type for user_id
        "database_id": "Int64",  # Integer type for database_id
        "query_id": "Int64",  # Integer type for query_id
        "arrival_timestamp": "datetime64[ns]",  # DateTime type for arrival_timestamp
        "compile_duration_ms": "float64",  # Float type for compile_duration_ms
        "queue_duration_ms": "Int64",  # Integer type for queue_duration_ms
        "execution_duration_ms": "Int64",  # Integer type for execution_duration_ms
        "feature_fingerprint": "string",  # String type for feature_fingerprint
        "was_aborted": "boolean",  # Boolean type for was_aborted
        "was_cached": "boolean",  # Boolean type for was_cached
        "cache_source_query_id": "float64",  # Float type for cache_source_query_id
        "query_type": "string",  # String type for query_type
        "num_permanent_tables_accessed": "float64",  # Float type for num_permanent_tables_accessed
        "num_external_tables_accessed": "float64",  # Float type for num_external_tables_accessed
        "num_system_tables_accessed": "float64",  # Float type for num_system_tables_accessed
        "read_table_ids": "string",  # String type for read_table_ids
        "write_table_ids": "string",  # String type for write_table_ids
        "mbytes_scanned": "float64",  # Float type for mbytes_scanned
        "mbytes_spilled": "float64",  # Float type for mbytes_spilled
        "num_joins": "Int64",  # Integer type for num_joins
        "num_scans": "Int64",  # Integer type for num_scans
        "num_aggregations": "Int64",  # Integer type for num_aggregations
        "batch_id": "Int64",  # Integer type for batch_id
    }

    # Loop through each column and cast it to the appropriate type
    for column, dtype in dtype_mapping.items():
        if dtype == "datetime64[ns]":
            # If the dtype is datetime, convert the column to datetime type, handle errors by coercing to NaT
            batch[column] = pd.to_datetime(batch[column], errors='coerce')
        else:
            # Cast the column to the specified data type
            batch[column] = batch[column].astype(dtype)



def write_to_topic(batch, topic, producer, list_columns):
    """
    Write a batch of data to a specified Kafka topic.

    This function selects relevant columns from the provided DataFrame, converts any datetime columns
    to ISO 8601 format, serializes the data to JSON, and sends each record to the specified Kafka topic.

    Args:
        batch (pandas.DataFrame): The input DataFrame containing data to be written to Kafka.
        topic (str): The Kafka topic to which the data should be written.
        producer (KafkaProducer): The Kafka producer instance used to send data.
        list_columns (list): A list of column names that should be included in the Kafka messages.

    Returns:
        None: This function does not return any value; it sends data to Kafka.
    """
    try:
        # Ensure the input batch is a pandas DataFrame
        if not isinstance(batch, pd.DataFrame):
            raise ValueError("Expected 'batch' to be a pandas DataFrame.")

        # Select only the relevant columns based on the provided list
        selected_columns = batch[list_columns].copy()  # Copy to avoid modifying the original DataFrame

        # If no relevant columns are found, print a warning and exit
        if selected_columns.empty:
            print(f"Warning: No relevant columns found for topic '{topic}'.")
            return
        
        # Convert datetime columns to string format (ISO 8601)
        for col in selected_columns.select_dtypes(include=['datetime64[ns]']).columns:
            selected_columns[col] = selected_columns[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Convert the DataFrame to a list of dictionaries (each row becomes a JSON object)
        json_payloads = selected_columns.to_dict(orient='records')

        # Send each record as a separate message to the Kafka topic
        for record in json_payloads:
            # Print the record for debugging (optional)
            print(record)
            # Send the record to the Kafka topic
            producer.produce(topic, value=json.dumps(record))
            producer.flush()

    except Exception as e:
        # Handle any exceptions and print an error message
        print(f"Error writing to topic '{topic}': {e}")

    finally:
        # Ensure all messages are flushed before ending
        producer.flush()

def main():
    parquet_file = 'sample_0.001.parquet'  # Parquet file name
    batch_size = 1000  # Batch size

    stream_parquet_to_kafka(parquet_file, batch_size)

if __name__ == '__main__':
    main()
