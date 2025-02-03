import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import duckdb
import os
import time
from confluent_kafka import Consumer
import json
from datetime import datetime
import matplotlib.pyplot as plt


# Define the DuckDB database file path
DUCKDB_FILE = 'cleaned_data.duckdb'
# Connect to DuckDB
tcon = duckdb.connect(DUCKDB_FILE)

# Define Kafka topic and relevant columns for flattened data
TOPIC_FLAT_TABLES = 'flattened'
FLAT_COLUMNS = ['instance_id','query_id','write_table_ids','read_table_ids','arrival_timestamp','query_type']

# Example instance ID for filtering\instance_id = 85
# Define Kafka broker address
KAFKA_BROKER = 'localhost:9092'

# Connect to DuckDB database
con = duckdb.connect(DUCKDB_FILE)

# Define SQL query to create the flattened table structure
create_flattened_table_ids_table = """
    CREATE OR REPLACE TABLE flattened_table_ids (
        instance_id int32,
        query_id int64,
        write_table_id int64,
        read_table_id int64,
        arrival_timestamp timestamp,
        query_type varchar)
"""

# Define SQL query to create ingestion intervals table
create_ingestion_intervals_per_table = """
    CREATE OR REPLACE TABLE ingestion_intervals_per_table (
        instance_id int32,
        query_id int64,
        write_table_id int64,
        current_timestamp timestamp,
        next_timestamp timestamp)
"""

# Define SQL query to create the output table for analysis
create_output_table = """
    CREATE OR REPLACE TABLE output_table(
        instance_id int32,
        query_id int64,
        query_type varchar,
        write_table_id int64,
        read_table_id int64,
        arrival_timestamp timestamp,
        last_write_table_insert timestamp,
        next_write_table_insert timestamp,
        time_since_last_ingest_ms int64,
        time_to_next_ingest_ms int64
    )
"""

# Define SQL query to create a view counting analytical queries vs. transformation queries per table_id
create_view_tables_workload_count = """
    CREATE OR REPLACE VIEW tables_workload_count AS 
    WITH select_count_table AS (        
        SELECT -- Count select queries based on read_table_id
            instance_id,
            read_table_id AS table_read_by_select,
            COUNT(CASE WHEN query_type = 'select' THEN 1 END) AS select_count
        FROM output_table
        WHERE 1
            AND query_type = 'select'
            --AND instance_id = {instance_id} -- Uncomment for filtering by instance_id
        GROUP BY ALL
    ), transform_count_table AS (
        SELECT -- Count transformation queries based on write_table_id
            instance_id,
            write_table_id AS table_transformed,
            COUNT(CASE WHEN query_type IN ('update', 'delete') THEN 1 END) AS transform_count
        FROM output_table
        WHERE 1
            AND query_type IN ('update', 'delete')
            --AND instance_id = {instance_id} -- Uncomment for filtering by instance_id
        GROUP BY ALL
    )
    SELECT 
        COALESCE(s.instance_id, t.instance_id) AS instance_id,
        COALESCE(t.table_transformed, s.table_read_by_select) AS table_id,
        t.transform_count,
        s.select_count
    FROM select_count_table s
    FULL OUTER JOIN transform_count_table t
    ON t.table_transformed = s.table_read_by_select
    --WHERE instance_id = {instance_id} -- Uncomment for filtering by instance_id
"""


# Function to create tables if they don't exist
def create_tables():
    """
    Creates necessary tables and views in DuckDB if they do not already exist.
    This function executes SQL commands to create the following:
      - flattened_table_ids table
      - ingestion_intervals_per_table table
      - output_table table
      - tables_workload_count view
    """
    con.execute(create_flattened_table_ids_table)
    con.execute(create_ingestion_intervals_per_table)
    con.execute(create_output_table)
    con.execute(create_view_tables_workload_count)

# Call the function to ensure tables are created
create_tables()

# Initialize a Kafka Consumer to consume messages from the specified topic
consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,  # Kafka broker address
        'group.id': 'analytics',  # Consumer group ID
        'auto.offset.reset': 'earliest',  # Start reading from the beginning if no offset exists
        'enable.auto.commit': False,  # Disable automatic offset commit
        'enable.partition.eof': False,  # Avoid EOF issues
    })

# Subscribe the consumer to the Kafka topic
consumer.subscribe([TOPIC_FLAT_TABLES])

def create_consumer(topic, group_id):
    """
    Creates and returns a Kafka consumer for the specified topic and consumer group.

    Parameters:
        topic (str): The Kafka topic to subscribe to.
        group_id (str): The consumer group ID.

    Returns:
        Consumer: A configured Kafka consumer instance.
    """
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,  # Kafka broker address
        'group.id': group_id,  # Consumer group ID
        'auto.offset.reset': 'earliest',  # Read from the beginning if no offset found
        'enable.auto.commit': False,  # Disable automatic offset commit
        'enable.partition.eof': False,  # Avoid EOF issues
    })
    
    # Subscribe to the specified topic
    consumer.subscribe([topic])
    
    return consumer

# Create a consumer for live analytics processing
create_consumer(TOPIC_FLAT_TABLES, 'liveanalytics')


def update_tables_periodically():
    """
    Periodically updates DuckDB tables with new Kafka messages and processes data.

    This function performs the following steps in a loop:
    1. Consumes data from Kafka and inserts it into the `flattened_table_ids` table.
    2. Updates and checks the relevant tables/views.
    3. Extracts the latest timestamp from `flattened_table_ids`.
    4. If data is available, inserts intervals into `ingestion_intervals_per_table`.
    5. Updates the `output_table` based on ingestion and query activity.
    6. Commits the Kafka consumer offsets and waits for the next interval.
    7. Builds historical ingestion statistics and select query histograms.
    8. Increments the processing window.

    The loop stops when the current timestamp exceeds `2024-05-31 00:00:00`.
    """
    start = datetime.strptime('2024-02-29 23:59:00', '%Y-%m-%d %H:%M:%S')
    end = datetime.strptime('2024-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')

    while True:
        # Consume and insert Kafka messages into DuckDB
        parquet_to_table(consumer, 'flattened_table_ids', con, FLAT_COLUMNS, TOPIC_FLAT_TABLES)
        consumer.commit()

        # Check if tables are being updated correctly
        check_duckdb_table('flattened_table_ids', con)
        check_duckdb_table('tables_workload_count', con)
        check_duckdb_table('output_table', con)

        # Get the maximum arrival timestamp from `flattened_table_ids`
        query_current_max_timestamp = """
        SELECT MAX(arrival_timestamp) FROM flattened_table_ids
        """
        result = con.execute(query_current_max_timestamp).fetchone()
        
        # Refresh the workload count view
        con.execute("SELECT * FROM tables_workload_count")  

        current_max_timestamp = result[0]  # Extract the value from the tuple
        print(current_max_timestamp)

        # If no data is available, continue the loop
        if current_max_timestamp is None:
            continue

        # Stop processing if the timestamp exceeds the threshold
        if current_max_timestamp > datetime.strptime('2024-05-31 00:00:00', '%Y-%m-%d %H:%M:%S'):
            break

        print(start, 'then:', end)

        # If new data is beyond the current processing window, process it
        if current_max_timestamp > end:
            print('Processing new data...')

            # Query to insert ingestion intervals
            insert_into_ingestion_intervals_per_table = f"""
                INSERT INTO ingestion_intervals_per_table (
                    instance_id, 
                    query_id, 
                    write_table_id, 
                    current_timestamp, 
                    next_timestamp
                )   
                SELECT DISTINCT
                    t1.instance_id,
                    t1.query_id,
                    t1.write_table_id,
                    t1.arrival_timestamp AS current_timestamp,
                    t2.arrival_timestamp AS next_timestamp
                FROM flattened_table_ids t1
                LEFT JOIN flattened_table_ids t2
                    ON t1.write_table_id = t2.write_table_id
                    AND t1.instance_id = t2.instance_id
                    AND t2.arrival_timestamp > t1.arrival_timestamp
                WHERE 1
                    AND t1.query_type IN ('insert', 'copy')
                    AND t1.arrival_timestamp BETWEEN '{start}' AND '{end}'
            """

            # Query to insert processed data into output_table
            insert_into_output_table = f"""
            INSERT INTO output_table(
                    instance_id,
                    query_id,
                    query_type,
                    write_table_id,
                    read_table_id,
                    arrival_timestamp,
                    last_write_table_insert,
                    next_write_table_insert,
                    time_since_last_ingest_ms,
                    time_to_next_ingest_ms
                    )
            WITH output AS (
            SELECT 
                q.instance_id,
                q.query_id,
                q.query_type,
                q.write_table_id,
                q.read_table_id,
                q.arrival_timestamp,
                i.current_timestamp AS last_write_table_insert,
                i.next_timestamp AS next_write_table_insert
            FROM flattened_table_ids q
            LEFT JOIN ingestion_intervals_per_table i
                ON q.write_table_id = i.write_table_id
                AND q.query_id = i.query_id
                AND q.instance_id = i.instance_id
            WHERE 1
                AND q.arrival_timestamp BETWEEN '{start}' AND '{end}'
            ) 
            SELECT DISTINCT
                o.instance_id,
                o.query_id,
                o.query_type,
                o.write_table_id,
                o.read_table_id,
                o.arrival_timestamp,
                i.last_write_table_insert,
                i.next_write_table_insert,
                EPOCH_MS(o.arrival_timestamp - i.last_write_table_insert) AS time_since_last_ingest_ms,
                EPOCH_MS(i.next_write_table_insert - o.arrival_timestamp) AS time_to_next_ingest_ms   
            FROM output AS o
            JOIN output AS i
                ON i.query_type IN ('insert','copy') -- Match ingestion queries
                AND o.arrival_timestamp BETWEEN 
                    i.last_write_table_insert 
                    AND COALESCE(i.next_write_table_insert, STRPTIME('31.12.2999', '%d.%m.%Y')::TIMESTAMP)
                AND o.instance_id = i.instance_id
                AND (
                    (o.query_type = 'select' AND o.read_table_id = i.write_table_id) 
                    OR (o.query_type != 'select' AND o.write_table_id = i.write_table_id)  
                )
                AND i.arrival_timestamp BETWEEN '{start}' AND '{end}' 
            WHERE 1
                AND o.query_type NOT IN ('insert', 'copy')
            UNION ALL  
            SELECT  
                instance_id,
                query_id,
                query_type,
                write_table_id,
                read_table_id,
                arrival_timestamp,
                last_write_table_insert,
                next_write_table_insert,
                EPOCH_MS(arrival_timestamp - last_write_table_insert) AS time_since_last_ingest_ms,
                EPOCH_MS(next_write_table_insert - arrival_timestamp) AS time_to_next_ingest_ms   
            FROM output
            WHERE 1
                AND query_type IN ('insert', 'copy')
                AND arrival_timestamp BETWEEN '{start}' AND '{end}'   
            """

            # Execute queries
            con.execute(insert_into_ingestion_intervals_per_table)
            print('Checkpoint 1: Ingestion intervals updated')

            con.execute(insert_into_output_table)
            print('Checkpoint 2: Output table updated')

            consumer.commit()

            # Sleep for 5 seconds before the next update cycle
            time.sleep(5)

            # Build historical ingestion table and select query histogram
            build_historical_ingestion_table()
            build_select_queries_histogram()

            # Move processing window forward
            start = end + timedelta(hours=6)
            end = end + timedelta(minutes=10) + timedelta(hours=6)


historical_ingestion_table_placeholder = st.empty()

def build_historical_ingestion_table():
    """
    Fetch and display a table with ingestion analytics for frequently read tables.

    This function retrieves ingestion metrics for tables where more than 80% 
    of the queries are 'select' queries. It calculates:
    - The average time since the last ingestion (in seconds).
    - The average time until the next ingestion (in seconds).

    The results are visualized using a Plotly table.
    """

    uniq_id = str(int(time.time()))  # Generate a unique identifier for Plotly chart rendering

    # SQL query to calculate ingestion analytics
    query = f"""
        WITH analytical_tables AS (
            SELECT instance_id, table_id,
                CAST(select_count / NULLIF(transform_count + select_count, 0) AS DECIMAL(20, 2)) AS percentage_select_queries
            FROM tables_workload_count
        )
        SELECT 
            instance_id, 
            read_table_id,
            CAST(AVG(time_since_last_ingest_ms) / 1000.0 AS DECIMAL(20, 0)) AS average_time_since_last_ingest_s, 
            CAST(AVG(time_to_next_ingest_ms) / 1000.0 AS DECIMAL(20, 0)) AS average_time_to_next_ingest_s
        FROM output_table
        WHERE read_table_id IN (
            SELECT table_id FROM analytical_tables WHERE percentage_select_queries > 0.80
        )
        AND query_type = 'select'
        GROUP BY instance_id, read_table_id
        ORDER BY read_table_id
        LIMIT 12 
    """
    
    print('checkpoint5')  # Debugging checkpoint

    # Execute the query and convert the result into a DataFrame
    df_table = con.execute(query).df()
    df_table.fillna(0, inplace=True)  # Replace NaN values with 0
    df_table['read_table_id'] = df_table['read_table_id'].astype(str)  # Ensure table IDs are strings

    # Create a Plotly Table visualization
    fig = go.Figure(data=[go.Table(
        columnwidth=[5, 10, 10, 10],  # Set column widths
        header=dict(
            values=["Instance ID", "Read Table ID", "Avg Time Since Ingest (s)", "Avg Time to Next Ingest (s)"],
            fill_color="royalblue",
            font=dict(color="white", size=14),
            align="center"
        ),
        cells=dict(
            values=[
                df_table["instance_id"], 
                df_table["read_table_id"], 
                df_table["average_time_since_last_ingest_s"], 
                df_table["average_time_to_next_ingest_s"]
            ],
            fill_color="black",
            font=dict(color="white", size=12),
            align="center"
        )
    )])

    # Configure visualization layout
    fig.update_layout(
        title="Historical Ingestion Metrics", 
        template="plotly_dark", 
        width=750, 
        height=300
    )

    # Display the visualization using Streamlit
    historical_ingestion_table_placeholder.plotly_chart(fig, use_container_width=True, key=uniq_id)


# Kafka consumer configuration for monitoring stress index
consumer_stress = Consumer({
    'bootstrap.servers': KAFKA_BROKER,  # Kafka broker address
    'group.id': 'analytics',  # Consumer group ID for analytics processing
    'auto.offset.reset': 'latest',  # Start reading from the latest message
    'enable.auto.commit': False,  # Disable auto-commit to manually control offset commits
})

# Subscribe to the 'stressindex' topic for monitoring stress-related metrics
consumer_stress.subscribe(['stressindex'])


@st.cache_data(ttl=10)  # Cache with TTL of 10 seconds
def build_select_queries_histogram():
    """
    Builds a histogram for the relative time between ingestion queries (select queries)
    for tables with a high percentage of select queries. The data is grouped into bins
    and visualized with a bar chart for each read_table_id.

    The query logic identifies tables where the percentage of select queries is greater than 80%
    and computes the relative time between ingestion queries. The data is then divided into 10 bins
    based on this relative time.
    """
    
    query = """
    WITH analytical_tables AS (
        SELECT instance_id, table_id,
                CAST(COALESCE(select_count / NULLIF(transform_count + select_count, 0), 0) AS DECIMAL(20, 2)) AS percentage_select_queries          
        FROM tables_workload_count
        WHERE (select_count / NULLIF(transform_count + select_count, 0)) > 0.80
    ), realtive_to_next_times AS (
        SELECT DISTINCT 
                instance_id, 
                query_id, 
                read_table_id,
                EPOCH_MS(arrival_timestamp - last_write_table_insert) / 
                EPOCH_MS(next_write_table_insert - last_write_table_insert) AS relative_to_next
        FROM output_table
        WHERE read_table_id IN (SELECT table_id FROM analytical_tables)
        AND query_type = 'select'
    ), hist_bins AS (
        SELECT instance_id, query_id, read_table_id,
                NTILE(10) OVER (ORDER BY relative_to_next) AS bin
        FROM realtive_to_next_times
    )
    SELECT instance_id, read_table_id, bin, COUNT(*) AS count
    FROM hist_bins
    GROUP BY instance_id, read_table_id, bin
    ORDER BY instance_id, read_table_id, bin;
    """
    
    # Execute the query and convert the result to a DataFrame
    result = con.execute(query).df()

    # If no data is returned, display a message and exit
    if result.empty:
        st.write("No data available.")
        return

    # Get the unique read_table_ids from the result
    read_table_ids = result["read_table_id"].unique()
    
    # If no read_table_ids exist in the data, display a message and exit
    if len(read_table_ids) == 0:
        st.write("No read_table_id in the data.")
        return

    # Create a figure with one subplot per unique read_table_id
    fig, axes = plt.subplots(
        nrows=len(read_table_ids),
        ncols=1,
        figsize=(8, len(read_table_ids) * 4),
        sharex=True
    )

    # If there is only one read_table_id, make axes a list for consistency
    if len(read_table_ids) == 1:
        axes = [axes]

    # Loop over each read_table_id and plot the histogram for it
    for i, read_table_id in enumerate(read_table_ids):
        # Query to fetch the data for the current read_table_id 
        # (you could filter the "result" DataFrame directly here as well)
        sub_query = f"""
        SELECT bin, count 
        FROM result
        WHERE read_table_id = {read_table_id}
        ORDER BY bin
        """
        
        # Execute the subquery and convert the result to a DataFrame
        subset = duckdb.sql(sub_query).df()

        # Plot the histogram for the current read_table_id
        axes[i].bar(
            subset["bin"],
            subset["count"],
            color="skyblue",  # Color of the bars
            edgecolor="black"  # Edge color of the bars
        )
        
        # Set title and labels for the current subplot
        axes[i].set_title(f"Read Table ID: {read_table_id}")
        axes[i].set_ylabel("Count")
        axes[i].grid(axis='y', linestyle='--', alpha=0.7)  # Grid lines for clarity

    # Set x-axis label only on the bottom subplot to avoid overlap
    axes[-1].set_xlabel("Relative time between ingestion queries")

    # Adjust layout for better spacing between subplots
    plt.tight_layout()

    # Display the figure in Streamlit
    st.pyplot(plt.gcf())




def real_time_graph_in_historical_view():
    """
    Fetches real-time data from Kafka, updates the same Plotly figure in a simulation style,
    and continuously updates the displayed graph.
    This function listens for messages from Kafka, processes the data to compute short-term
    and long-term averages, and updates a real-time plot of the stress index, bytes spilled, 
    and averages over time.
    """

    # Initialize lists to hold time series data for plotting
    timestamps = []  # To store the time stamps of each data point
    short_avgs = []  # To store the short-term moving averages
    long_avgs = []  # To store the long-term moving averages
    bytes_spilled_vals = []  # To store the number of bytes spilled at each point in time

    # Initialize the averages for short-term and long-term (start with 0 if no previous values exist)
    long_avg = 0.0
    short_avg = 0.0

    # Create a Streamlit placeholder where the graph will be displayed
    graph_placeholder = st.empty()

    # Create the initial Plotly figure with empty traces
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=timestamps, y=short_avgs,
                             mode='lines',
                             name='Short-term Avg',
                             line=dict(color='blue')))  # Line for short-term average
    fig.add_trace(go.Scatter(x=timestamps, y=long_avgs,
                             mode='lines',
                             name='Long-term Avg',
                             line=dict(color='red')))  # Line for long-term average
    fig.add_trace(go.Scatter(x=timestamps, y=bytes_spilled_vals,
                             mode='lines',
                             name='Bytes Spilled',
                             line=dict(color='green')))  # Line for bytes spilled
    fig.update_layout(title="Real-Time Stress Index",
                      xaxis_title="Time",
                      yaxis_title="Average Value",
                      template="plotly_dark")  # Update graph layout and styling
    
    # Render the initial figure (empty at first)
    graph_placeholder.plotly_chart(fig, use_container_width=True)

    # Begin polling Kafka and updating the figure with new data
    while True:
        msg = consumer_stress.poll(timeout=.01)  # Poll Kafka for a message (with a timeout of 10 ms)
        if msg and msg.value():
            try:
                # Parse the received message (JSON format)
                data = json.loads(msg.value().decode('utf-8'))
                execution_duration = float(data.get("execution_duration_ms", 0))  # Execution duration (in ms)
                bytes_spilled = float(data.get("mbytes_spilled", 0))  # Number of bytes spilled (in MB)

                # Update the moving averages:
                # long_avg uses a smaller weight for recent data, short_avg uses a higher weight
                long_avg = (0.0002 * execution_duration) + (1 - 0.0002) * (long_avgs[-1] if long_avgs else 0)
                short_avg = (0.02 * execution_duration) + (1 - 0.02) * (short_avgs[-1] if short_avgs else 0)

                # Get the current time and append it along with the computed values
                current_time = datetime.now().strftime("%H:%M:%S")
                timestamps.append(current_time)
                short_avgs.append(short_avg)
                long_avgs.append(long_avg)
                bytes_spilled_vals.append(bytes_spilled)

                # Keep only the most recent 50 data points for clarity
                if len(timestamps) > 50:
                    timestamps = timestamps[-50:]
                    short_avgs = short_avgs[-50:]
                    long_avgs = long_avgs[-50:]
                    bytes_spilled_vals = bytes_spilled_vals[-50:]

                # Update the existing figure's data (do not recreate the figure)
                fig.data[0].x = timestamps
                fig.data[0].y = short_avgs
                fig.data[1].x = timestamps
                fig.data[1].y = long_avgs
                fig.data[2].x = timestamps
                fig.data[2].y = bytes_spilled_vals

                # Update the placeholder with the newly updated figure
                graph_placeholder.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                # Handle any errors that occur while processing the Kafka message
                st.error(f"Error processing Kafka message: {e}")

        # Sleep for 2 seconds before polling for new messages from Kafka
        time.sleep(2)


def parquet_to_table(consumer, table, conn, columns, topic):
    """
    Reads messages from a Kafka consumer, extracts data from JSON, writes the data to a Parquet file,
    and then loads it into a DuckDB table.
    
    Args:
        consumer: Kafka consumer instance used to read messages from Kafka.
        table: The name of the DuckDB table where the data will be loaded.
        conn: DuckDB connection object used to interact with the database.
        columns: List of column names to be selected from the incoming Kafka messages.
        topic: The Kafka topic to check for the structure ('flattened' or other types).
    """
    
    data_list = []  # List to hold the data extracted from Kafka messages
    parquet_file = f"kafka_data_{table}.parquet"  # Parquet file name
    
    # Continuously poll Kafka for messages
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll Kafka with a timeout of 1 second
        if msg is None:
            break  # Stop polling when there are no more messages
        
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue  # Skip errors and keep polling

        # Deserialize JSON Kafka message into a string
        message_value = msg.value().decode('utf-8')

        try:
            # Convert JSON string to a dictionary
            records = json.loads(message_value)

            # Ensure records are in list format for consistency
            if isinstance(records, dict):
                records = [records]
            
            # Add the records to the data_list
            data_list.extend(records)
        
        except json.JSONDecodeError as e:
            # Handle any JSON decoding errors
            print(f"Error decoding JSON: {e}")

    # If no data was received from Kafka, print a message and exit
    if not data_list:
        print("No data received from Kafka.")
        return

    # Convert the list of records into a DataFrame and select relevant columns
    df = pd.DataFrame(data_list)
    df = df[columns]  # Select the specific columns to keep

    # Convert 'arrival_timestamp' to datetime format if it exists
    if "arrival_timestamp" in df.columns:
        df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors='coerce')  # Handle errors during conversion

    # Special handling for 'flattened' topic
    if topic == 'flattened':
        # Split 'read_table_ids' by comma and explode into separate rows
        df['read_table_ids'] = df['read_table_ids'].astype(str).str.split(",")
        df = df.explode('read_table_ids', ignore_index=True)

        # Handle any None/NaN values and convert to numeric (NaN becomes NaN)
        df['read_table_ids'] = pd.to_numeric(df['read_table_ids'], errors='coerce')

        # Convert 'read_table_ids' to nullable integer type to allow NaN values
        df['read_table_ids'] = df['read_table_ids'].astype(pd.Int64Dtype())

    # Save the DataFrame as a Parquet file
    df.to_parquet(parquet_file, index=False)

    # Get the absolute path of the Parquet file for DuckDB compatibility
    parquet_path = os.path.abspath(parquet_file)

    # Load the Parquet file into DuckDB
    conn.execute(f"COPY {table} FROM '{parquet_path}' (FORMAT PARQUET)")
    
    # Commit Kafka offset to ensure that only new data is written in the next poll
    consumer.commit()


def check_duckdb_table(table_name, conn):
    """
    Verifies if a table exists and has data in DuckDB.
    
    This function checks the existence of a table in DuckDB and counts the number of rows in it.
    If the table exists, it prints the row count and previews the first few rows. If the table
    is empty, it prints a corresponding message.

    Args:
        table_name (str): Name of the table to check.
        conn (duckdb.DuckDBPyConnection): DuckDB connection object used to query the database.

    Returns:
        Tuple (exists, row_count): 
        - exists (bool): True if the table exists, False otherwise.
        - row_count (int): Number of rows in the table (0 if empty).
    """
    try:
        # Check if the table exists in the DuckDB database by querying the information schema
        table_exists = conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()[0] > 0

        if not table_exists:
            print(f"Table '{table_name}' does NOT exist in DuckDB.")
            return False, 0  # Return False and 0 rows if the table doesn't exist

        # If the table exists, check how many rows it contains
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        if row_count > 0:
            # If the table has data, print the row count and show a preview of the first few rows
            print(f"Table '{table_name}' exists and contains {row_count} rows.")
            
            # Optionally show the first few rows (useful for debugging or inspection)
            df_preview = conn.execute(f"SELECT * FROM {table_name} LIMIT 5").df()
            print("Table Preview:", table_name)
            print(df_preview)
            
        else:
            # If the table exists but has no rows, print a message
            print(f"Table '{table_name}' exists but is EMPTY.")

        return True, row_count  # Return True and the row count

    except Exception as e:
        # If any error occurs (e.g., invalid table name or connection issues), print the error
        print(f"Error checking table '{table_name}': {e}")
        return False, 0  # Return False and 0 rows in case of error



