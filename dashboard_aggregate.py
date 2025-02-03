import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import os
from confluent_kafka import Consumer
import duckdb
import json
import time
import plotly.graph_objects as go


# File path for DuckDB storage
DUCKDB_FILE = "data.duckdb"  # This is where the DuckDB database file will be stored on disk.
# Kafka broker address
KAFKA_BROKER = 'localhost:9092'  # Kafka broker's address, typically in the form of 'hostname:port'
# Kafka topic names
TOPIC_RAW_DATA = 'parquet_stream'  # Kafka topic that handles the raw data stream, which contains unprocessed data.
TOPIC_CLEAN_DATA = 'clean_data'  # Kafka topic for publishing the cleaned data after processing.
TOPIC_QUERY_METRICS = 'query_metrics'  # Kafka topic used to store query performance metrics (e.g., execution times, resource usage).
TOPIC_COMPILE_METRICS = 'compile_metrics'  # Kafka topic used to store compilation metrics related to query execution.
TOPIC_LEADERBOARD = 'leaderboard'  # Kafka topic for storing data related to the leaderboard (e.g., query ranking).
TOPIC_STRESS_INDEX = 'stressindex'  # Kafka topic used to store stress index data (e.g., server performance or load metrics).
# Column names for different data structures
LEADERBOARD_COLUMNS = ['instance_id', 'query_id', 'user_id', 'arrival_timestamp', 'compile_duration_ms']  # Columns related to leaderboard data.
QUERY_COLUMNS = ['instance_id', 'was_aborted', 'was_cached', 'query_type']  # Columns related to query metadata (e.g., if the query was aborted or cached).
COMPILE_COLUMNS = ['instance_id', 'num_joins', 'num_scans', 'num_aggregations', 'mbytes_scanned', 'mbytes_spilled']  # Columns related to query compilation metrics.
STRESS_COLUMNS = ['instance_id', 'was_aborted', 'arrival_timestamp', 'compile_duration_ms',
                  'execution_duration_ms', 'queue_duration_ms', 'mbytes_scanned', 'mbytes_spilled']  # Columns related to stress testing metrics, including execution and queue durations.


def initialize_duckdb():
    """Create tables in DuckDB if they do not exist"""
    # Establish connection to DuckDB
    con = duckdb.connect(DUCKDB_FILE)

    # Create the LIVE_QUERY_METRICS table if it does not already exist
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS LIVE_QUERY_METRICS (
        instance_id BIGINT,               -- Unique identifier for the instance
        was_aborted BOOLEAN,              -- Flag to indicate if the query was aborted
        was_cached BOOLEAN,               -- Flag to indicate if the query was cached
        query_type VARCHAR                -- Type of the query (e.g., select, update)
    )
    """)

    # Create the LIVE_LEADERBOARD table if it does not already exist
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS LIVE_LEADERBOARD (
        instance_id BIGINT,               -- Instance identifier
        query_id BIGINT,                  -- Query identifier
        user_id BIGINT,                   -- User identifier
        arrival_timestamp TIMESTAMP,      -- Timestamp of when the query arrived
        compile_duration_ms DOUBLE        -- Duration of query compilation in milliseconds
    )
    """)

    # Create the LIVE_COMPILE_METRICS table if it does not already exist
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS LIVE_COMPILE_METRICS (
            instance_id BIGINT,            -- Instance identifier
            num_joins BIGINT,              -- Number of joins in the query
            num_scans BIGINT,              -- Number of scans performed
            num_aggregations BIGINT,       -- Number of aggregations in the query
            mbytes_scanned DOUBLE,         -- Total megabytes scanned during query execution
            mbytes_spilled DOUBLE,         -- Total megabytes spilled to disk
        )
    """)

    # Close the connection to the DuckDB database
    con.close()


def create_consumer(topic, group_id):
    """Create a Confluent Kafka Consumer for a specific topic and group."""
    
    # Initialize Kafka Consumer with required configurations
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,  # Kafka broker address where the consumer connects
        'group.id': group_id,               # Consumer group ID to track offsets within the group
        'auto.offset.reset': 'earliest',     # Start reading from the beginning of the topic if no offsets are found
        'enable.auto.commit': False,         # Disable auto commit of offsets, allowing manual control
        'enable.partition.eof': False,       # Avoid issues with reading empty partitions (end of file)
    })
    
    # Subscribe the consumer to the specified topic
    consumer.subscribe([topic])
    
    # Return the created consumer instance
    return consumer


def Kafka_topic_to_DuckDB():
    """Listen to Kafka topics, extract data, process it, and update DuckDB and dashboard."""
    
    # Initialize Kafka consumers for various topics
    consumer_raw_data = create_consumer(TOPIC_RAW_DATA, 'raw_data')
    consumer_leaderboard = create_consumer(TOPIC_LEADERBOARD, 'live_analytics')
    consumer_query_counter = create_consumer(TOPIC_QUERY_METRICS, 'live_analytics')
    consumer_compile = create_consumer(TOPIC_COMPILE_METRICS, 'live_analytics')
    consumer_stress = create_consumer(TOPIC_STRESS_INDEX, 'live_analytics')
    
    # Initialize DuckDB connection and tables
    initialize_duckdb()
    con = duckdb.connect(DUCKDB_FILE)

    # Prepare the dashboard components (Figures)
    print(f"Listening for messages on topic '{TOPIC_RAW_DATA}'...")
    figure_keys = ["fig1", "fig2", "fig3", "fig4", "fig5", "fig6"]
    for key in figure_keys:
        if key not in st.session_state:
            st.session_state[key] = go.Figure()  # Create placeholder for figures

    metrics_placeholder = st.empty()
    fig1_placeholder = st.empty()
    fig2_placeholder = st.empty()
    fig4_placeholder = st.empty()
    fig5_placeholder = st.empty()
    
    # Initialize variables for real-time tracking of metrics
    history = pd.DataFrame(columns=["timestamp", "short_avg", "long_avg", "bytes_spilled"])
    long_avg, short_avg, mb_spilled = 30, 30, 0  # Initialize avg variables
    start_time = time.time()  # Track processing time

    # List of DuckDB tables to reset every 60 seconds
    tables_to_initialize = ['LIVE_QUERY_METRICS', 'LIVE_COMPILE_METRICS']

    try:
        while True:
            # Every 60 seconds, reset the tables in DuckDB
            if time.time() - start_time > 60:
                for table in tables_to_initialize:
                    con.execute(f"TRUNCATE TABLE {table}")  # Truncate each table to reset its data
                print("All tables initialized!")
                start_time = time.time()  # Reset the timer

            # Load new data from Kafka topics into DuckDB
            parquet_to_table(consumer_query_counter, 'LIVE_QUERY_METRICS', con, QUERY_COLUMNS, TOPIC_QUERY_METRICS)
            parquet_to_table(consumer_leaderboard, 'LIVE_LEADERBOARD', con, LEADERBOARD_COLUMNS, TOPIC_LEADERBOARD)
            parquet_to_table(consumer_compile, 'LIVE_COMPILE_METRICS', con, COMPILE_COLUMNS, TOPIC_COMPILE_METRICS)

            # Use session state to store and display dynamic figures
            st.session_state.fig1 = build_leaderboard_compiletime(con)
            st.session_state.fig2 = build_leaderboard_user_queries(con)
            st.session_state.fig4 = build_live_query_distribution(con)
            st.session_state.fig5 = build_live_compile_metrics(con)

            # Display metrics dynamically in the app
            display_metrics(con, metrics_placeholder)

            # Render the dashboard figures in the UI
            st.session_state.fig1.update_layout(width=400)
            st.session_state.fig2.update_layout(width=400)
            st.session_state.fig4.update_layout(width=400)
            st.session_state.fig5.update_layout(width=400)
            uniq_id = str(int(time.time()))

            # Render figures in two rows with interactive placeholders
            with st.container():
                col1, col2 = st.columns(2)  # Two columns for first row
                with col1:  # Left Column for Compile Time Leaderboard
                    fig1_placeholder.plotly_chart(st.session_state.fig1, config={"responsive": True}, use_container_width=True, key=f"fig1_chart_{uniq_id}")
                with col2:  # Right Column for Query Distribution
                    fig4_placeholder.plotly_chart(st.session_state.fig4, config={"responsive": True}, use_container_width=True, key=f"fig4_chart_{uniq_id}")

            with st.container():
                col3, col4 = st.columns(2)  # Second row with two columns
                with col3:  # Left Column for User Queries
                    fig2_placeholder.plotly_chart(st.session_state.fig2, config={"responsive": True}, use_container_width=True, key=f"fig2_chart_{uniq_id}")
                with col4:  # Right Column for Compile Metrics
                    fig5_placeholder.plotly_chart(st.session_state.fig5, config={"responsive": True}, use_container_width=True, key=f"fig5_chart_{uniq_id}")

            # Real-time Stress Data Update
            time_index = len(history)  # Use index as time tracker
            short_avg, long_avg, mb_spilled = calculate_stress(consumer_stress, long_avg, short_avg, time_index)

            # Append new stress data to history (keep last 100 points)
            new_row = pd.DataFrame([{
                "timestamp": pd.Timestamp.now(),
                "short_avg": short_avg,
                "long_avg": long_avg,
                "bytes_spilled": mb_spilled
            }])
            history = pd.concat([history, new_row]).tail(100)

            # Visualize stress data in real-time
            fig_stress = go.Figure()
            fig_stress.add_trace(go.Scatter(x=history["timestamp"], y=history["short_avg"], mode='lines', name='Short-term Avg', line=dict(color='blue')))
            fig_stress.add_trace(go.Scatter(x=history["timestamp"], y=history["long_avg"], mode='lines', name='Long-term Avg', line=dict(color='red')))
            fig_stress.add_trace(go.Bar(x=history["timestamp"], y=history["bytes_spilled"], name="Bytes Spilled", marker=dict(color="green", opacity=0.6)))

            fig_stress.update_layout(title="Real-Time Query Performance",
                                    xaxis_title="Time",
                                    yaxis_title="Performance Metrics",
                                    template="plotly_dark")

            # Display real-time performance chart
            graph_placeholder.plotly_chart(fig_stress, use_container_width=True)

            time.sleep(2)  # Wait before next iteration

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        # Close Kafka consumers when the process is stopped
        consumer_raw_data.close()
        consumer_leaderboard.close()
        consumer_query_counter.close()
        consumer_compile.close()
        consumer_stress.close()



def display_metrics(con, metrics_placeholder):
    """
    Fetches and updates the query and compile metrics from DuckDB and displays them in a fixed UI position in the Streamlit app.
    
    This function performs the following tasks:
    - Queries DuckDB for various performance metrics like total queries, successful queries, aborted queries, cached queries,
      total bytes scanned, bytes spilled, total joins, and total aggregations.
    - Updates the metrics display in the Streamlit app within a container, using different columns for layout.
    - The metrics are displayed with a stylized UI (background color, borders, etc.) to make them visually appealing.
    
    Parameters:
    con (duckdb.DuckDBPyConnection): The DuckDB connection object used to query the database.
    metrics_placeholder (streamlit.delta_generator.DeltaGenerator): The placeholder in Streamlit UI where the metrics are rendered.
    """
    
    # Query DuckDB tables to fetch the latest metrics data
    total_query = con.execute("SELECT COUNT(*) FROM LIVE_QUERY_METRICS").fetchone()[0]
    successful_query = con.execute("SELECT COUNT(*) FROM LIVE_QUERY_METRICS WHERE was_aborted = FALSE").fetchone()[0]
    aborted_query = con.execute("SELECT COUNT(*) FROM LIVE_QUERY_METRICS WHERE was_aborted = TRUE").fetchone()[0]
    cached_query = con.execute("SELECT COUNT(*) FROM LIVE_QUERY_METRICS WHERE was_cached = TRUE").fetchone()[0]
    total_scan_mbytes = con.execute("SELECT SUM(mbytes_scanned) FROM LIVE_COMPILE_METRICS").fetchone()[0] or 0  # Handle NULL values
    total_spilled_mbytes = con.execute("SELECT SUM(mbytes_spilled) FROM LIVE_COMPILE_METRICS").fetchone()[0] or 0
    total_joins = con.execute("SELECT SUM(num_joins) FROM LIVE_COMPILE_METRICS").fetchone()[0] or 0
    total_aggregations = con.execute("SELECT SUM(num_aggregations) FROM LIVE_COMPILE_METRICS").fetchone()[0] or 0

    # Start updating the metrics display in the UI container
    with metrics_placeholder.container():
        # Title for the query metrics section
        st.markdown("### Query Metrics")

        # Define layout for displaying metrics in 4 columns
        col1, col2, col3, col4 = st.columns(4)
        light_red = "#FFEBEE"  # Background color for the metrics
        dark_red = "#D32F2F"   # Border color for the metrics

        # Display the total number of queries in the first column
        with col1:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>📊 Total Queries</h5>
                    <h3>{total_query}</h3>
                </div>
            """, unsafe_allow_html=True)

        # Display the number of successful queries in the second column
        with col2:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>✅ Successful Queries</h5>
                    <h3>{successful_query}</h3>
                </div>
            """, unsafe_allow_html=True)

        # Display the number of aborted queries in the third column
        with col3:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>❌ Aborted Queries</h5>
                    <h3>{aborted_query}</h3>
                </div>
            """, unsafe_allow_html=True)

        # Display the number of cached queries in the fourth column
        with col4:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>💾 Cached Queries</h5>
                    <h3>{cached_query}</h3>
                </div>
            """, unsafe_allow_html=True)

        # Additional metrics section
        st.markdown("### Additional Metrics")

        # Use two columns for additional metrics (MBs Scanned and MBs Spilled)
        col5, col6 = st.columns(2)
        with col5:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>📊 MBs Scanned</h5>
                    <h3>{total_scan_mbytes} MB</h3>
                </div>
            """, unsafe_allow_html=True)

        with col6:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>💡 MBs Spilled</h5>
                    <h3>{total_spilled_mbytes} MB</h3>
                </div>
            """, unsafe_allow_html=True)

        # Display total joins and total aggregations in two more columns
        col7, col8 = st.columns(2)
        with col7:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>🔗 Total Joins</h5>
                    <h3>{total_joins}</h3>
                </div>
            """, unsafe_allow_html=True)

        with col8:
            st.markdown(f"""
                <div style="padding: 20px; background-color: {light_red}; border-left: 10px solid {dark_red}; border-radius: 10px; text-align: center; margin-bottom: 20px;">
                    <h5>🔢 Total Aggregations</h5>
                    <h3>{total_aggregations}</h3>
                </div>
            """, unsafe_allow_html=True)



def build_leaderboard_compiletime(con):
    '''
    PREREQUISITES: parquet_to_table(consumer, 'LIVE_LEADERBOARD', con, LEADERBOARD_COLUMNS, TOPIC_LEADERBOARD) 
    has already been called to load the data into the LIVE_LEADERBOARD table.
    
    This function queries the LIVE_LEADERBOARD table to retrieve the top 10 instances with the longest compile times.
    It returns a Plotly table visualization showing the rank, instance ID, and formatted compile time (in mm:ss format).
    
    Returns:
    - fig: A Plotly figure containing a table visualization of the top 10 longest compile times.
    '''
    
    # Query the LIVE_LEADERBOARD table to fetch top 10 longest compile durations
    df1 = con.execute(f"""
    SELECT DISTINCT
    query_id, 
    compile_duration_ms as compile_duration
    FROM LIVE_LEADERBOARD
    ORDER BY compile_duration_ms DESC
    LIMIT 10;
    """).df()

    # Handle missing (NaN) compile times by filling them with 0 and ensuring integer format
    df1["compile_duration"] = df1["compile_duration"].fillna(0).astype(int)

    # Format the compile times in mm:ss format (e.g., 02:34 for 154000 ms)
    df1['formatted_compile_time'] = df1['compile_duration'].apply(
        lambda x: f"{x // 60000}:{(x % 60000) // 1000:02d}"
    )

    # Add a rank column to display the position in the leaderboard
    df1.insert(0, "Rank", range(1, len(df1) + 1))

    # Create a Plotly table visualization for the leaderboard
    fig = go.Figure(data=[go.Table(
        columnwidth=[2, 5, 5],  # Adjust column widths
        header=dict(values=["Rank", "Instance ID", "Compile Duration (mm:ss)"],
                    fill_color="royalblue",  # Header background color
                    font=dict(color="white", size=14),  # Header font styling
                    align="center"),  # Header alignment
        cells=dict(values=[
            df1["Rank"].tolist(),  # List of rank values
            df1["query_id"].tolist(),  # List of instance IDs
            df1["formatted_compile_time"].tolist()  # List of formatted compile times
        ],
                   fill_color="black",  # Cells background color
                   font=dict(color="white", size=12),  # Cells font styling
                   align="center")  # Cells alignment
    )])

    # Update the layout and appearance of the table
    fig.update_layout(
        title="Leaderboard: Top 10 Longest Compile Times",  # Set the title
        template="plotly_dark",  # Use a dark theme for the table
        autosize=False,  # Disable auto-sizing for custom width and height
        width=750,  # Set the width of the table
        height=450  # Set the height of the table
    )

    return fig


def build_leaderboard_user_queries(con):
    '''
    PREREQUISITES: parquet_to_table(consumer, 'LIVE_LEADERBOARD', 
    con, LEADERBOARD_COLUMNS, TOPIC_LEADERBOARD) has already been called 
    to ensure the data is loaded into the LIVE_LEADERBOARD table.
    
    This function queries the LIVE_LEADERBOARD table to identify the top 5 users who 
    issued the most queries. It returns a Plotly bar chart displaying these top users 
    and their respective query counts.
    
    Returns:
    - fig: A Plotly figure containing a vertical bar chart visualizing the top 5 users 
      and the number of queries they issued.
    '''
    
    # Query the LIVE_LEADERBOARD table to get the top 5 users who issued the most queries
    df = con.execute(f"""
                       SELECT user_id, COUNT(*) as most_queries
                       FROM LIVE_LEADERBOARD
                       GROUP BY user_id
                       ORDER BY most_queries DESC
                       LIMIT 5;
                       """).df()

    # Visualization: Vertical Bar Chart using Plotly
    fig = go.Figure()

    # Add a bar trace for the query count per user
    fig.add_trace(go.Bar(
        x=df['user_id'],  # User IDs on the x-axis
        y=df['most_queries'],  # Query counts on the y-axis
        marker=dict(color='green'),  # Color of the bars
        text=df['most_queries'],  # Display query counts on top of bars
        textposition='auto'  # Position text labels on the bars
    ))

    # Customize the layout of the chart
    fig.update_layout(
        title='Top 5 Users by Query Count',  # Title of the chart
        xaxis_title='User ID',  # Label for the x-axis
        yaxis_title='Query Count',  # Label for the y-axis
        template='plotly_dark',  # Use a dark theme for the chart
        width=750,  # Set the width of the chart
        height=300  # Set the height of the chart
    )

    # Return the figure object for further use (such as rendering in Streamlit)
    return fig


def build_live_query_distribution(con):
    '''
    PREREQUISITES: parquet_to_table(consumer, 'LIVE_QUERY_METRICS', 
    con, QUERY_COLUMNS, TOPIC_QUERY_METRICS) has already been called 
    to ensure the data is loaded into the LIVE_QUERY_METRICS table.
    
    This function queries the LIVE_QUERY_METRICS table to determine the distribution 
    of query types and returns a Plotly pie chart visualizing the proportions of each query type.
    
    Returns:
    - fig: A Plotly figure containing a pie chart visualizing the distribution of query types.
    '''
    
    # Query the LIVE_QUERY_METRICS table to count the occurrence of each query type
    df = con.execute(f"""
        SELECT 
            query_type, 
            COUNT(*) AS occurrence_count
        FROM LIVE_QUERY_METRICS
        GROUP BY query_type
        ORDER BY occurrence_count DESC;
    """).df()

    # Visualization: Pie Chart using Plotly
    fig = go.Figure(data=[go.Pie(
        labels=df['query_type'],  # Labels are the query types
        values=df['occurrence_count'],  # Values represent the occurrence count of each query type
        hole=0.3,  # Create a donut chart effect
        marker=dict(colors=px.colors.qualitative.Plotly),  # Use dynamic colors from Plotly color palette
        textinfo='label+percent'  # Display both the label (query type) and the percentage
    )])

    # Customize layout for the chart
    fig.update_layout(
        title='Query Type Distribution',  # Set the chart title
        template='plotly_dark',  # Use dark theme for the chart
        width=750,  # Set the width of the chart
        height=500  # Set the height of the chart
    )

    # Return the figure object for use in further visualization (e.g., in Streamlit or a web app)
    return fig


def build_live_query_counts(con):
    '''
    PREREQUISITES: parquet_to_table(consumer, 'LIVE_QUERY_METRICS', 
    con, QUERY_COLUMNS, TOPIC_QUERY_METRICS) has already been called 
    to ensure that the LIVE_QUERY_METRICS table is populated with the necessary data.
    
    This function queries the LIVE_QUERY_METRICS table to retrieve the distribution 
    of query types and generates a pie chart to visualize the proportions of each query type.
    
    Returns:
    - fig: A Plotly figure containing a pie chart that represents the distribution of different query types.
    '''
    
    # Query the LIVE_QUERY_METRICS table to count occurrences of each query type
    df = con.execute(f"""
        SELECT 
            query_type, 
            COUNT(*) AS occurrence_count
        FROM LIVE_QUERY_METRICS
        GROUP BY query_type
        ORDER BY occurrence_count DESC;
    """).df()

    # Visualization: Pie Chart using Plotly
    fig = go.Figure()

    # Add Pie Chart trace to the figure
    fig.add_trace(go.Pie(
        labels=df['query_type'],  # Use the query type as labels
        values=df['occurrence_count'],  # Use the count of occurrences as values
        hoverinfo='label+percent',  # Display both label and percentage when hovering over a slice
        textinfo='label+percent',  # Display both label and percentage on the chart itself
        marker=dict(colors=px.colors.qualitative.Plotly),  # Dynamic color palette from Plotly
    ))

    # Customize layout
    fig.update_layout(
        title='Query Type Distribution',  # Set the title of the chart
        template='plotly_dark'  # Use the dark template for a better visual appearance
    )

    # Return the figure object for use in further visualization (e.g., in Streamlit or a web app)
    return fig



def build_live_compile_metrics(con):
    '''
    PREREQUISITES: parquet_to_table(consumer, 'LIVE_QUERY_METRICS', 
    con, QUERY_COLUMNS, TOPIC_QUERY_METRICS) has already been called.
    
    This function retrieves the sum of scans, aggregations, and joins from the LIVE_COMPILE_METRICS 
    table and generates a stacked bar chart to visualize the compiled metrics.
    
    Returns:
    - fig: A Plotly figure containing a stacked bar chart representing the total scans, 
      aggregations, and joins.
    '''
    
    # Query the LIVE_COMPILE_METRICS table to get the sum of scans, aggregations, and joins
    df = con.execute(f"""
    SELECT
        SUM(num_scans) AS total_scans,
        SUM(num_aggregations) AS total_aggregations,
        SUM(num_joins) AS total_joins
    FROM LIVE_COMPILE_METRICS;
    """).df()

    # Visualization: Stacked Bar Chart using Plotly
    fig = go.Figure(data=[
        # Bar for Scans
        go.Bar(name='Scans', x=['Metrics'], y=[df['total_scans'][0]], marker=dict(color='blue')),
        # Bar for Aggregates
        go.Bar(name='Aggregates', x=['Metrics'], y=[df['total_aggregations'][0]], marker=dict(color='orange')),
        # Bar for Joins
        go.Bar(name='Joins', x=['Metrics'], y=[df['total_joins'][0]], marker=dict(color='green'))
    ])

    # Update layout settings
    fig.update_layout(
        title='Compile Metrics (Scans, Aggregates, Joins)',  # Chart title
        xaxis_title='Metric Type',  # X-axis label
        yaxis_title='Count',  # Y-axis label
        template='plotly_dark',  # Dark theme for the chart
        width=750,  # Set a smaller width for the chart
        barmode='stack'  # Stack the bars on top of each other
    )

    # Return the figure object for further use in a web app or dashboard
    return fig



graph_placeholder = st.empty()

def calculate_stress(consumer, long_avg, short_avg, time_index=0):
    """
    Reads the latest message from Kafka, updates the running averages of execution duration,
    and calculates the stress metric based on the amount of memory spilled. This data can
    be visualized in a dashboard for real-time monitoring.

    Parameters:
    - consumer: Kafka consumer to fetch the latest message.
    - long_avg: The current long-term running average of execution duration.
    - short_avg: The current short-term running average of execution duration.
    - time_index: An optional parameter for tracking time (default is 0).

    Returns:
    - short_avg: Updated short-term running average of execution duration.
    - long_avg: Updated long-term running average of execution duration.
    - mb_spilled: The amount of memory spilled (in MB) from the latest message.
    """

    # Long-term and short-term averaging factors
    long_alpha = 0.005  # Weight for long-term average (smooths over longer periods)
    short_alpha = 0.02   # Weight for short-term average (reacts quicker to recent changes)

    # Poll Kafka for the latest message
    msg = consumer.poll(timeout=.5)  # Timeout after 1 second if no new messages
    if msg is None or msg.value() is None:
        return short_avg, long_avg, 0  # No update if no new message

    if msg.error():
        return short_avg, long_avg, 0  # Ignore messages with errors

    try:
        # Decode and parse the message from Kafka (assuming JSON format)
        message_value = msg.value().decode('utf-8')  # Decode byte string to UTF-8
        message_dict = json.loads(message_value)  # Parse JSON string into a dictionary

        # Extract values for execution duration and memory spilled from the message
        execution_duration = float(message_dict.get("execution_duration_ms", 0))  # Execution time in ms
        mb_spilled = float(message_dict.get("mbytes_spilled", 0))  # Memory spilled in MB

        # Update the running averages for execution duration
        long_avg = (long_alpha * execution_duration) + (1 - long_alpha) * long_avg  # Update long-term average
        short_avg = (short_alpha * execution_duration) + (1 - short_alpha) * short_avg  # Update short-term average

        # Return the updated averages and the amount of memory spilled
        return short_avg, long_avg, mb_spilled

    except json.JSONDecodeError:
        return short_avg, long_avg, 0  # Return no update if the message is not valid JSON


def parquet_to_table(consumer, table, conn, columns, topic):
    """
    Reads messages from a Kafka consumer, extracts data from JSON, writes to Parquet,
    and loads into a DuckDB table.
    
    Args:
        consumer: Kafka consumer instance for fetching messages.
        table: DuckDB table name where data will be loaded.
        conn: DuckDB connection to interact with the database.
        columns: List of columns to select from the Kafka message.
        topic: The topic name to identify the format or processing type (e.g., 'flattened').
    """
    data_list = []  # List to store processed data
    parquet_file = f"kafka_data_{table}.parquet"  # Define the Parquet file name

    while True:
        # Poll for a message from Kafka with a 1-second timeout
        msg = consumer.poll(timeout=.5)
        if msg is None:
            break  # Exit if no more messages are available
        
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue  # Skip errors and keep polling

        # Deserialize the message content
        message_value = msg.value().decode('utf-8')

        try:
            # Convert JSON string to Python dictionary
            records = json.loads(message_value)

            if isinstance(records, dict):
                records = [records]  # Ensure the data is in list format (for consistency)

            data_list.extend(records)  # Add the records to the list

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")  # Handle any JSON decoding errors

    if not data_list:  # Exit if no valid data was received
        print("No data received from Kafka.")
        return

    # Convert the data into a DataFrame
    df = pd.DataFrame(data_list)
    df = df[columns]  # Select only the required columns

    # Handle the 'arrival_timestamp' column if it exists
    if "arrival_timestamp" in df.columns:
        df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors='coerce')  # Convert to datetime

    # If topic is 'flattened', handle specific processing for this topic
    if topic == 'flattened':
        # Split and explode 'read_table_ids' if present
        df['read_table_ids'] = df['read_table_ids'].astype(str).str.split(",")
        df = df.explode('read_table_ids', ignore_index=True)

        # Handle conversion of 'read_table_ids' to numeric, allowing errors to be coerced
        df['read_table_ids'] = pd.to_numeric(df['read_table_ids'], errors='coerce')

        # Convert to nullable integer type (NaN values allowed)
        df['read_table_ids'] = df['read_table_ids'].astype(pd.Int64Dtype())

    print(df)  # Print the DataFrame for debugging

    # Save the DataFrame to a Parquet file
    df.to_parquet(parquet_file, index=False)

    # Add a short delay before continuing
    time.sleep(4)

    # Get the absolute path for DuckDB compatibility
    parquet_path = os.path.abspath(parquet_file)

    # Load the Parquet file into the DuckDB table
    conn.execute(f"COPY {table} FROM '{parquet_path}' (FORMAT PARQUET)")

    # Commit the Kafka consumer offset to ensure we only process new data in the future
    consumer.commit()
