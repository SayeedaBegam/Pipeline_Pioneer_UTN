# README

## Team Information

**Team Name**: Pipeline Pioneers  
**Domain**: Data Engineering  
**Institution/Organization**: University of Technology, Nuremberg  

### Team Members:
- **Orsu Vinay** – Data Processing Pioneer
- **Carl-David Reese** – Analytics Pioneer
- **Noas Shaalan** – Debug & Pipeline Pioneer
- **Kengo Kaneda** – Integration & Pipeline Pioneer
- **Sayeeda Begam Mohamed Ikbal** – Integration & Dashboard Pioneer

## Overview

This project provides a data pipeline and visualization dashboard using two different views:

1. **Expert View** - Provides in-depth insights and detailed data analysis.
2. **Aggregate View** - Offers summarized and high-level insights.

Both views are integrated into a common framework for streamlined operations.

## Project Structure

├── requirements.txt
├── producer_expert.py 
├── producer_aggregate.py
├── dashboard_aggregate.py 
├── dashboard_expert.py 
├── common.py 
└── README.md


## System Architecture

The architecture of the system is as follows:

1. **Data Input**: Batch inputs of raw data are ingested into the system.
2. **Producer 1**: Processes the data and divides it into two views:
   - **Live View**: For real-time processing and visualization.
   - **Expert View**: For in-depth data insights and analysis.
3. **Data Processing**: The pipeline cleans and processes the data through multiple stages:
   - `RAW_DATA`: Stores the raw input data.
   - `CLEAN_DATA`: Processes and cleans the raw data.
   - `QUERY_METRICS`: Extracts query-related metrics.
   - `COMPILE_METRICS`: Compiles detailed metrics for advanced analysis.
   - `LEADERBOARD`: Aggregates and displays top-level metrics.
   - `FLATTENED`: Provides a flattened, summarized dataset.
4. **Visualization**: Dashboards display the processed data:
   - The **Expert Dashboard** offers detailed insights for advanced users.
   - The **Aggregate Dashboard** provides a high-level overview for streamlined consumption.

## Key Features

- Real-time processing with live views.
- Detailed **Expert View** for advanced data analysis.
- Simplified **Aggregate View** for quick decision-making.
- Modular Architecture for easy scalability and maintenance.
- Batch and Stream Processing integration.

## Technologies Used

- **Programming Languages**: Python
- **Messaging Systems**: Apache Kafka
- **Databases**: DuckDB
- **Visualization Tools**: Streamlit
- **Data Replication**: [Redset](https://github.com/amazon-science/redset)

## Components

### 1. `requirements.txt`
This file lists all the dependencies required for the project.


### 2. Producers

The producers generate data streams for both views.

- **`producer_expert.py`**: Produces expert-level data.
- **`producer_aggregate.py`**: Generates summarized aggregate data.

### 3. Dashboards

These scripts serve the dashboard views for different user requirements.

- **`dashboard_expert.py`**: Displays the expert-level data with detailed insights.
- **`dashboard_aggregate.py`**: Displays a summarized view for high-level analysis.

### 4. Common File

- **`common.py`**: This file contains shared utilities, configurations, and functions used across the producers and dashboards to ensure consistency and reusability.

## Installation and Setup
### Clone the repository:
git clone https://github.com/SayeedaBegam/Pipeline_Pioneer_UTN
cd Pipeline_Pioneer_UTN

###Install dependencies:

pip install -r requirements.txt
###Run the producer(s):
python producer_expert.py
python producer_aggregate.py
###Start the dashboards:

streamlit run common.py

##Contributions
Feel free to fork the repository and submit pull requests for enhancements.

##References
-**Redset**: Redset Repository
-Why TPC is not enough: VLDB 2024 Publication
##License
This project is licensed under Pipeline Pioneers@UTN.










