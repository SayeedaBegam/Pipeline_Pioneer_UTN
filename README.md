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
This file lists all the dependencies required for the project. Install them using:

```sh
pip install -r requirements.txt




