import streamlit as st
# Initialize the Streamlit layout and options
st.set_page_config(page_title="Redset Dashboard", page_icon="📀", layout="wide")

import Dashboard_Live_Final as live
import Dashboard_Historical_Final as historical

st.header("Redset Dashboard")

# Sidebar configuration
st.sidebar.header("Menu")
view_mode = st.sidebar.radio("Select View", ("Aggregate View", "Expert View"))


# Add custom styling (CSS) for Query Counter and Leaderboard
st.markdown("""
    <style>
    body {
        animation: fadeIn 1.5s ease-in;
    }
    
    @keyframes fadeIn {
        0% {
            opacity: 0;
        }
        100% {
            opacity: 1;
        }
    }

    /* Section transitions */
    .section {
        animation: fadeUp 0.5s ease-in-out;
    }

    @keyframes fadeUp {
        0% {
            transform: translateY(20px);
            opacity: 0;
        }
        100% {
            transform: translateY(0);
            opacity: 1;
        }
    }

    /* Fun animations on hover */
    .hover-box:hover {
        transform: scale(1.05);
        transition: all 0.3s ease;
    }
    
    /* Styling for Query Counter Table */
    .query-counter-table {
        width: 100%;
        margin: 20px 0;
        padding: 15px;
        border-collapse: collapse;
        background-color: #f4f4f9;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
    }

    .query-counter-table th, .query-counter-table td {
        padding: 12px;
        text-align: center;
        font-size: 16px;
        color: #333;
        border: 1px solid #ddd;
    }

    .query-counter-table th {
        background-color: #4CAF50;
        color: white;
        font-weight: bold;
    }

    .query-counter-table td {
        background-color: #f9f9f9;
    }

    .query-counter-table tr:nth-child(even) td {
        background-color: #f1f1f1;
    }

    /* Styling for Leaderboard */
    .leaderboard-container {
        margin-top: 30px;
        padding: 20px;
        background-color: #fff;
        border-radius: 10px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }

    .leaderboard-header {
        font-size: 24px;
        font-weight: bold;
        text-align: center;
        margin-bottom: 20px;
        color: #4CAF50;
    }

    .leaderboard-table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 15px;
    }

    .leaderboard-table th, .leaderboard-table td {
        padding: 15px;
        text-align: center;
        font-size: 18px;
        color: #333;
        border: 1px solid #ddd;
    }

    .leaderboard-table th {
        background-color: #4CAF50;
        color: white;
        font-weight: bold;
    }

    .leaderboard-table tr:nth-child(even) td {
        background-color: #f9f9f9;
    }

    .leaderboard-table tr:hover td {
        background-color: #e3f2fd;
        cursor: pointer;
    }

    .leaderboard-table td {
        background-color: #fafafa;
    }

    /* Adding Hover Effect for Leaderboard Rows */
    .leaderboard-table td:hover {
        background-color: #e3f2fd;
    }
    
    </style>
""", unsafe_allow_html=True)


if view_mode == "Aggregate View":
    live.Kafka_topic_to_DuckDB()
elif view_mode == "Expert View":
    historical.update_tables_periodically()


# Footer: 
st.markdown("""
    <footer style="text-align:center; font-size:12px; color:grey; padding-top:20px; border-top: 1px solid #e0e0e0; margin-top:20px;">
        <p>Pipeline Pioneers &copy; 2025 | UTN</p>
    </footer>
""", unsafe_allow_html=True)


hide_st_style = """
<style>
#MainMenu {visibility:hidden;}
footer
header {visibility:hidden;}
</style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)
