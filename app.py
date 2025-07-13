import streamlit as st
import pandas as pd
import os # Import os module to handle file paths
import random # Import random for shuffling

# --- Page Configuration ---
st.set_page_config(
    page_title="Walmart India Problem Solver",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS for Styling (mimicking the dark theme) ---
st.markdown(
    """
    <style>
    .reportview-container {
        background: #1a202c;
        color: #e2e8f0;
    }
    .main .block-container {
        padding-top: 2rem;
        padding-right: 2rem;
        padding-left: 2rem;
        padding-bottom: 2rem;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #e2e8f0;
    }
    .stMetric > div > div > div {
        color: #e2e8f0;
    }
    .stMetric > div > label {
        color: #cbd5e0;
    }
    .stMetric > div > div > div > div {
        font-size: 3rem !important; /* Adjust font size for metric values */
        font-weight: bold;
    }
    .stMetric > div > label {
        font-size: 1.125rem; /* Adjust font size for metric labels */
        font-weight: 500;
    }

    /* Custom colors for severity */
    .severity-low { color: #48bb78; font-weight: bold; } /* Green */
    .severity-medium { color: #ecc94b; font-weight: bold; } /* Yellow */
    .severity-high { color: #ed8936; font-weight: bold; } /* Orange */
    .severity-critical { color: #f56565; font-weight: bold; } /* Red */

    /* Custom table styling for the manually rendered table */
    .custom-table-container {
        background-color: #2d3748;
        border-radius: 0.5rem;
        overflow: hidden;
        margin-bottom: 1rem;
    }
    .custom-table-header {
        background-color: #283142;
        padding: 0.75rem 1.5rem;
        display: flex;
        align-items: center;
        border-bottom: 1px solid #4a5568;
    }
    .custom-table-header-cell {
        font-size: 0.75rem;
        font-weight: 500;
        color: #cbd5e0;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 0.5rem 0; /* Adjust padding for header cells */
    }
    .custom-table-row {
        display: flex;
        align-items: center;
        padding: 0.75rem 1.5rem;
        border-bottom: 1px solid #4a5568;
    }
    .custom-table-row:nth-child(odd) {
        background-color: #242c3b;
    }
    .custom-table-row:nth-child(even) {
        background-color: #2d3748;
    }
    .custom-table-cell {
        font-size: 0.875rem;
        color: #e2e8f0;
        padding: 0.5rem 0; /* Adjust padding for data cells */
    }
    /* Adjust button styling within the table */
    .stButton > button {
        background-color: #2563eb;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 0.5rem;
        border: none;
        cursor: pointer;
        transition: background-color 0.3s ease;
    }
    .stButton > button:hover {
        background-color: #1d4ed8;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Load and Process Data ---
@st.cache_data
def load_data(file_path):
    """
    Loads data from a CSV file.
    Checks if the file exists before attempting to read.
    """
    if not os.path.exists(file_path):
        st.error(f"Error: The file '{file_path}' was not found. Please ensure it exists.")
        return pd.DataFrame() # Return an empty DataFrame if file not found
    try:
        df = pd.read_csv(file_path)
        # Ensure timestamp column is datetime type for trend analysis
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        st.error(f"Error loading CSV file: {e}")
        return pd.DataFrame() # Return an empty DataFrame on error

@st.cache_data
def process_alerts(df_raw, max_per_category=4):
    """
    Filters and shuffles alerts:
    - Picks a maximum of 'max_per_category' alerts from each category.
    - Randomly shuffles the combined list of alerts.
    """
    if df_raw.empty:
        return pd.DataFrame()

    processed_alerts = []
    # Get unique categories and shuffle them to ensure random order of processing
    categories = df_raw['category'].unique().tolist()
    random.shuffle(categories)

    for category in categories:
        category_df = df_raw[df_raw['category'] == category]
        # Sample up to max_per_category alerts from the current category
        # If fewer than max_per_category alerts are available, take all of them
        sampled_alerts = category_df.sample(n=min(len(category_df), max_per_category), random_state=None) # random_state=None for true randomness
        processed_alerts.append(sampled_alerts)

    if not processed_alerts:
        return pd.DataFrame()

    # Concatenate all sampled alerts
    df_filtered = pd.concat(processed_alerts)

    # Shuffle the entire DataFrame to mix alerts from different categories
    df_shuffled = df_filtered.sample(frac=1, random_state=None).reset_index(drop=True)
    return df_shuffled


# Define the path to your CSV file
csv_file_path = 'data/alerts.csv'
df_raw_alerts = load_data(csv_file_path) # Load raw data (for bar chart)

# Process the alerts: limit per category and shuffle (for table display and summary cards)
df_alerts = process_alerts(df_raw_alerts, max_per_category=4)


# Only proceed if data was loaded and processed successfully
if not df_alerts.empty: # Use df_alerts for summary counts (sampled data)
    # Calculate summary counts based on the PROCESSED alerts
    total_alerts = len(df_alerts)
    critical_alerts = df_alerts[df_alerts['severity'].str.lower() == 'critical'].shape[0]
    medium_alerts = df_alerts[df_alerts['severity'].str.lower() == 'medium'].shape[0]
    low_alerts = df_alerts[df_alerts['severity'].str.lower() == 'low'].shape[0]
else:
    # Set counts to 0 if no data is loaded or processed
    total_alerts = 0
    critical_alerts = 0
    medium_alerts = 0
    low_alerts = 0

# --- Dashboard Header ---
st.markdown(
    """
    <h1 style="display: flex; align-items: center;">
        <svg class="w-10 h-10 mr-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" style="width: 2.5rem; height: 2.5rem; margin-right: 1rem;">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 14v6m-3-3h6m-9-11h0M6 14h0m6-10h0M9 18h0"></path>
        </svg>
        Walmart India Problem Solver - Dashboard Overview
    </h1>
    """,
    unsafe_allow_html=True
)

st.markdown("---") # A separator for visual appeal

# --- Summary Cards ---
st.subheader("Alert Summary")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
        <div class="card text-center">
            <h2 class="text-lg font-medium text-gray-400">Total Alerts</h2>
            <p class="text-5xl font-bold text-gray-100 mt-2">{total_alerts}</p>
        </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
        <div class="card text-center">
            <h2 class="text-lg font-medium text-gray-400">Critical Alerts</h2>
            <p class="text-5xl font-bold severity-critical mt-2">{critical_alerts}</p>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
        <div class="card text-center">
            <h2 class="text-lg font-medium text-gray-400">Medium Alerts</h2>
            <p class="text-5xl font-bold severity-medium mt-2">{medium_alerts}</p>
        </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
        <div class="card text-center">
            <h2 class="text-lg font-medium text-gray-400">Low Alerts</h2>
            <p class="text-5xl font-bold severity-low mt-2">{low_alerts}</p>
        </div>
    """, unsafe_allow_html=True)

st.markdown("---") # Another separator

# --- Current Problem Alerts Table ---
st.markdown(
    """
    <h2 style="display: flex; align-items: center;">
        <svg class="w-8 h-8 mr-3 text-yellow-400" fill="currentColor" viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg" style="width: 2rem; height: 2rem; margin-right: 0.75rem;">
            <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm-1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l3 3a1 1 0 001.414-1.414L11 9.586V6a1 1 0 00-1-1z" clip-rule="evenodd"></path>
        </svg>
        Current Problem Alerts (Sampled)
    </h2>
    <p class="text-gray-400 mb-6">Click 'View Details' to see specific problem analysis and recommendations:</p>
    """,
    unsafe_allow_html=True
)

# Display the table only if data is available
if not df_alerts.empty:
    st.markdown('<div class="custom-table-container">', unsafe_allow_html=True)

    # Table Header
    col_widths = [1, 3, 1.5, 1, 2, 1] # Relative widths for columns
    header_cols = st.columns(col_widths)
    headers = ["Alert ID", "Alert Title", "Category", "Severity", "Timestamp", "Action"]
    for i, header_text in enumerate(headers):
        with header_cols[i]:
            st.markdown(f'<div class="custom-table-header-cell">{header_text}</div>', unsafe_allow_html=True)

    # Table Rows
    for index, row in df_alerts.iterrows():
        row_cols = st.columns(col_widths)
        with row_cols[0]:
            st.markdown(f'<div class="custom-table-cell">{row["alert_id"]}</div>', unsafe_allow_html=True)
        with row_cols[1]:
            st.markdown(f'<div class="custom-table-cell">{row["alert_title"]}</div>', unsafe_allow_html=True)
        with row_cols[2]:
            st.markdown(f'<div class="custom-table-cell">{row["category"]}</div>', unsafe_allow_html=True)
        with row_cols[3]:
            # Apply severity styling directly to the text
            severity_lower = row["severity"].lower()
            severity_class = ''
            if severity_lower == 'low':
                severity_class = 'severity-low'
            elif severity_lower == 'medium':
                severity_class = 'severity-medium'
            elif severity_lower == 'high':
                severity_class = 'severity-high'
            elif severity_lower == 'critical':
                severity_class = 'severity-critical'
            st.markdown(f'<div class="custom-table-cell {severity_class}">{row["severity"]}</div>', unsafe_allow_html=True)
        with row_cols[4]:
            st.markdown(f'<div class="custom-table-cell">{row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")}</div>', unsafe_allow_html=True) # Format timestamp for display
        with row_cols[5]:
            # Use st.button for actual buttons
            if st.button("View Details", key=f"details_btn_{row['alert_id']}"):
                st.info(f"Viewing details for Alert ID: {row['alert_id']}") # Placeholder for future functionality

    st.markdown('</div>', unsafe_allow_html=True) # Close custom-table-container

else:
    st.info("No alert data to display. Please ensure 'data/alerts.csv' is correctly placed and accessible.")

st.markdown("---") # Another separator

# --- Bar Chart for Categories ---
st.markdown(
    """
    <h2 style="display: flex; align-items: center;">
        <svg class="w-8 h-8 mr-3 text-green-400" fill="currentColor" viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg" style="width: 2rem; height: 2rem; margin-right: 0.75rem;">
            <path fill-rule="evenodd" d="M3 3a1 1 0 00-1 1v12a1 1 0 001 1h14a1 1 0 001-1V4a1 1 0 00-1-1H3zm10 2a1 1 0 00-1 1v8a1 1 0 001 1h2a1 1 0 001-1V6a1 1 0 00-1-1h-2zM6 6a1 1 0 011-1h2a1 1 0 011 1v7a1 1 0 01-1 1H7a1 1 0 01-1-1V6z" clip-rule="evenodd"></path>
        </svg>
        Alerts by Category
    </h2>
    <p class="text-gray-400 mb-6">Total number of alerts per category:</p>
    """,
    unsafe_allow_html=True
)

if not df_raw_alerts.empty:
    # Group by category and count alerts
    alerts_by_category = df_raw_alerts['category'].value_counts().reset_index(name='Alert Count')
    alerts_by_category.columns = ['Category', 'Alert Count'] # Rename columns for clarity

    st.bar_chart(alerts_by_category, x='Category', y='Alert Count', use_container_width=True)
else:
    st.info("No raw alert data available to display category chart.")

