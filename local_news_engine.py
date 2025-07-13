import pandas as pd
import uuid
import os
import base64
from datetime import datetime

# --- Configuration ---
LOCAL_NEWS_DATA_PATH = os.path.join('data', 'local_news.csv')
ALERTS_DATA_PATH = os.path.join('data', 'alerts.csv') # Using the same alerts file
UPDATED_LOCAL_NEWS_DATA_PATH = os.path.join('data', 'local_news_with_alerts.csv')

# Define thresholds and keywords for Local News/Events alerts

# Affected Population Estimates
POP_CRITICAL_THRESHOLD = 5_000_000   # e.g., major city-wide impact
POP_MEDIUM_THRESHOLD = 500_000     # e.g., large metropolitan area impact
POP_LOW_THRESHOLD = 50_000         # e.g., town or significant neighborhood impact

# Keywords for Weather Event descriptions
WEATHER_CRITICAL_KEYWORDS = ['cyclone', 'tornado', 'hurricane', 'blizzard', 'flood', 'severe thunderstorm', 'landslide']
WEATHER_MEDIUM_KEYWORDS = ['heavy rain', 'snow storm', 'flash flood', 'heatwave', 'dense fog', 'thunderstorm']
WEATHER_LOW_KEYWORDS = ['rain', 'light snow', 'wind advisory', 'drizzle']

# Keywords for Road/Logistics Impact descriptions
ROAD_CRITICAL_KEYWORDS = ['major highway closure', 'airport closure', 'port strike', 'trucking strike', 'bridge collapse', 'total closure']
ROAD_MEDIUM_KEYWORDS = ['street closure', 'traffic disruption', 'construction delay', 'partial closure']

# Keywords for Public Safety (always Critical regardless of other factors)
PUBLIC_SAFETY_KEYWORDS = ['evacuation', 'lockdown', 'riot', 'protest', 'bomb threat', 'natural disaster', 'emergency']

# --- Helper Functions (re-used for consistency) ---

def load_data(filepath, columns=None):
    """
    Loads a CSV file into a pandas DataFrame.
    Handles EmptyDataError if the file is empty or has no columns.
    If 'columns' are provided, creates an empty DataFrame with those columns if the file is truly empty.
    """
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        print(f"'{filepath}' not found or is empty. Initializing empty DataFrame for it.")
        return pd.DataFrame(columns=columns)

    try:
        df = pd.read_csv(filepath)
        # Ensure all expected columns are present, even if some rows don't have data for them
        if columns:
            for col in columns:
                if col not in df.columns:
                    df[col] = None
        return df
    except pd.errors.EmptyDataError:
        print(f"'{filepath}' exists but has no columns to parse. Initializing empty DataFrame for it.")
        return pd.DataFrame(columns=columns)
    except Exception as e:
        print(f"An unexpected error occurred while loading '{filepath}': {e}")
        return pd.DataFrame(columns=columns)

def save_data(df, filepath):
    """Saves a pandas DataFrame to a CSV file."""
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

def generate_alert_id():
    """Generates a shorter, URL-safe alert ID using Base64 encoding of a UUID."""
    full_uuid = uuid.uuid4()
    short_id = base64.urlsafe_b64encode(full_uuid.bytes).decode('utf-8').rstrip('=')
    return short_id

def log_alert(alerts_df, alert_title, category, severity):
    """
    Logs an alert to the alerts DataFrame and returns its ID.
    """
    alert_id = generate_alert_id()
    new_alert = pd.DataFrame([{
        'alert_id': alert_id,
        'alert_title': alert_title,
        'category': category,
        'severity': severity,
        'timestamp': pd.Timestamp.now()
    }])
    if alerts_df.empty:
        alerts_df = new_alert
    else:
        alerts_df = pd.concat([alerts_df, new_alert], ignore_index=True)
    return alerts_df, alert_id

# --- Rule Engine ---

def local_news_rule_engine(local_news_df, alerts_df):
    """
    Applies local news/event-related rules to the DataFrame and logs alerts.
    Considers event type, impact level, affected population, and event duration.
    """
    print("Running local news/events rule engine...")

    if 'alert_id' not in local_news_df.columns:
        local_news_df['alert_id'] = None

    alerts_generated_count = 0
    current_time = pd.Timestamp.now() # Get current time for active event check

    for index, row in local_news_df.iterrows():
        event_id = row.get('event_id', 'N/A')
        event_type = str(row.get('event_type', '')).lower()
        region_id = row.get('region_id', 'Unknown Region')
        impact_level_csv = str(row.get('impact_level', '')).lower() # impact_level from CSV
        description = str(row.get('description', '')).lower()
        affected_population_estimate = pd.to_numeric(row.get('affected_population_estimate'), errors='coerce')
        route_affected = str(row.get('route_affected', '')).lower()

        event_start_date = pd.to_datetime(row.get('event_start_date'), errors='coerce')
        event_end_date = pd.to_datetime(row.get('event_end_date'), errors='coerce')

        # Skip events that are not currently active or have invalid dates
        if pd.isna(event_start_date) or pd.isna(event_end_date) or \
           current_time < event_start_date or current_time > event_end_date:
            # print(f"Skipping inactive/invalid event: {event_id} ({event_type})") # For debugging
            continue

        context_info = f"Event ID: {event_id}, Region: {region_id}"
        current_alerts_for_row = []
        
        # Default severity based on CSV impact_level, then refine
        severity = "Low" # Default
        if impact_level_csv == 'high':
            severity = "Medium" # We'll start with Medium for high, then elevate to Critical if specific conditions met
        elif impact_level_csv == 'medium':
            severity = "Low" # We'll start with Low for medium, then elevate to Medium if specific conditions met
        elif impact_level_csv == 'critical': # If CSV already has 'critical' as a level
             severity = "Critical"

        alert_title_base = f"{event_type.title()} in {region_id}"
        
        # --- Rule 1: Public Safety/Emergency Events (Always Critical) ---
        if any(keyword in description for keyword in PUBLIC_SAFETY_KEYWORDS):
            alert_title = f"Critical Public Safety Event: {alert_title_base} ({description})"
            category = "Local News/Events"
            severity = "Critical"
            alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1
            # If a critical public safety event is detected, no need to check other rules for this row
            inventory_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
            continue


        # --- Rule 2: Weather Alerts ---
        if 'weather alert' in event_type:
            final_severity = severity # Start with derived severity from CSV impact_level

            if any(keyword in description for keyword in WEATHER_CRITICAL_KEYWORDS) or \
               (pd.notna(affected_population_estimate) and affected_population_estimate >= POP_CRITICAL_THRESHOLD):
                final_severity = "Critical"
            elif any(keyword in description for keyword in WEATHER_MEDIUM_KEYWORDS) or \
                 (pd.notna(affected_population_estimate) and affected_population_estimate >= POP_MEDIUM_THRESHOLD):
                if final_severity != "Critical": # Don't downgrade if already critical
                    final_severity = "Medium"
            elif any(keyword in description for keyword in WEATHER_LOW_KEYWORDS) or \
                 (pd.notna(affected_population_estimate) and affected_population_estimate >= POP_LOW_THRESHOLD):
                if final_severity not in ["Critical", "Medium"]: # Don't downgrade
                    final_severity = "Low"
            
            # Log alert if conditions met
            if final_severity in ["Critical", "Medium", "Low"]:
                alert_title = f"{final_severity} Weather Alert: {alert_title_base} ({description})"
                category = "Local News/Events"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, final_severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 3: Road Closures / Logistics Impacts ---
        if 'road closure' in event_type or any(keyword in description for keyword in ROAD_CRITICAL_KEYWORDS + ROAD_MEDIUM_KEYWORDS):
            final_severity = severity # Start with derived severity from CSV impact_level

            if any(keyword in description for keyword in ROAD_CRITICAL_KEYWORDS) or 'critical' in impact_level_csv:
                final_severity = "Critical"
            elif any(keyword in description for keyword in ROAD_MEDIUM_KEYWORDS) or 'route affected' in description or 'significant delay' in description:
                 if final_severity != "Critical":
                    final_severity = "Medium"
            
            if final_severity in ["Critical", "Medium", "Low"]: # Only log if a severity was assigned
                alert_title = f"{final_severity} Logistics Disruption: {alert_title_base} (Route: {route_affected if route_affected != 'none' else 'N/A'}, Desc: {description})"
                category = "Local News/Events"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, final_severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
        
        # --- Rule 4: Local Festivals / Community Fairs (Opportunity/Minor Disruption) ---
        if 'local festival' in event_type or 'community fair' in event_type:
            final_severity = "Low" # Usually not critical issues, more about opportunity/minor traffic

            if pd.notna(affected_population_estimate) and affected_population_estimate >= POP_MEDIUM_THRESHOLD:
                final_severity = "Medium" # Large event, significant demand shift or traffic
            
            # Always log these, as they are relevant for marketing/staffing
            alert_title = f"{final_severity} Local Event: {alert_title_base} (Pop: {int(affected_population_estimate) if pd.notna(affected_population_estimate) else 'N/A'})"
            category = "Local News/Events"
            alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, final_severity)
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1

        # Link alert_id(s) to the local_news data row
        if current_alerts_for_row:
            local_news_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
        else:
            local_news_df.loc[index, 'alert_id'] = None

    print(f"Local news/events rule engine completed. Generated {alerts_generated_count} alerts.")
    return local_news_df, alerts_df

# --- Main Execution ---
if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)

    alerts_schema = ['alert_id', 'alert_title', 'category', 'severity', 'timestamp']

    local_news_df = load_data(LOCAL_NEWS_DATA_PATH)
    if local_news_df.empty:
        print(f"Error: {LOCAL_NEWS_DATA_PATH} not found or is empty. Please ensure the file has data and correct format.")
    else:
        print(f"Loaded {len(local_news_df)} rows from {LOCAL_NEWS_DATA_PATH}")

        # Convert date columns to datetime objects for comparison
        local_news_df['event_start_date'] = pd.to_datetime(local_news_df['event_start_date'], errors='coerce')
        local_news_df['event_end_date'] = pd.to_datetime(local_news_df['event_end_date'], errors='coerce')

        alerts_df = load_data(ALERTS_DATA_PATH, columns=alerts_schema)
        if not alerts_df.empty:
            if 'timestamp' in alerts_df.columns:
                alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            print(f"Loaded {len(alerts_df)} existing alerts from {ALERTS_DATA_PATH}")
        else:
            print("No existing alerts found or alerts file was empty. Initialized empty alerts DataFrame.")

        updated_local_news_df, updated_alerts_df = local_news_rule_engine(local_news_df.copy(), alerts_df.copy()) 

        save_data(updated_local_news_df, UPDATED_LOCAL_NEWS_DATA_PATH)
        save_data(updated_alerts_df, ALERTS_DATA_PATH)

        print("\n--- Sample of Updated Local News Data (first 10 rows with alerts) ---")
        print(updated_local_news_df[['timestamp', 'event_type', 'region_id', 'impact_level', 'description', 'event_start_date', 'event_end_date', 'affected_population_estimate', 'alert_id']].head(10))

        print("\n--- Sample of Generated Alerts (last 10 alerts) ---")
        print(updated_alerts_df.tail(10))