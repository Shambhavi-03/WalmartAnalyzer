import pandas as pd
import uuid
import os
import base64 # Import base64

# --- Configuration ---
WEATHER_DATA_PATH = os.path.join('data', 'weather.csv')
ALERTS_DATA_PATH = os.path.join('data', 'alerts.csv')
UPDATED_WEATHER_DATA_PATH = os.path.join('data', 'weather_with_alerts.csv')

# Define thresholds for alerts (refined based on new requirements and typical Indian weather)
# Temperature (Celsius)
TEMP_LOW_SEVERITY_THRESHOLD_C = 35.0   # e.g., for general "hot" conditions
TEMP_CRITICAL_SEVERITY_THRESHOLD_C = 40.0 # e.g., for "heatwave" conditions

# Humidity (Percent)
HUMIDITY_LOW_SEVERITY_THRESHOLD_PERCENT = 80
HUMIDITY_CRITICAL_SEVERITY_THRESHOLD_PERCENT = 90

# Wind Speed (MPS - Meters per Second)
WIND_LOW_SEVERITY_THRESHOLD_MPS = 12.0 # Approx 43 km/h
WIND_CRITICAL_SEVERITY_THRESHOLD_MPS = 20.0 # Approx 72 km/h

# --- Helper Functions ---

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
                    df[col] = None # Or pd.NA, depending on desired null representation
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
    # Generate a standard UUID
    full_uuid = uuid.uuid4()
    # Encode the UUID's bytes into a URL-safe Base64 string and remove padding
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

def weather_rule_engine(weather_df, alerts_df):
    """
    Applies weather-related rules to the weather DataFrame
    and logs alerts. Skips erroneous data.
    """
    print("Running weather rule engine...")

    # Ensure 'alert_id' column exists in weather_df, initialize with None/NaN
    if 'alert_id' not in weather_df.columns:
        weather_df['alert_id'] = None

    alerts_generated_count = 0

    # Filter out rows with erroneous Weather_Fetch_Status at the start
    initial_rows = len(weather_df)
    weather_df = weather_df[weather_df['Weather_Fetch_Status'] == 'Success'].copy()
    filtered_rows = initial_rows - len(weather_df)
    if filtered_rows > 0:
        print(f"Skipped {filtered_rows} rows due to erroneous Weather_Fetch_Status.")

    for index, row in weather_df.iterrows():
        location = row['City & State'] if pd.notna(row['City & State']) else row['Full Address']
        current_alerts_for_row = [] # To store alert IDs for this specific row

        # Rule 1: Temperature Alerts
        if pd.notna(row['Temperature_C']):
            temp_c = float(row['Temperature_C'])
            if temp_c >= TEMP_CRITICAL_SEVERITY_THRESHOLD_C:
                alert_title = f"Critical Heatwave in {location} ({temp_c}°C)"
                category = "Weather"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif temp_c >= TEMP_LOW_SEVERITY_THRESHOLD_C:
                alert_title = f"Extreme Heat in {location} ({temp_c}°C)"
                category = "Weather"
                severity = "Low" # As per the last request, this is 'Low' unless further thresholding is needed for 'Medium'
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # Rule 2: Humidity Alerts
        if pd.notna(row['Humidity_Percent']):
            humidity_percent = float(row['Humidity_Percent'])
            if humidity_percent >= HUMIDITY_CRITICAL_SEVERITY_THRESHOLD_PERCENT:
                alert_title = f"Critical Humidity in {location} ({humidity_percent}%)"
                category = "Weather"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif humidity_percent >= HUMIDITY_LOW_SEVERITY_THRESHOLD_PERCENT:
                alert_title = f"High Humidity in {location} ({humidity_percent}%)"
                category = "Weather"
                severity = "Low" # As per the last request
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # Rule 3: Wind Speed Alerts
        if pd.notna(row['Wind_Speed_MPS']):
            wind_speed_mps = float(row['Wind_Speed_MPS'])
            if wind_speed_mps >= WIND_CRITICAL_SEVERITY_THRESHOLD_MPS:
                alert_title = f"Critical Wind Warning in {location} ({wind_speed_mps} MPS)"
                category = "Weather"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif wind_speed_mps >= WIND_LOW_SEVERITY_THRESHOLD_MPS:
                alert_title = f"High Wind Advisory in {location} ({wind_speed_mps} MPS)"
                category = "Weather"
                severity = "Low" # As per the last request
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # Rule 4: Rain/Storm Alerts (based on Weather_Description)
        weather_desc = str(row.get('Weather_Description', '')).lower()
        
        critical_rain_keywords = ['cyclone', 'heavy storm', 'torrential rain', 'flood', 'severe thunderstorm', 'hurricane', 'typhoon']
        low_severity_rain_keywords = ['rain', 'storm', 'thunderstorm', 'drizzle', 'shower', 'monsoon']

        if any(keyword in weather_desc for keyword in critical_rain_keywords):
            alert_title = f"Critical Storm/Rainfall in {location} ({weather_desc})"
            category = "Weather"
            severity = "Critical"
            alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1
        elif any(keyword in weather_desc for keyword in low_severity_rain_keywords):
            alert_title = f"Heavy Rain/Storm in {location} ({weather_desc})"
            category = "Weather"
            severity = "Low" # As per the last request
            alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
            current_alerts_for_row.append(new_alert_id)
            alerts_generated_count += 1

        # Link alert_id(s) to the weather data row
        if current_alerts_for_row:
            weather_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
        else:
            weather_df.loc[index, 'alert_id'] = None # Explicitly set to None if no alerts

    print(f"Weather rule engine completed. Generated {alerts_generated_count} alerts.")
    return weather_df, alerts_df

# --- Main Execution ---
if __name__ == "__main__":
    # Ensure the 'data' directory exists
    os.makedirs('data', exist_ok=True)

    # Define the schema for the alerts table for initial creation if it's truly empty
    alerts_schema = ['alert_id', 'alert_title', 'category', 'severity', 'timestamp']

    # Load existing weather data
    weather_df = load_data(WEATHER_DATA_PATH)
    if weather_df.empty:
        print(f"Error: {WEATHER_DATA_PATH} not found or is empty after loading. Please ensure the file has data and correct format.")
    else:
        print(f"Loaded {len(weather_df)} rows from {WEATHER_DATA_PATH}")

        # Load existing alerts data or create an empty one with schema if file is empty
        alerts_df = load_data(ALERTS_DATA_PATH, columns=alerts_schema)
        
        if not alerts_df.empty:
            if 'timestamp' in alerts_df.columns:
                alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            print(f"Loaded {len(alerts_df)} existing alerts from {ALERTS_DATA_PATH}")
        else:
            print("No existing alerts found or alerts file was empty. Initialized empty alerts DataFrame.")

        # Run the rule engine
        updated_weather_df, updated_alerts_df = weather_rule_engine(weather_df.copy(), alerts_df.copy()) 

        # Save the updated dataframes
        save_data(updated_weather_df, UPDATED_WEATHER_DATA_PATH)
        save_data(updated_alerts_df, ALERTS_DATA_PATH)

        print("\n--- Sample of Updated Weather Data (first 10 rows with alerts) ---")
        print(updated_weather_df[['City & State', 'Temperature_C', 'Feels_Like_C', 'Humidity_Percent', 'Wind_Speed_MPS', 'Weather_Description', 'Weather_Fetch_Status', 'alert_id']].head(10))

        print("\n--- Sample of Generated Alerts (last 10 alerts) ---")
        print(updated_alerts_df.tail(10))