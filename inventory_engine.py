import pandas as pd
import uuid
import os
import base64 # Import base64 for shorter IDs

# --- Configuration ---
INVENTORY_DATA_PATH = os.path.join('data', 'inventory.csv')
ALERTS_DATA_PATH = os.path.join('data', 'alerts.csv') # Using the same alerts file
UPDATED_INVENTORY_DATA_PATH = os.path.join('data', 'inventory_with_alerts.csv')

# Define thresholds for Inventory alerts (CRITICAL, MEDIUM, LOW)

# 1. Stockout Risk Thresholds (based on current_stock relative to safety_stock_units/reorder_point_units)
# Note: Lower current stock means higher risk
STOCKOUT_CRITICAL_THRESHOLD_FACTOR = 0.2 # Current stock is less than 20% of safety stock, very high risk
STOCKOUT_MEDIUM_THRESHOLD_FACTOR = 0.5   # Current stock is less than 50% of safety stock
STOCKOUT_LOW_THRESHOLD_FACTOR = 1.0      # Current stock is less than 100% of safety stock (i.e., below safety stock)
REORDER_POINT_THRESHOLD_FACTOR = 1.0     # Current stock is below reorder point (triggers reorder, not necessarily stockout risk)


# 2. Overstock Risk Thresholds (based on current_stock relative to storage_capacity_units or Days of Supply)
# Note: Higher current stock means higher risk
OVERSTOCK_CRITICAL_CAPACITY_FACTOR = 1.05 # Current stock exceeds capacity by 5% (critical physical issue)
OVERSTOCK_MEDIUM_CAPACITY_FACTOR = 0.95   # Current stock is 95-105% of capacity (approaching limit)
OVERSTOCK_LOW_CAPACITY_FACTOR = 0.85      # Current stock is 85-95% of capacity (high but manageable)

# Days of Supply (DoC): current_stock / daily_sales_avg
OVERSTOCK_CRITICAL_DOS = 90  # More than 90 days of supply (severe overstock)
OVERSTOCK_MEDIUM_DOS = 60    # More than 60 days of supply
OVERSTOCK_LOW_DOS = 30     # More than 30 days of supply


# 3. Inventory Discrepancy Thresholds (on_hand_units vs. available_for_sale_units)
# Note: Absolute difference. For demo, setting these to trigger on sample data.
DISCREPANCY_CRITICAL_ABS = 10 # Difference of 10 units or more
DISCREPANCY_MEDIUM_ABS = 5    # Difference of 5-9 units (e.g., for Store_A1, Store_A4 in sample)
DISCREPANCY_LOW_ABS = 1       # Any difference (1-4 units)

# 4. Sales Velocity Anomalies (last_24h_sales vs. daily_sales_avg)
# Slow-Moving Inventory: last_24h_sales is much lower than daily_sales_avg
SLOW_MOVING_CRITICAL_SALES_FACTOR = 0.0  # 0 sales in 24h, AND significant stock
SLOW_MOVING_MEDIUM_SALES_FACTOR = 0.25 # Last 24h sales < 25% of daily avg
SLOW_MOVING_LOW_SALES_FACTOR = 0.5   # Last 24h sales < 50% of daily avg

# High Sales Velocity (potential for fast depletion): last_24h_sales is much higher than daily_sales_avg
HIGH_SALES_CRITICAL_FACTOR = 2.0  # Last 24h sales > 200% of daily avg AND stock critically low
HIGH_SALES_MEDIUM_FACTOR = 1.5    # Last 24h sales > 150% of daily avg
HIGH_SALES_LOW_FACTOR = 1.2       # Last 24h sales > 120% of daily avg

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

def inventory_rule_engine(inventory_df, alerts_df):
    """
    Applies inventory-related rules to the DataFrame and logs alerts.
    """
    print("Running inventory rule engine...")

    if 'alert_id' not in inventory_df.columns:
        inventory_df['alert_id'] = None

    alerts_generated_count = 0

    for index, row in inventory_df.iterrows():
        location_id = row.get('location_id', 'Unknown Location')
        product_sku = row.get('product_sku', 'Unknown SKU')
        context_info = f"SKU: {product_sku}, Location: {location_id}"
        current_alerts_for_row = []

        # Ensure necessary columns are numeric and not NaN
        current_stock = pd.to_numeric(row.get('current_stock'), errors='coerce')
        in_transit_in = pd.to_numeric(row.get('in_transit_in'), errors='coerce')
        daily_sales_avg = pd.to_numeric(row.get('daily_sales_avg'), errors='coerce')
        last_24h_sales = pd.to_numeric(row.get('last_24h_sales'), errors='coerce')
        safety_stock_units = pd.to_numeric(row.get('safety_stock_units'), errors='coerce')
        reorder_point_units = pd.to_numeric(row.get('reorder_point_units'), errors='coerce')
        storage_capacity_units = pd.to_numeric(row.get('storage_capacity_units'), errors='coerce')
        on_hand_units = pd.to_numeric(row.get('on_hand_units'), errors='coerce')
        available_for_sale_units = pd.to_numeric(row.get('available_for_sale_units'), errors='coerce')

        # --- Rule 1: Stockout Risk ---
        # Consider potential stock after incoming transit
        effective_stock = current_stock + in_transit_in if pd.notna(current_stock) and pd.notna(in_transit_in) else current_stock

        if pd.notna(effective_stock) and pd.notna(safety_stock_units) and pd.notna(reorder_point_units):
            if effective_stock <= 0: # Absolute stockout
                alert_title = f"Critical Stockout for {context_info} (Stock: {int(current_stock)})"
                category = "Inventory"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif effective_stock < (safety_stock_units * STOCKOUT_CRITICAL_THRESHOLD_FACTOR):
                alert_title = f"Critical Stock Risk for {context_info} (Stock: {int(effective_stock)} < {int(safety_stock_units * STOCKOUT_CRITICAL_THRESHOLD_FACTOR)} of Safety Stock)"
                category = "Inventory"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif effective_stock < (safety_stock_units * STOCKOUT_MEDIUM_THRESHOLD_FACTOR):
                alert_title = f"Medium Stock Risk for {context_info} (Stock: {int(effective_stock)} < {int(safety_stock_units * STOCKOUT_MEDIUM_THRESHOLD_FACTOR)} of Safety Stock)"
                category = "Inventory"
                severity = "Medium"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif effective_stock < (safety_stock_units * STOCKOUT_LOW_THRESHOLD_FACTOR):
                alert_title = f"Low Stock Risk for {context_info} (Stock: {int(effective_stock)} < Safety Stock)"
                category = "Inventory"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif effective_stock < reorder_point_units: # Below reorder point but above safety stock
                alert_title = f"Reorder Point Reached for {context_info} (Stock: {int(effective_stock)})"
                category = "Inventory"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 2: Overstock Risk ---
        if pd.notna(current_stock) and pd.notna(storage_capacity_units) and pd.notna(daily_sales_avg) and daily_sales_avg > 0:
            days_of_supply = current_stock / daily_sales_avg

            if current_stock > (storage_capacity_units * OVERSTOCK_CRITICAL_CAPACITY_FACTOR) or days_of_supply > OVERSTOCK_CRITICAL_DOS:
                alert_title = f"Critical Overstock for {context_info} (Stock: {int(current_stock)}, Capacity: {int(storage_capacity_units)}, DoS: {days_of_supply:.1f})"
                category = "Inventory"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif current_stock > (storage_capacity_units * OVERSTOCK_MEDIUM_CAPACITY_FACTOR) or days_of_supply > OVERSTOCK_MEDIUM_DOS:
                alert_title = f"Medium Overstock for {context_info} (Stock: {int(current_stock)}, Capacity: {int(storage_capacity_units)}, DoS: {days_of_supply:.1f})"
                category = "Inventory"
                severity = "Medium"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif current_stock > (storage_capacity_units * OVERSTOCK_LOW_CAPACITY_FACTOR) or days_of_supply > OVERSTOCK_LOW_DOS:
                alert_title = f"Low Overstock for {context_info} (Stock: {int(current_stock)}, Capacity: {int(storage_capacity_units)}, DoS: {days_of_supply:.1f})"
                category = "Inventory"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # --- Rule 3: Inventory Discrepancy ---
        if pd.notna(on_hand_units) and pd.notna(available_for_sale_units):
            discrepancy = abs(on_hand_units - available_for_sale_units)
            if discrepancy >= DISCREPANCY_CRITICAL_ABS:
                alert_title = f"Critical Inventory Discrepancy for {context_info} (Diff: {int(discrepancy)})"
                category = "Inventory"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif discrepancy >= DISCREPANCY_MEDIUM_ABS:
                alert_title = f"Medium Inventory Discrepancy for {context_info} (Diff: {int(discrepancy)})"
                category = "Inventory"
                severity = "Medium"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif discrepancy >= DISCREPANCY_LOW_ABS:
                alert_title = f"Low Inventory Discrepancy for {context_info} (Diff: {int(discrepancy)})"
                category = "Inventory"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
        
        # --- Rule 4: Sales Velocity Anomalies (Slow-Moving) ---
        if pd.notna(last_24h_sales) and pd.notna(daily_sales_avg) and pd.notna(current_stock) and pd.notna(reorder_point_units):
            if daily_sales_avg > 0: # Only check if there's an expectation of sales
                if last_24h_sales == 0 and current_stock > reorder_point_units:
                    alert_title = f"Critical Slow-Moving Inventory for {context_info} (0 Sales, High Stock)"
                    category = "Inventory"
                    severity = "Critical"
                    alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                    current_alerts_for_row.append(new_alert_id)
                    alerts_generated_count += 1
                elif last_24h_sales < (daily_sales_avg * SLOW_MOVING_MEDIUM_SALES_FACTOR):
                    alert_title = f"Medium Slow-Moving Inventory for {context_info} ({int(last_24h_sales)} sales vs {int(daily_sales_avg)} avg)"
                    category = "Inventory"
                    severity = "Medium"
                    alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                    current_alerts_for_row.append(new_alert_id)
                    alerts_generated_count += 1
                elif last_24h_sales < (daily_sales_avg * SLOW_MOVING_LOW_SALES_FACTOR):
                    alert_title = f"Low Slow-Moving Inventory for {context_info} ({int(last_24h_sales)} sales vs {int(daily_sales_avg)} avg)"
                    category = "Inventory"
                    severity = "Low"
                    alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                    current_alerts_for_row.append(new_alert_id)
                    alerts_generated_count += 1

        # --- Rule 5: Sales Velocity Anomalies (High Sales / Fast Depletion) ---
        if pd.notna(last_24h_sales) and pd.notna(daily_sales_avg) and pd.notna(current_stock) and daily_sales_avg > 0:
            if last_24h_sales > (daily_sales_avg * HIGH_SALES_CRITICAL_FACTOR) and current_stock < (daily_sales_avg * 2): # Selling extremely fast, very low stock
                alert_title = f"Critical High Sales Velocity for {context_info} (Sales: {int(last_24h_sales)}x avg, Stock: {int(current_stock)})"
                category = "Inventory"
                severity = "Critical"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif last_24h_sales > (daily_sales_avg * HIGH_SALES_MEDIUM_FACTOR) and current_stock < (daily_sales_avg * 3): # Selling fast, medium stock
                alert_title = f"Medium High Sales Velocity for {context_info} (Sales: {int(last_24h_sales)}x avg, Stock: {int(current_stock)})"
                category = "Inventory"
                severity = "Medium"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1
            elif last_24h_sales > (daily_sales_avg * HIGH_SALES_LOW_FACTOR) and current_stock < (daily_sales_avg * 4): # Selling slightly faster, decent stock
                alert_title = f"Low High Sales Velocity for {context_info} (Sales: {int(last_24h_sales)}x avg, Stock: {int(current_stock)})"
                category = "Inventory"
                severity = "Low"
                alerts_df, new_alert_id = log_alert(alerts_df, alert_title, category, severity)
                current_alerts_for_row.append(new_alert_id)
                alerts_generated_count += 1

        # Link alert_id(s) to the inventory data row
        if current_alerts_for_row:
            inventory_df.loc[index, 'alert_id'] = ",".join(current_alerts_for_row)
        else:
            inventory_df.loc[index, 'alert_id'] = None

    print(f"Inventory rule engine completed. Generated {alerts_generated_count} alerts.")
    return inventory_df, alerts_df

# --- Main Execution ---
if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)

    alerts_schema = ['alert_id', 'alert_title', 'category', 'severity', 'timestamp']

    inventory_df = load_data(INVENTORY_DATA_PATH)
    if inventory_df.empty:
        print(f"Error: {INVENTORY_DATA_PATH} not found or is empty. Please ensure the file has data and correct format.")
    else:
        print(f"Loaded {len(inventory_df)} rows from {INVENTORY_DATA_PATH}")

        alerts_df = load_data(ALERTS_DATA_PATH, columns=alerts_schema)
        if not alerts_df.empty:
            if 'timestamp' in alerts_df.columns:
                alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            print(f"Loaded {len(alerts_df)} existing alerts from {ALERTS_DATA_PATH}")
        else:
            print("No existing alerts found or alerts file was empty. Initialized empty alerts DataFrame.")

        updated_inventory_df, updated_alerts_df = inventory_rule_engine(inventory_df.copy(), alerts_df.copy()) 

        save_data(updated_inventory_df, UPDATED_INVENTORY_DATA_PATH)
        save_data(updated_alerts_df, ALERTS_DATA_PATH)

        print("\n--- Sample of Updated Inventory Data (first 10 rows with alerts) ---")
        print(updated_inventory_df[['timestamp', 'location_id', 'product_sku', 'current_stock', 'safety_stock_units', 'reorder_point_units', 'on_hand_units', 'available_for_sale_units', 'daily_sales_avg', 'last_24h_sales', 'alert_id']].head(10))

        print("\n--- Sample of Generated Alerts (last 10 alerts) ---")
        print(updated_alerts_df.tail(10))