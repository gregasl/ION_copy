import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Load the parquet files from script directory
order_data = pd.read_parquet(os.path.join(script_dir, 'order_data.parquet'))
apex_data = pd.read_parquet(os.path.join(script_dir, 'apex_data.parquet'))

# Set display options for full timestamps and precision
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.precision', 6)
pd.options.display.float_format = '{:.6f}'.format

# Make sure timestamps are in datetime format with full precision
order_data['timestamp'] = pd.to_datetime(order_data['timestamp'])
apex_data['Start Date'] = pd.to_datetime(apex_data['Entry Time'])

# Print basic information about both datasets
print("Order Data Shape:", order_data.shape)
print("Apex Data Shape:", apex_data.shape)

print("\nOrder Data Columns:", order_data.columns.tolist())
print("\nApex Data Columns:", apex_data.columns.tolist())

# Display sample data
print("\nOrder Data Sample:")
print(order_data.head(3))

print("\nApex Data Sample:")
print(apex_data.head(3))

# Map direction from order_data to apex_data terminology
# BUY in order_data = REV in apex_data
# SELL in order_data = REP in apex_data
direction_mapping = {
    'Buy': 'REV',
    'Sell': 'REP'
}

# Map source to bilateral cpty shortcode
source_mapping = {
    'BTEC_REPO_US': 'BTEC',
    'DEALERWEB_REPO': 'DEAL',
    'FENICSUS_REPO': 'FENICS'
}

# Create a new column with mapped direction for easier matching
order_data['mapped_direction'] = order_data['direction'].map(direction_mapping)

# Extract CUSIP from instrumentId in order_data
# The CUSIP is part of the instrumentId field
def extract_cusip(instrument_id):
    # Look for a 9-character pattern that could be a CUSIP
    # CUSIP is typically 9 characters (letters and numbers)
    import re
    match = re.search(r'[A-Z0-9]{9}', str(instrument_id))
    if match:
        return match.group(0)
    return None

order_data['cusip'] = order_data['instrumentId'].apply(extract_cusip)

# Function to check if timestamps are close enough (within a time window)
def is_time_close(time1, time2, window_minutes=5):
    time_diff = abs(time1 - time2)
    return time_diff <= timedelta(minutes=window_minutes)

# Function to find matches between order_data and apex_data
def find_matches(order_row, apex_data, time_window_minutes=5):
    # Filter apex_data by CUSIP
    cusip_matches = apex_data[apex_data['CUSIP'] == order_row['cusip']]
    print(f"\nMatching order {order_row['orderId']} - {order_row['cusip']}")
    print(f"CUSIP matches: {len(cusip_matches)}")
    
    if cusip_matches.empty:
        return None
    
    # Filter by direction (BUY=REV, SELL=REP)
    direction_matches = cusip_matches[cusip_matches['Type'] == order_row['mapped_direction']]
    print(f"Direction matches: {len(direction_matches)}")
    
    if direction_matches.empty:
        return None
        
    # Filter by source/shortcode
    shortcode = source_mapping.get(order_row['source'])
    if shortcode:
        source_matches = direction_matches[direction_matches['Bilateral Cpty ShortCode'] == shortcode]
        print(f"Source matches: {len(source_matches)}")
        
        if source_matches.empty:
            return None
    else:
        source_matches = direction_matches
        print("No source mapping found, skipping source filter")
    
    # Convert order size to quantity (multiply by 1 million)
    order_quantity = order_row['size'] * 1_000_000
    print(f"Looking for quantity: {order_quantity:,.0f}")
    
    # Filter by size and price
    size_price_matches = source_matches[
        (np.isclose(source_matches['Quantity'], order_quantity)) & 
        (np.isclose(source_matches['Rate'], order_row['price']))
    ]
    print(f"Size and price matches: {len(size_price_matches)}")
    print(f"Order price: {order_row['price']}")
    if not size_price_matches.empty:
        print("Sample matching rates:", size_price_matches['Rate'].head().tolist())
        # Return the first match found
        return size_price_matches.iloc[0]
    
    return None

# Create results dictionary to store matches
results = {
    'matched_orders': [],
    'unmatched_orders': []
}

# Set a time window for matching (in minutes)
time_window = 2

# Process each order in order_data
for idx, order_row in order_data.iterrows():
    match = find_matches(order_row, apex_data, time_window)
    
    if match is not None:
        results['matched_orders'].append({
            'order_id': order_row['orderId'] if not pd.isna(order_row['orderId']) else None,
            'order_timestamp': order_row['timestamp'],
            'order_cusip': order_row['cusip'],
            'order_direction': order_row['direction'],
            'order_size': order_row['size'],
            'order_price': order_row['price'],
            'order_source': order_row['source'],
            'apex_trade_no': match['Trade No'],
            'apex_timestamp': match['Start Date'],
            'apex_cusip': match['CUSIP'],
            'apex_type': match['Type'],
            'apex_quantity': match['Quantity'],
            'apex_rate': match['Rate'],
            'apex_shortcode': match['Bilateral Cpty ShortCode']
        })
    else:
        results['unmatched_orders'].append({
            'order_id': order_row['orderId'] if not pd.isna(order_row['orderId']) else None,
            'timestamp': order_row['timestamp'],
            'cusip': order_row['cusip'],
            'direction': order_row['direction'],
            'size': order_row['size'],
            'price': order_row['price'],
            'source': order_row['source']
        })

# Convert results to DataFrames
matched_df = pd.DataFrame(results['matched_orders'])
unmatched_df = pd.DataFrame(results['unmatched_orders'])

# Define reverse matching function
def find_reverse_matches(apex_row, order_data, time_window_minutes=5):
    # Filter order_data by CUSIP
    cusip_matches = order_data[order_data['cusip'] == apex_row['CUSIP']]
    
    if cusip_matches.empty:
        return None
    
    # Filter by direction (REV=BUY, REP=SELL)
    reverse_direction_map = {'REV': 'Buy', 'REP': 'Sell'}
    direction_matches = cusip_matches[cusip_matches['direction'] == reverse_direction_map.get(apex_row['Type'])]
    
    if direction_matches.empty:
        return None
    
    # Filter by source/shortcode
    shortcode = apex_row['Bilateral Cpty ShortCode']
    source = next((src for src, code in source_mapping.items() if code == shortcode), None)
    if source:
        source_matches = direction_matches[direction_matches['source'] == source]
        if source_matches.empty:
            return None
    else:
        source_matches = direction_matches
    
    # Convert apex quantity to size
    apex_size = apex_row['Quantity'] / 1_000_000
    
    # Filter by size and price
    size_price_matches = source_matches[
        (np.isclose(source_matches['size'], apex_size)) & 
        (np.isclose(source_matches['price'], apex_row['Rate']))
    ]
    
    if size_price_matches.empty:
        return None
    
    # Return first match (we can add time window filtering later if needed)
    return size_price_matches.iloc[0]

# Process unmatched apex trades
unmatched_apex = []
for idx, apex_row in apex_data.iterrows():
    match = find_reverse_matches(apex_row, order_data, time_window)
    
    if match is None:
        unmatched_apex.append({
            'trade_no': apex_row['Trade No'],
            'start_date': apex_row['Start Date'],
            'cusip': apex_row['CUSIP'],
            'type': apex_row['Type'],
            'quantity': apex_row['Quantity'],
            'rate': apex_row['Rate']
        })

# Convert to DataFrame
unmatched_apex_df = pd.DataFrame(unmatched_apex)

# Now display all results together
print("\n--- DETAILED RESULTS ---")
print(f"Total orders: {len(order_data)}")
print(f"Matched orders: {len(matched_df)}")
print(f"Unmatched orders: {len(unmatched_df)}")
print(f"Total apex trades: {len(apex_data)}")
print(f"Unmatched apex trades: {len(unmatched_apex_df)}")

print("\nMatched Orders Detail (first 10):")
if not matched_df.empty:
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(matched_df.head(10))
    print("\nMatched Orders Summary:")
    print(matched_df.describe())
else:
    print("No matched orders found")

print("\nUnmatched Orders Detail (first 10):")
if not unmatched_df.empty:
    print(unmatched_df.head(10))
    print("\nUnmatched Orders by CUSIP:")
    print(unmatched_df['cusip'].value_counts().head(10))
    print("\nUnmatched Orders by Direction:")
    print(unmatched_df['direction'].value_counts())
else:
    print("No unmatched orders found")

print("\n--- UNMATCHED APEX TRADES DETAIL ---")
print(f"Total apex trades: {len(apex_data)}")
print(f"Unmatched apex trades: {len(unmatched_apex_df)}")

print("\nUnmatched Apex Trades (first 10):")
if not unmatched_apex_df.empty:
    print(unmatched_apex_df.head(10))
    print("\nUnmatched Apex Trades by CUSIP:")
    print(unmatched_apex_df['cusip'].value_counts().head(10))
    print("\nUnmatched Apex Trades by Type:")
    print(unmatched_apex_df['type'].value_counts())
    print("\nUnmatched Apex Trades Summary:")
    print(unmatched_apex_df.describe())
else:
    print("No unmatched apex trades found")

def save_results(df, base_name, script_dir):
    """Save results to both CSV and Parquet with deduplication by date"""
    csv_path = os.path.join(script_dir, f'{base_name}.csv')
    parquet_path = os.path.join(script_dir, f'{base_name}.parquet')
    
    # Save current results to CSV
    df.to_csv(csv_path, index=False, float_format='%.6f')
    
    # Handle Parquet with deduplication
    if os.path.exists(parquet_path):
        existing_df = pd.read_parquet(parquet_path)
        if 'timestamp' in df.columns or 'order_timestamp' in df.columns:
            date_col = 'timestamp' if 'timestamp' in df.columns else 'order_timestamp'
            # Get unique dates in new data
            new_dates = df[date_col].dt.date.unique()
            # Remove existing data for these dates
            existing_df = existing_df[~existing_df[date_col].dt.date.isin(new_dates)]
        # Combine with new data
        df = pd.concat([existing_df, df], ignore_index=True)
    
    # Save to Parquet
    df.to_parquet(parquet_path, index=False)

# Save detailed results to CSV and Parquet with additional information
save_results(matched_df, 'matched_orders_detailed', script_dir)
save_results(unmatched_df, 'unmatched_orders_detailed', script_dir)
save_results(unmatched_apex_df, 'unmatched_apex_trades_detailed', script_dir)

print("\nDetailed results saved to CSV and Parquet files in script directory")

# Calculate statistics for matched orders
if not matched_df.empty:
    print("\n--- MATCH STATISTICS ---")
    # Time difference between order timestamp and apex timestamp
    matched_df['time_diff_seconds'] = matched_df.apply(
        lambda row: abs((row['order_timestamp'] - row['apex_timestamp']).total_seconds()), axis=1
    )
    print(f"Average time difference: {matched_df['time_diff_seconds'].mean():.2f} seconds")
    print(f"Max time difference: {matched_df['time_diff_seconds'].max():.2f} seconds")
    print(f"Min time difference: {matched_df['time_diff_seconds'].min():.2f} seconds")
    
    # Group by CUSIP and count matches
    cusip_counts = matched_df['order_cusip'].value_counts()
    print("\nMatches by CUSIP:")
    print(cusip_counts.head(10))
    
    # Group by direction and count matches
    direction_counts = matched_df['order_direction'].value_counts()
    print("\nMatches by direction:")
    print(direction_counts)
