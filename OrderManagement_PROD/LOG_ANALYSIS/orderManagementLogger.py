import pandas as pd
import os
import re
from datetime import datetime
import glob

def parse_order_management_log(log_file_path):
    """
    Parse the order management log file and convert it to a pandas DataFrame,
    validating that each SEND operation has a corresponding SENT.
    
    Args:
        log_file_path: Path to the log file
        
    Returns:
        tuple: (DataFrame with SENT operations, list of unmatched SEND operations)
    """
    # Read the log file
    with open(log_file_path, 'r') as file:
        lines = file.readlines()
    
    # Extract the header line (first line) and get column names
    header = lines[0].strip()
    columns = header.split(',')
    
    # Initialize lists to store data rows
    send_operations = []
    sent_operations = []
    
    # Process each log entry (skipping the header line)
    for line in lines[1:]:
        line = line.strip()
        if not line:  # Skip empty lines
            continue
            
        # Parse the line
        row_values = line.split(',')
        
        # If we have the correct number of fields, process the row
        if len(row_values) == len(columns):
            # Create a dictionary for easier access
            row_dict = dict(zip(columns, row_values))
            
            # Store operation based on type
            if row_dict['operation'] == 'SEND':
                send_operations.append(row_dict)
            elif row_dict['operation'] == 'SENT':
                sent_operations.append(row_dict)
        else:
            print(f"Warning: Line has incorrect field count: {line}")
    
    # Validate that each SEND has a corresponding SENT
    unmatched_sends = []
    matched_send_indices = set()
    
    for sent_op in sent_operations:
        # Find matching SEND operation
        found_match = False
        for i, send_op in enumerate(send_operations):
            if i in matched_send_indices:
                continue  # Skip already matched SENDs
                
            # Compare relevant fields to find a match
            if (send_op['instrumentId'] == sent_op['instrumentId'] and
                send_op['direction'] == sent_op['direction'] and
                send_op['size'] == sent_op['size'] and
                send_op['price'] == sent_op['price'] and
                send_op['orderType'] == sent_op['orderType'] and
                send_op['tif'] == sent_op['tif'] and
                abs(pd.to_datetime(send_op['timestamp']) - 
                    pd.to_datetime(sent_op['timestamp'])).total_seconds() < 1):  # Within 1 second
                
                found_match = True
                matched_send_indices.add(i)
                break
        
        if not found_match:
            print(f"Warning: Could not find matching SEND for SENT: {sent_op}")
    
    # Find unmatched SEND operations
    for i, send_op in enumerate(send_operations):
        if i not in matched_send_indices:
            unmatched_sends.append(send_op)
    
    # Create DataFrame from SENT operations (which we want to keep)
    sent_df = pd.DataFrame(sent_operations)
    
    # Convert timestamp to datetime without specifying format (pandas will auto-detect ISO format)
    sent_df['timestamp'] = pd.to_datetime(sent_df['timestamp'])
    
    # Convert numeric fields to appropriate types
    if 'size' in sent_df.columns:
        sent_df['size'] = pd.to_numeric(sent_df['size'], errors='coerce')
    if 'price' in sent_df.columns:
        sent_df['price'] = pd.to_numeric(sent_df['price'], errors='coerce')
    if 'requestId' in sent_df.columns:
        sent_df['requestId'] = pd.to_numeric(sent_df['requestId'], errors='coerce')
    if 'orderId' in sent_df.columns:
        sent_df['orderId'] = pd.to_numeric(sent_df['orderId'], errors='coerce')
    
    # Add a file date column to help track source file date
    file_date = extract_date_from_filename(log_file_path)
    sent_df['file_date'] = file_date
    
    return sent_df, unmatched_sends

def process_apex360_report(file_path, target_user='AD157949.EGERHAR'):
    """
    Process the Apex360Report CSV file, filtering for records where both
    the last user and entry user match the target user and Trade Date matches file date.
    
    Args:
        file_path: Path to the Apex360Report CSV file
        target_user: The user to filter for (default: AD157949.EGERHAR)
        
    Returns:
        DataFrame containing the filtered records
    """
    print(f"Processing Apex360 report: {file_path}")
    
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Get the file date first
        file_date = extract_date_from_filename(file_path)
        if file_date is None:
            print("Warning: Could not extract date from filename")
            return pd.DataFrame()
            
        # Convert Trade Date before filtering
        if 'Trade Date' in df.columns:
            df['Trade Date'] = pd.to_datetime(df['Trade Date'], format='%d-%b-%y', errors='coerce')
            
            # Filter for matching Trade Date (comparing only the date part)
            df = df[df['Trade Date'].dt.date == file_date.date()]
            print(f"Found {len(df)} records for trade date {file_date.date()}")
        
        # Filter for records where both Last User and Entry User match the target
        filtered_df = df[(df['Last User'] == target_user) & (df['Entry User'] == target_user)]
        print(f"Found {len(filtered_df)} records with Last User and Entry User = {target_user}")
        
        # Convert date columns to datetime
        date_columns = ['Start Date', 'EndDate']
        for col in date_columns:
            if col in filtered_df.columns:
                filtered_df[col] = pd.to_datetime(filtered_df[col], format='%d-%b-%y', errors='coerce')
        
        # Convert time columns to datetime
        time_columns = ['Entry Time', 'Last Time']
        for col in time_columns:
            if col in filtered_df.columns:
                # Keep original times as they are
                filtered_df[f'{col}_orig'] = filtered_df[col]
        
        # Add a file date column to help track source file date
        filtered_df['file_date'] = file_date
        
        # Add a source identifier
        filtered_df['source'] = 'apex360'
        
        return filtered_df
        
    except Exception as e:
        print(f"Error processing Apex360 report: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def find_latest_apex360_report(base_dir=r'\\aslfile01\FIS'):
    """
    Find the latest Apex360Report in the specified directory
    
    Args:
        base_dir: Base directory to search for reports
        
    Returns:
        Path to the latest report file or None if not found
    """
    try:
        # Pattern to match Apex360 report files
        pattern = os.path.join(base_dir, 'Apex360Report_Trades_EOD-*.csv')
        
        # Find all matching files
        files = glob.glob(pattern)
        
        if not files:
            print(f"No Apex360Report files found in {base_dir}")
            return None
        
        # Sort files by modification time (most recent first)
        latest_file = max(files, key=os.path.getmtime)
        
        print(f"Found latest Apex360Report: {latest_file}")
        return latest_file
        
    except Exception as e:
        print(f"Error finding Apex360Report: {e}")
        return None

def extract_date_from_filename(filename):
    """
    Extract the date from the filename
    
    Args:
        filename: Filename to extract date from
        
    Returns:
        datetime object or None if not found
    """
    # Try to find a date in the format YYYY-MM-DD in the filename
    date_pattern = r'(\d{4}-\d{2}-\d{2})'
    match = re.search(date_pattern, filename)
    
    if match:
        try:
            return pd.to_datetime(match.group(1))
        except:
            pass
    
    # Try to find a date in the format YYYYMMDD in the filename (for Apex360 reports)
    date_pattern2 = r'(\d{8})'
    match = re.search(date_pattern2, filename)
    
    if match:
        try:
            date_str = match.group(1)
            return pd.to_datetime(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}")
        except:
            pass
    
    # Default to file modification date if date not in filename
    try:
        mod_time = os.path.getmtime(filename)
        return datetime.fromtimestamp(mod_time).date()
    except:
        return None

def analyze_order_data(df):
    """
    Perform basic analysis on the order data to provide insights
    
    Args:
        df: DataFrame containing parsed log data
    
    Returns:
        Dictionary with analysis results
    """
    analysis = {}
    
    # Count orders by direction if column exists
    if 'direction' in df.columns:
        analysis['orders_by_direction'] = df['direction'].value_counts().to_dict()
    
    # Count orders by source
    if 'source' in df.columns:
        analysis['orders_by_source'] = df['source'].value_counts().to_dict()
    
    # Average order size if column exists
    if 'size' in df.columns:
        analysis['avg_order_size'] = df['size'].mean()
    elif 'Quantity' in df.columns:
        analysis['avg_quantity'] = df['Quantity'].mean()
    
    # Order type distribution if column exists
    if 'orderType' in df.columns:
        analysis['order_type_distribution'] = df['orderType'].value_counts().to_dict()
    elif 'Type' in df.columns:
        analysis['type_distribution'] = df['Type'].value_counts().to_dict()
    
    # Count of orders by trader if column exists
    if 'trader' in df.columns:
        analysis['orders_by_trader'] = df['trader'].value_counts().to_dict()
    
    return analysis

def analyze_apex_data(df):
    """
    Perform analysis specifically for Apex360 data
    
    Args:
        df: DataFrame containing Apex360 data
    
    Returns:
        Dictionary with analysis results
    """
    analysis = {}
    
    # Trade type distribution
    if 'Type' in df.columns:
        analysis['trade_type_distribution'] = df['Type'].value_counts().to_dict()
    
    # Average quantity
    if 'Quantity' in df.columns:
        analysis['avg_quantity'] = df['Quantity'].mean()
        analysis['total_quantity'] = df['Quantity'].sum()
    
    # Security type distribution
    if 'Security Type' in df.columns:
        analysis['security_type_distribution'] = df['Security Type'].value_counts().to_dict()
    
    # Trade date distribution
    if 'Trade Date' in df.columns:
        analysis['trades_by_date'] = df['Trade Date'].value_counts().sort_index().to_dict()
    
    # User activity
    if 'Last User' in df.columns:
        analysis['activity_by_user'] = df['Last User'].value_counts().to_dict()
    
    # Market analysis
    if 'Market' in df.columns:
        analysis['trades_by_market'] = df['Market'].value_counts().to_dict()
    
    return analysis

def write_errors_to_file(unmatched_sends, log_file_path):
    """
    Write unmatched SEND operations to a CSV file for review
    
    Args:
        unmatched_sends: List of dictionaries containing unmatched SEND operations
        log_file_path: Original log file path (to include in error filename)
    """
    if unmatched_sends:
        # Extract a date or identifier from the log filename
        file_id = os.path.basename(log_file_path).split('.')[0]
        error_filename = f"unmatched_sends_{file_id}.csv"
        
        df = pd.DataFrame(unmatched_sends)
        df.to_csv(error_filename, index=False)
        print(f"Wrote {len(unmatched_sends)} unmatched SEND operations to {error_filename}")
    else:
        print("No unmatched SEND operations found.")

def append_to_parquet(new_df, parquet_path='order_data.parquet'):
    """
    Append new data to an existing Parquet file or create a new one,
    checking if data from this file date has already been processed
    
    Args:
        new_df: DataFrame containing new data to append
        parquet_path: Path to the Parquet file
        
    Returns:
        Combined DataFrame
    """
    if os.path.exists(parquet_path):
        try:
            # Read existing data
            existing_df = pd.read_parquet(parquet_path)
            print(f"Read {len(existing_df)} existing records from {parquet_path}")
            
            # Check if this file date has already been processed
            if 'file_date' in new_df.columns and 'file_date' in existing_df.columns:
                new_dates = new_df['file_date'].unique()
                existing_dates = existing_df['file_date'].unique()
                
                if all(date in existing_dates for date in new_dates):
                    print(f"Data from {new_dates} already exists in the parquet file. Skipping.")
                    return existing_df
                    
                print(f"Processing new data for dates: {[d for d in new_dates if d not in existing_dates]}")
            
            # Check if we have columns for duplicate detection
            if ('orderId' in new_df.columns and 'orderId' in existing_df.columns and 
                'instrumentId' in new_df.columns and 'instrumentId' in existing_df.columns and
                'timestamp' in new_df.columns and 'timestamp' in existing_df.columns):
                # Create a combined key for duplicate detection
                new_df['_temp_key'] = new_df['timestamp'].astype(str) + '_' + new_df['instrumentId'] + '_' + new_df['orderId'].astype(str)
                existing_df['_temp_key'] = existing_df['timestamp'].astype(str) + '_' + existing_df['instrumentId'] + '_' + existing_df['orderId'].astype(str)
                
                # Find records in new_df that don't exist in existing_df
                new_keys = set(new_df['_temp_key']) - set(existing_df['_temp_key'])
                unique_new_df = new_df[new_df['_temp_key'].isin(new_keys)]
                
                # Drop temporary key
                unique_new_df = unique_new_df.drop(columns=['_temp_key'])
                existing_df = existing_df.drop(columns=['_temp_key'])
                
                print(f"Found {len(new_df) - len(unique_new_df)} potential duplicates that will be skipped")
                
                # Combine data
                combined_df = pd.concat([existing_df, unique_new_df], ignore_index=True)
            
            # For Apex360 data, use Trade No for duplicate detection
            elif ('Trade No' in new_df.columns and 'Trade No' in existing_df.columns):
                # Create a combined key for duplicate detection
                new_df['_temp_key'] = new_df['Trade No'].astype(str)
                existing_df['_temp_key'] = existing_df['Trade No'].astype(str)
                
                # Find records in new_df that don't exist in existing_df
                new_keys = set(new_df['_temp_key']) - set(existing_df['_temp_key'])
                unique_new_df = new_df[new_df['_temp_key'].isin(new_keys)]
                
                # Drop temporary key
                unique_new_df = unique_new_df.drop(columns=['_temp_key'])
                existing_df = existing_df.drop(columns=['_temp_key'])
                
                print(f"Found {len(new_df) - len(unique_new_df)} potential duplicates that will be skipped")
                
                # Combine data
                combined_df = pd.concat([existing_df, unique_new_df], ignore_index=True)
            else:
                # If we don't have columns for duplicate detection, just append all rows
                # This might result in duplicates
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
            print(f"Combined dataset now has {len(combined_df)} records")
            return combined_df
            
        except Exception as e:
            print(f"Error reading existing Parquet file: {e}")
            print("Creating a new file with just the current data")
            return new_df
    else:
        print(f"Parquet file {parquet_path} does not exist. Creating a new file.")
        return new_df

def main():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # Move up one level from LOG_ANALYSIS
    
    # Define logs directory relative to parent directory
    logs_dir = os.path.join(parent_dir, 'RUN_OrderManagement', 'LOGS')
    
    # Ensure logs directory exists
    if not os.path.exists(logs_dir):
        print(f"Logs directory {logs_dir} does not exist. Exiting.")
        return
    
    # Get todays log file or the one with the most current date or use default
    latest_log_date = datetime.now().strftime('%Y-%m-%d')
    log_file_path = os.path.join(logs_dir, f'orderManagement_{latest_log_date}_machine.log.0')
    if not os.path.exists(log_file_path):
        print(f"Log file {log_file_path} does not exist. finding latest log file.")
        
    # Find the latest log file in the logs directory
    log_files = [f for f in os.listdir(logs_dir) if re.match(r'orderManagement_\d{4}-\d{2}-\d{2}_machine\.log\.0', f)]
    if log_files:
        log_file_path = os.path.join(logs_dir, max(log_files, key=lambda x: os.path.getctime(os.path.join(logs_dir, x))))        
        print(f"Using latest log file: {log_file_path}")
    else:
        print("No log files found. Exiting.")
        return
    
    # Set output parquet files
    order_parquet_path = os.path.join(script_dir, 'order_data.parquet')
    apex_parquet_path = os.path.join(script_dir, 'apex_data.parquet')
    
    # Parse order management log file
    print(f"\nParsing order management log file: {log_file_path}...")
    sent_df, unmatched_sends = parse_order_management_log(log_file_path)
    
    # Display basic information about the parsed data
    print(f"\nParsed {len(sent_df)} SENT operations.")
    print(f"Found {len(unmatched_sends)} unmatched SEND operations.")
    
    if len(sent_df) > 0:
        print(f"Columns: {sent_df.columns.tolist()}")
        print("\nSample data:")
        print(sent_df.head())
        
        # Write unmatched SEND operations to a file for review
        write_errors_to_file(unmatched_sends, log_file_path)
        
        # Analyze order data
        print("\nAnalyzing order data...")
        order_analysis = analyze_order_data(sent_df)
        print("\nOrder analysis results:")
        for key, value in order_analysis.items():
            print(f"{key}: {value}")
        
        # Append to existing Parquet file or create new one
        combined_order_df = append_to_parquet(sent_df, order_parquet_path)
        
        # Save to Parquet file
        try:
            combined_order_df.to_parquet(order_parquet_path, index=False)
            print(f"\nOrder data successfully saved to {order_parquet_path}")
            
            # Check file size
            file_size_mb = os.path.getsize(order_parquet_path) / (1024 * 1024)
            print(f"Parquet file size: {file_size_mb:.2f} MB")
        except Exception as e:
            print(f"Error saving to Parquet: {e}")
            
            # Fallback to CSV if there's an issue with Parquet
            print("Falling back to CSV format...")
            csv_path = os.path.join(script_dir, 'order_data.csv')
            combined_order_df.to_csv(csv_path, index=False)
            print(f"Data saved to {csv_path} instead")
    else:
        print("No valid SENT operations found in the log file.")
    
    # Process Apex360 report
    apex_file = find_latest_apex360_report()
    if apex_file:
        apex_df = process_apex360_report(apex_file)
        
        if len(apex_df) > 0:
            print(f"\nProcessed {len(apex_df)} records from Apex360 report.")
            print(f"Columns: {apex_df.columns.tolist()}")
            print("\nSample data:")
            print(apex_df.head())
            
            # Analyze Apex data using the specialized method
            print("\nAnalyzing Apex data...")
            apex_analysis = analyze_apex_data(apex_df)
            print("\nApex analysis results:")
            for key, value in apex_analysis.items():
                print(f"{key}: {value}")
            
            # Append to existing Parquet file or create new one
            combined_apex_df = append_to_parquet(apex_df, apex_parquet_path)
            
            # Save to Parquet file
            try:
                combined_apex_df.to_parquet(apex_parquet_path, index=False)
                print(f"\nApex data successfully saved to {apex_parquet_path}")
                
                # Check file size
                file_size_mb = os.path.getsize(apex_parquet_path) / (1024 * 1024)
                print(f"Parquet file size: {file_size_mb:.2f} MB")
            except Exception as e:
                print(f"Error saving to Parquet: {e}")
                
                # Fallback to CSV if there's an issue with Parquet
                print("Falling back to CSV format...")
                csv_path = os.path.join(script_dir, 'apex_data.csv')
                combined_apex_df.to_csv(csv_path, index=False)
                print(f"Data saved to {csv_path} instead")
        else:
            print("No matching records found in Apex360 report.")
    
    # Print summary of data in the Parquet files
    print("\nSummary of order data in Parquet file:")
    try:
        all_order_data = pd.read_parquet(order_parquet_path)
        print(f"Total order records: {len(all_order_data)}")
        
        # Show date range if file_date column exists
        if 'file_date' in all_order_data.columns:
            print(f"Date range: {all_order_data['file_date'].min()} to {all_order_data['file_date'].max()}")
        
        # Show timestamp range
        print(f"Timestamp range: {all_order_data['timestamp'].min()} to {all_order_data['timestamp'].max()}")
        
        # Count of records by trader
        if 'trader' in all_order_data.columns:
            trader_counts = all_order_data['trader'].value_counts()
            print("\nRecords by trader:")
            print(trader_counts)
        
    except Exception as e:
        print(f"Error reading back order Parquet file for summary: {e}")
    
    print("\nSummary of Apex data in Parquet file:")
    try:
        if os.path.exists(apex_parquet_path):
            all_apex_data = pd.read_parquet(apex_parquet_path)
            print(f"Total Apex records: {len(all_apex_data)}")
            
            # Show date range if file_date column exists
            if 'file_date' in all_apex_data.columns:
                print(f"Date range: {all_apex_data['file_date'].min()} to {all_apex_data['file_date'].max()}")
            
            # Show Trade Date range
            if 'Trade Date' in all_apex_data.columns:
                print(f"Trade Date range: {all_apex_data['Trade Date'].min()} to {all_apex_data['Trade Date'].max()}")
            
            # Count of records by Type
            if 'Type' in all_apex_data.columns:
                type_counts = all_apex_data['Type'].value_counts()
                print("\nRecords by Type:")
                print(type_counts)
        else:
            print("No Apex data file exists yet.")
        
    except Exception as e:
        print(f"Error reading back Apex Parquet file for summary: {e}")

if __name__ == "__main__":
    main()