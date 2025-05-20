"""
MySQL to Bigtable Data Transfer Script

This script extracts transaction data from MySQL and loads it into Bigtable.
It uses Secret Manager for secure credential handling.
"""

import json
import time
import uuid
import os
import logging
from datetime import datetime, timedelta
import subprocess
import time

import mysql.connector
from google.cloud import secretmanager
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/royaldsouza/Downloads/my_gcp_project.json"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Google Cloud project settings
PROJECT_ID = "elevated-column-458305-f8"
BIGTABLE_INSTANCE_ID = "fin-bt"
BIGTABLE_TABLE_ID = "txn_tbl"
MYSQL_SECRET_ID = "mysql-credentials"

def get_mysql_credentials():
    """Retrieve MySQL credentials from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{PROJECT_ID}/secrets/{MYSQL_SECRET_ID}/versions/latest"
    response = client.access_secret_version(name=secret_name)
    cred = json.loads(response.payload.data.decode('UTF-8'))
    return cred

def start_cloud_sql_proxy(cred):
    proxy_process = subprocess.Popen([
        'cloud-sql-proxy',
        f'--credentials-file={os.getenv("GOOGLE_APPLICATION_CREDENTIALS")}',
        f'--port={cred["port"]}',
        cred['instance_connection_name']
    ])
    time.sleep(5)  # Give the proxy time to establish the connection
    return proxy_process

def connect_to_mysql(credentials):
    """Establish a connection to MySQL using the provided credentials."""
    try:
        connection = mysql.connector.connect(
            host=credentials['host'],
            port=credentials['port'],
            user=credentials['username'],
            password=credentials['password'],
            database=credentials['database']
        )
        print("MySQL connection established.")
        return connection
    except mysql.connector.Error as err:
        logger.error(f"Error connecting to MySQL: {err}")
        raise

def get_transactions(connection, last_sync_time=None):
    """
    Query the MySQL database for transactions.
    If last_sync_time is provided, only fetch transactions updated since then.
    """
    cursor = connection.cursor(dictionary=True)
    
    if last_sync_time:
        query = """
        SELECT * FROM transactions 
        WHERE updated_at >= %s
        ORDER BY updated_at ASC
        """
        cursor.execute(query, (last_sync_time,))
    else:
        # Initial load - get all transactions
        query = "SELECT * FROM transactions ORDER BY transaction_time ASC"
        cursor.execute(query)
    
    transactions = cursor.fetchall()
    cursor.close()
    
    return transactions

def write_to_bigtable(transactions):
    """Write transaction data to Bigtable."""
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(BIGTABLE_INSTANCE_ID)
    table = instance.table(BIGTABLE_TABLE_ID)
    
    timestamp = datetime.utcnow() 
    
    # Number of transactions to process in a batch
    batch_size = 100
    total_transactions = len(transactions)
    
    for i in range(0, total_transactions, batch_size):
        batch = transactions[i:i+batch_size]
        rows = []
        
        for transaction in batch:
            # Use transaction_id as the row key
            row_key = f"{transaction['transaction_id']}".encode()
            row = table.direct_row(row_key)
            
            # Store the current data in cf_latest column family
            for key, value in transaction.items():
                if value is not None:
                    # Convert non-string values to string for storage
                    if isinstance(value, (datetime, timedelta)):
                        value = value.isoformat()
                    elif not isinstance(value, str):
                        value = str(value)
                    
                    column = f"data:{key}".encode()
                    row.set_cell(
                        "cf_latest",
                        column,
                        value.encode(),
                        timestamp=timestamp
                    )
            
            # Also store in history for tracking changes over time
            # This creates a timestamped version in the cf_history family
            for key, value in transaction.items():
                if value is not None:
                    if isinstance(value, (datetime, timedelta)):
                        value = value.isoformat()
                    elif not isinstance(value, str):
                        value = str(value)
                    
                    column = f"data:{key}".encode()
                    row.set_cell(
                        "cf_history",
                        column,
                        value.encode(),
                        timestamp=timestamp
                    )
            
            rows.append(row)
        
        # Send the batch to Bigtable
        table.mutate_rows(rows)
        logger.info(f"Processed batch {i//batch_size + 1}, {min(i+batch_size, total_transactions)}/{total_transactions} transactions")
    
    return total_transactions

def get_last_sync_time():
    """
    Get the last sync time from a file.
    """
    # For simplicity, we'll just use a file
    try:
        with open('last_sync_time.txt', 'r') as f:
            time_str = f.read().strip()
            if time_str:
                return datetime.fromisoformat(time_str)
    except (FileNotFoundError, ValueError):
        pass
    
    # Default to 24 hours ago if no sync time is found
    return datetime.now() - timedelta(days=1)

def save_last_sync_time(sync_time):
    """Save the current sync time for the next run."""
    with open('last_sync_time.txt', 'w') as f:
        f.write(sync_time.isoformat())

def main():
    """Main function to orchestrate the ETL process."""
    try:
        # Get MySQL credentials from Secret Manager
        credentials = get_mysql_credentials()

        # Start Cloud SQL Proxy
        proxy = start_cloud_sql_proxy(credentials)
        
        # Get the last sync time
        last_sync_time = get_last_sync_time()
        logger.info(f"Fetching transactions updated since: {last_sync_time}")

        # Connect to MySQL
        mysql_conn = connect_to_mysql(credentials)
        
        # Extract transactions from MySQL
        transactions = get_transactions(mysql_conn, last_sync_time)
        logger.info(f"Extracted {len(transactions)} transactions from MySQL")
        
        # Load the transactions into Bigtable
        if transactions:
            processed_count = write_to_bigtable(transactions)
            logger.info(f"Successfully loaded {processed_count} transactions into Bigtable")
            
            # Update the last sync time to now
            save_last_sync_time(datetime.now())
        else:
            logger.info("No new transactions to process")
        
        # Close MySQL connection
        mysql_conn.close()
        proxy.terminate()  # Stop the Cloud SQL Proxy
        logger.info("ETL process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        if 'mysql_conn' in locals():
            mysql_conn.close()
        if 'proxy' in locals():
            proxy.terminate()
        raise

if __name__ == "__main__":
    main()