# Import necessary packages
from pyhive import hive

# Hive connection parameters
hive_host = 'localhost'
hive_port = 10000
hive_user = 'hive'
hive_password = 'hive'
hive_database = 'fraude_detection'

# connect to hive
def get_connection():
    try:
        # Create a connection
        connection = hive.Connection(
            host=hive_host,
            port=hive_port,
            username=hive_user,
            password=hive_password,
            database=hive_database,
            auth='CUSTOM',  # Use 'CUSTOM' for non-Kerberos authentication
            configuration={'hive.server2.transport.mode': 'http'}
        )
        return connection
    except Exception as e:
        print(f"Error connecting to Hive: {str(e)}")
        return None

def create_tables():
    queries = [
        '''
        CREATE TABLE IF NOT EXISTS transactions (
            amount FLOAT,
            currency STRING,
            customer_id STRING,
            date_time TIMESTAMP,
            location STRING,
            merchant_details STRING,
            transaction_id STRING,
            transaction_type STRING
        )
        STORED AS PARQUET
        LOCATION 'hdfs://namenode:8020/user/hive/warehouse/fraude_detection.db/transactions'
        ''',
        '''
        CREATE TABLE IF NOT EXISTS customers (
            account_history ARRAY<STRING>,
            avg_transaction_value FLOAT,
            customer_id STRING,
            demographics_age INT,
            demographics_locations STRING
        )
        STORED AS PARQUET
        LOCATION 'hdfs://namenode:8020/user/hive/warehouse/fraude_detection.db/customers'
        ''',
        '''
        CREATE TABLE IF NOT EXISTS blacklist_info (
            id INT,
            merchand STRING
        )
        STORED AS PARQUET
        LOCATION 'hdfs://namenode:8020/user/hive/warehouse/fraude_detection.db/blacklist_info'
        ''',
        '''
        CREATE TABLE IF NOT EXISTS credit_fraud (
            customer_id STRING,
            credit_score INT,
            fraud_reports INT
        )
        STORED AS PARQUET
        LOCATION 'hdfs://namenode:8020/user/hive/warehouse/fraude_detection.db/credit_fraud'
        '''
    ]

    try:
        # Connect to Hive
        connection = get_connection()

        if not connection:
            return None
        else:
            # Create a cursor
            cursor = connection.cursor()

            # Execute each CREATE TABLE query
            for query in queries:
                cursor.execute(query)

            # Commit the transaction
            connection.commit()

            # Execute a query to show tables (just for verification)
            cursor.execute('SHOW TABLES')

            # Fetch and return the result
            return cursor.fetchall()

    except Exception as e:
        return f'Error: {str(e)}'