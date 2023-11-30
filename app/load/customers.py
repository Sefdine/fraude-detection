# Import necessary packages
from datetime import datetime
import sys
sys.path.append('..')
from app import extract
from app.database import get_connection
import numpy as np

# Define a connection to the database
connection = get_connection()

# Transform the customer data
def load_customers():
    customers_data = extract.get_customers()

    try:
        # Create a cursor
        cursor = connection.cursor()

        # Insert each customer into the customers table
        count = 1
        for customer in customers_data:

            # Define the Hive INSERT INTO statement for a Parquet table
            insert_query = f'''
                INSERT INTO TABLE customers
                SELECT 
                    array('{','.join(customer['account_history'])}'),
                    {customer['behavioral_patterns']['avg_transaction_value']},
                    '{customer['customer_id']}',
                    {customer['demographics']['age']},
                    '{customer['demographics']['location']}'
            '''

            # Execute the INSERT INTO statement
            cursor.execute(insert_query)

            print(f"Customer {customer['customer_id']} inserted successfully", end='\r')
            count += 1

        # Commit the transaction
        connection.commit()
        print("Customers inserted successfully!")

    except Exception as e:
        print(f"Error inserting customers: {str(e)}")

# Call the function to load customers into the customers table
load_customers()
