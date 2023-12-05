# Import necessary packages
from datetime import datetime
import sys
sys.path.append('..')
from app import extract
from app.database import get_connection
import pandas as pd
import sys

# Define a connection to the database
connection = get_connection()

# Transform the customer data
def load_customers():
    customers_data = extract.get_customers()

    customers_df = pd.DataFrame(customers_data)
    customers_df.to_csv('data/customers.csv')

    try:
        # Create a cursor
        cursor = connection.cursor()

        query = "LOAD DATA LOCAL INPATH '/tables_data/customers.csv' OVERWRITE INTO TABLE customers"

        cursor.execute(query)

        # Commit the transaction
        connection.commit()
        print("Customers inserted successfully!")

    except Exception as e:
        print(f"Error inserting customers: {str(e)}")

# Call the function to load customers into the customers table
load_customers()

