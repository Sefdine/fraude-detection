# Import necessary packages
from datetime import datetime
import extract
from database import get_connection

# Define a connection to the database
connection = get_connection()

# Transform the transaction data
def load_transaction():
    transactions_data = extract.get_transactions()

    try:
        # Create a cursor
        cursor = connection.cursor()

        # Define the SQL query for inserting a transaction
        insert_query = '''
            INSERT INTO transactions (
                amount,
                currency,
                customer_id,
                date_time,
                location,
                merchant_details,
                transaction_id,
                transaction_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''

        # Insert each transaction into the transactions table
        count = 1
        for transaction in transactions_data:
            # Format the date string to a datetime object
            transaction['date_time'] = datetime.strptime(transaction['date_time'], '%Y-%m-%dT%H:%M:%S')

            # Execute the insert query with transaction data
            cursor.execute(insert_query, (
                transaction['amount'],
                transaction['currency'],
                transaction['customer_id'],
                transaction['date_time'],
                transaction['location'],
                transaction['merchant_details'],
                transaction['transaction_id'],
                transaction['transaction_type']
            ))

            print(f"Transaction n°{count} inserted successfully", end='\r')
            count += 1

        # Commit the transaction
        connection.commit()
        print("Transactions inserted successfully!")

    except Exception as e:
        print(f"Error inserting transactions: {str(e)}")

# Call the function to load transactions into the transactions table
load_transaction()
