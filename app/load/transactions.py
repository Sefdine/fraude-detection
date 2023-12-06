# Import necessary packages
from datetime import datetime
import sys
sys.path.append('..')
from app import extract
from app.database import get_connection

# Transform the transaction data
def detect_fraud(transaction, account_historys, blacklists, fraud_reports):

    # Insert each transaction into the transactions table
    transaction['date_time'] = datetime.strptime(transaction['date_time'], '%Y-%m-%dT%H:%M:%S')
    if transaction['date_time'] > datetime.now():
        return True

    elif transaction['transaction_id'] in account_historys:
        return True

    elif transaction['merchant_details'] in blacklists and transaction['customer_id'] in fraud_reports:
        return True
    
    else:
        return False

# Insert a new transaction
def load_transaction(transaction):
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
    
    try:
        print(f"Loading transactions {transaction['transaction_id']}")
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

        print(f"Transaction {transaction['transaction_id']} inserted successfully")
        return True
    except Exception as e:
        print('Error ',str(e))
        return False

# Update the customer table
def update_customer(customer_id, transaction_id, new_amount):

    try:
        print(f"Updating customer {customer_id}")
        query = "SELECT account_history, behavioral_patterns FROM customers WHERE customer_id = %s"

        cursor.execute(query, (customer_id,))
        customer_data = cursor.fetchall()

        account_history = eval(customer_data[0][0])
        average_amount = eval(customer_data[0][1])['avg_transaction_value']

        # Updated values
        account_history_updated = account_history + [transaction_id]
        sum_amount = average_amount * len(account_history)
        sum_amount += new_amount
        new_average_amount = sum_amount / len(account_history_updated) 

        create_temptable = '''
            CREATE TABLE temp_updated_customers AS
            SELECT
                customer_id,
                %s AS account_history,
                %s AS behavioral_patterns,
                demographics
            FROM
                customers
            WHERE
                customer_id = %s
        '''
        cursor.execute(create_temptable, (
            str(account_history_updated),
            str({'avg_transaction_value': new_average_amount}),
            customer_id
        ))

        insert_query = '''
            INSERT OVERWRITE TABLE customers
            SELECT * FROM temp_updated_customers
        '''
        cursor.execute(insert_query)

        drop_temptable = f'''
            DROP TABLE temp_updated_customers
        '''
        cursor.execute(drop_temptable)

        print(f"Customer {customer_id} updated successfully")
    except Exception as e:
        print('Error ',str(e))

# Update the fraud_report table
def update_fraud_report(customer_id):
    try:
        print('Updating fraud reports...')
        query = "SELECT fraud_report FROM fraud_reports WHERE customer_id = %s"

        cursor.execute(query, (customer_id,))
        fraud_report = eval(cursor.fetchall()[0][0])
        print(f"Customer {customer_id} currently have {fraud_report} fraud reports")

        create_temptable = '''
            CREATE TABLE temp_updated_fraud_reports AS
            SELECT
                customer_id,
                %s AS fraud_report
            FROM
                fraud_reports
            WHERE
                customer_id = %s
        '''
        cursor.execute(create_temptable, (str(fraud_report+1), customer_id))

        insert_query = '''
            INSERT OVERWRITE TABLE fraud_reports
            SELECT * FROM temp_updated_fraud_reports
        '''
        cursor.execute(insert_query)

        drop_temptable = f'''
            DROP TABLE temp_updated_fraud_reports
        '''
        cursor.execute(drop_temptable)

        print(f"Fraud report updated for customer {customer_id} updated to {fraud_report+1}")
    except Exception as e:
        print('Error ',str(e))



if __name__ == '__main__':

    try:

        # Define a connection to the database
        connection = get_connection()

        # Create a cursor
        cursor = connection.cursor()

        transactions_data = extract.get_transactions()

        # Get all account histories
        account_historys = []
        query = "SELECT account_history FROM customers"
        cursor.execute(query)
        account_history = cursor.fetchall()

        for x in account_history:
            account_historys.extend([y for y in eval(x[0])])

        account_historys = set(account_historys)

        # Get blacklist info
        query = "SELECT * FROM blacklist_info"
        cursor.execute(query)
        response = cursor.fetchall()
        blacklists = [x[0] for x in response]

        # Get the customer_id with fraud report > 0
        query = "SELECT customer_id FROM fraud_reports WHERE fraud_report > 0"
        cursor.execute(query)
        response = cursor.fetchall()
        fraud_reports = [x[0] for x in response]

        for transaction in transactions_data:
            is_fraude = detect_fraud(transaction, account_history, blacklists, fraud_reports)
            customer_id = transaction['customer_id']

            if is_fraude:
                update_fraud_report(customer_id)
            else:
                transaction_loaded = load_transaction(transaction)
                if transaction_loaded:
                    update_customer(customer_id, transaction['transaction_id'], transaction['amount'])
            break


    except Exception as e:
        print(f"Error inserting transactions: {str(e)}")