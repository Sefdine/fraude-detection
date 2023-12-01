# Import necessary packages
from datetime import datetime
import sys
sys.path.append('..')
from app import extract
from app.database import get_connection

# Define a connection to the database
connection = get_connection()

# Create a cursor
cursor = connection.cursor()

# Load external data
def load_external_data():
    # Get the external data
    external_data = extract.get_external_data()

    # Extract blacklist info
    blacklist_infos = external_data['blacklist_info']
    # insert into blacklist
    insert_blacklist_query = '''
        INSERT INTO blacklist_info (id, merchand) VALUES (%s, %s)
    '''
    blacklist_id = 1
    for blacklist_info in blacklist_infos:
        try:
            cursor.execute(insert_blacklist_query, (blacklist_id, blacklist_info))
            print (f"{blacklist_info} inserted successfully", end="\r")
            blacklist_id += 1
        except Exception as e:
            print('Error inserting: ',str(e))

    print('----- END blacklist -----')

    # Get the credit fraud information
    credit_scores = external_data['credit_scores']
    fraud_reports = external_data['fraud_reports']

    # Merge dictionaries
    merged_data = {key: (credit_scores[key], fraud_reports[key]) for key in credit_scores}
    # insert into credit_fraud
    insert_credit_fraud_query = '''
        INSERT INTO credit_fraud(customer_id, credit_score, fraud_reports) VALUES(%s, %s, %s)
    '''
    for customer_id, values in merged_data.items():
        try:
            cursor.execute(insert_credit_fraud_query, (
                customer_id,
                values[0], # Credit score
                values[1] # Fraud report
            ))
            print(f"Customer {customer_id} inserted into credit_fraud", end="\r")
        except Exception as e:
            print('Error inserting ',str(e))
    print('Proccess completed -----------------------')

load_external_data()