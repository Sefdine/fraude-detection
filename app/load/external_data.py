# Import necessary packages
from datetime import datetime
import sys
sys.path.append('..')
from app import extract
from app.database import get_connection
import pandas as pd

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
    blacklist_info_df = pd.DataFrame(blacklist_infos)
    blacklist_info_df.to_csv('data/blacklist_info.csv', index=False, header=False)
    insert_blacklist_query = "LOAD DATA LOCAL INPATH '/tables_data/blacklist_info.csv' OVERWRITE INTO TABLE blacklist_info"

    # insert into credit score
    credit_scores = external_data['credit_scores']
    with open('data/credit_scores.csv', 'w') as f:
        for key, value in credit_scores.items():
            f.write(f"{key}, {value}\n")
    insert_credit_scores_query = "LOAD DATA LOCAL INPATH '/tables_data/credit_scores.csv' OVERWRITE INTO TABLE credit_scores"

    # insert into fraud reports
    fraud_reports = external_data['fraud_reports']
    with open('data/fraud_reports.csv', 'w') as f:
        for key, value in fraud_reports.items():
            f.write(f"{key}, {value}\n")
    insert_fraud_reports_query = "LOAD DATA LOCAL INPATH '/tables_data/fraud_reports.csv' OVERWRITE INTO TABLE fraud_reports"

    try:
        # Black list
        cursor.execute(insert_blacklist_query)
        print('Blacklist_info inserted successfully')
        
        # Credit score
        cursor.execute(insert_credit_scores_query)
        print('Credit scores inserted successfully')

        # fraud reports
        cursor.execute(insert_fraud_reports_query)
        print('Fraud reports inserted successfully')
    except Exception as e:
        print('Error inserting: ',str(e))

load_external_data()