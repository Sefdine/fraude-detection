import requests
import json

# Fetch tranctions data
def get_transactions():
    try:
        response = requests.get('http://localhost:8000/api/transactions')
        return response.json()
    except:
        return []
    
# Fetch customers data
def get_customers():
    try:
        response = requests.get('http://localhost:8000/api/customers')
        return response.json()
    except:
        return []
    
# Fetch external data
def get_external_data():
    try:
        response = requests.get('http://localhost:8000/api/externalData')
        return response.json()
    except:
        return []