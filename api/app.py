from flask import Flask, jsonify
import json

# Initiate a flask instance
app = Flask(__name__)

# Define the root endpoint
@app.route('/', methods=['GET'])
def index():
    return '''
        Hi, welcome to the transaction API\n
         -------------- 
        Here are the available endpoints:\n   
         /api/transactions,  /api/customers, /api/externalData
    '''

# Route for the transaction
@app.route('/api/transactions', methods=['GET'])
def transaction():
    with open('data/transactions.json', 'r') as f:
        data = json.load(f)
    return data

# Route for the customers
@app.route('/api/customers', methods=['GET'])
def customers():
    with open('data/customers.json', 'r') as f:
        data = json.load(f)
    return data

# Route for the externalData
@app.route('/api/externalData', methods=['GET'])
def externalData():
    with open('data/external_data.json', 'r') as f:
        data = json.load(f)
    return data

# Run the api
if __name__ == '__main__':
    app.run(
        host='localhost',
        port=8000,
        debug=True
    )