import requests
from datetime import datetime
import random
import string

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": "Basic bGlrZTpsaWtl"
}

# Generate a random string for dag_run_id
dag_run_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))

body = {
    "conf": {},
    "dag_run_id": dag_run_id,
    "note": "test postman"
}

url = "http://localhost:8080/api/v1/dags/hive-loader/dagRuns"

requests.post(url, headers=headers, json=body)
