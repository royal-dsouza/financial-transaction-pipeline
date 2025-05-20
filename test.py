import json

with open('transactions.avsc') as f:
    json.load(f)  # Will raise error if it's invalid JSON
