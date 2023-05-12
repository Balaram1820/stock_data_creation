import os
import pandas as pd
from pymongo import MongoClient
import time

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017')
db_nse = client['nse_listed_stocks']
db_bse = client['bse_listed_stocks']

while True:
    # List all CSV files in NSE_STOCKS directory
    nse_csv_files = [f for f in os.listdir(
        'C:/Users/balar/OneDrive/Desktop/STOCKS_DATAGENERATION/NSE_STOCKS') if f.endswith('.csv')]

    # Loop over each NSE CSV file and insert its latest record into the database
    for file in nse_csv_files:
        collection_name = os.path.splitext(file)[0]
        collection = db_nse[collection_name]
        try:
            data = pd.read_csv(
                f'/Users/balar/OneDrive/Desktop/STOCKS_DATAGENERATION/NSE_STOCKS/{file}')
            last_record = data.iloc[-1].to_dict()
        except IndexError:
            print(f'Error reading {file}: Skipping to next file.')
            continue
        existing_record = collection.find_one({'Date': last_record['Date']})
        if existing_record is None:
            collection.insert_one(last_record)
            print(
                f'Record with Date {last_record["Date"]} inserted into {collection_name} collection in NSE.')
        else:
            print(
                f'Record with Date {last_record["Date"]} already exists in {collection_name} collection in NSE.')

    # List all CSV files in BSE_STOCKS directory
    bse_csv_files = [f for f in os.listdir(
        'C:/Users/balar/OneDrive/Desktop/final stock module/BSE_STOCKS') if f.endswith('.csv')]

    # Loop over each BSE CSV file and insert its latest record into the database
    for file in bse_csv_files:
        collection_name = os.path.splitext(file)[0]
        collection = db_bse[collection_name]
        try:
            data = pd.read_csv(
                f'/Users/balar/OneDrive/Desktop/final stock module/BSE_STOCKS/{file}')
            last_record = data.iloc[-1].to_dict()
        except IndexError:
            print(f'Error reading {file}: Skipping to next file.')
            continue
        existing_record = collection.find_one({'Date': last_record['Date']})
        if existing_record is None:
            collection.insert_one(last_record)
            print(
                f'Record with Date {last_record["Date"]} inserted into {collection_name} collection in BSE.')
        else:
            print(
                f'Record with Date {last_record["Date"]} already exists in {collection_name} collection in BSE.')

    # Pause the execution for 1 second before running the loop again
    time.sleep(1)
