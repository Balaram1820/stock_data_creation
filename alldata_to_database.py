import os
import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017')
# Insert BSE data into MongoDB
bse_db = client['bse_listed_stocks']
def insert_bse_csv_data():
    # List all CSV files in a directory
    csv_files = [f for f in os.listdir('C:/Users/balar/OneDrive/Desktop/final stock module/BSE_STOCKS') if f.endswith('.csv')]

    # Loop over each CSV file and insert its data into the database
    for file in csv_files:
        collection_name = os.path.splitext(file)[0]
        collection = bse_db[collection_name]
        try:
            data = pd.read_csv(f'C:/Users/balar/OneDrive/Desktop/final stock module/BSE_STOCKS/{file}')
            records = data.to_dict(orient='records')
            collection.insert_many(records)
            print(f'{len(records)} records inserted into {collection_name} collection')
        except TypeError:
            print(f'Error inserting {file}: Skipping to next file.')
            continue

    print("BSE data insertion complete")

# Insert NSE data into MongoDB
nse_db = client['nse_listed_stocks']
def insert_nse_csv_data():
    # List all CSV files in a directory
    csv_files = [f for f in os.listdir('C:/Users/balar/OneDrive/Desktop/STOCKS_DATAGENERATION/NSE_STOCKS') if f.endswith('.csv')]

    # Loop over each CSV file and insert its data into the database
    for file in csv_files:
        collection_name = os.path.splitext(file)[0]
        collection = nse_db[collection_name]
        try:
            data = pd.read_csv(f'C:/Users/balar/OneDrive/Desktop/STOCKS_DATAGENERATION/NSE_STOCKS/{file}')
            records = data.to_dict(orient='records')
            collection.insert_many(records)
            print(f'{len(records)} records inserted into {collection_name} collection')
        except TypeError:
            print(f'Error inserting {file}: Skipping to next file.')
            continue

    print("NSE data insertion complete")

# Call the functions to run the script
insert_bse_csv_data()
insert_nse_csv_data()

# Close the MongoDB connection
client.close()
