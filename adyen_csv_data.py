import os
import pandas as pd
import requests
#import datetime
#import json 

def download_files(git_url: str, csv_dir: str):
    """Using this function only for download CSV files. Parameters: git url and directory name for csv files"""

    # folder creation    
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    # Files download
    response = requests.get(git_url)
    files = response.json()
    #print(json.dumps(files, indent=4, sort_keys=True))

    for file in files:
        if file["name"].endswith(".csv"):           
            # files data
            file_response = requests.get(file["download_url"])
      
            # save to local dir
            file_path = os.path.join(csv_dir, file["name"])
            with open(file_path, "wb") as f:
                f.write(file_response.content)

def insert_adyen_data_to_psql(csv_dir: str, pg_engine):
    """Insert Adyen data to PSQL"""

    with pg_engine.begin() as connection:
        # list of files
        file_folder = csv_dir
        csv_files = [f for f in os.listdir(file_folder) if f.endswith(".csv")]

        # "pagination" as a  solution for big files
        for csv_file in csv_files:
            csv_path = os.path.join(file_folder, csv_file)
            
            for chunk_df in pd.read_csv(csv_path, sep=";", chunksize=50000):
                chunk_df.columns = chunk_df.columns.str.lower()
                chunk_df['date'] = pd.to_datetime(chunk_df['date']).dt.date

                chunk_df = chunk_df[["merchant_account", "batch_number", "order_ref", "date", "type", "net", "fee", "gross"]]

                # Insert data
                chunk_df.to_sql("adyen_transactions", pg_engine, if_exists="append", index=False)
                #print(pd.io.sql.get_schema(chunk_df, name='adyen_transactions', con=pg_engine) ) # using "con" we gete types sutible for psql   
