import pandas as pd
import boto3
import datetime
from io import StringIO
import sys
import os

def get_noaa_file(year):
    bucket_name = 'noaa-ghcn-pds'
    key_name_prefix = 'csv/by_year/'
    key= key_name_prefix+f"{year}.csv"
   
    client = boto3.client('s3')

    try:
        if os.path.exists(f'./seeds/raw_data/{year}.csv'):
            print(f"The file {year}.csv already exists")
        else:
            print("Retrieving the object we need.")
            csv_obj = client.get_object(Bucket=bucket_name, Key=key)
            print("Parsing the body from the Object Retreived.")
            body = csv_obj['Body']
            print("Decoding the Body in UTF-8")
            csv_string= body.read().decode('utf-8')
            print("Converting the CSV to a Pandas DataFrame")
            df= pd.read_csv(StringIO(csv_string))
            print(f"Storing file as ./raw/{year}.csv")
            df.to_csv(f"./seeds/raw_data/{year}.csv",index=False)

    except:

        print("Couldn't retrieve file, doesn't exist.")

    finally:

        print("Trying to Create the File Locally..")



def get_all_noaa_files(start_year, end_year):
    for y in range(start_year, end_year+1):
        print(f"Getting NOAA Data for year: {y}")
        get_noaa_file(y)
    print("All File Retrieved!! Check your storage location")

if __name__ == '__main__':
    start = 1763
    end = datetime.date.today().year-1
    
    get_all_noaa_files(start,end)
 






