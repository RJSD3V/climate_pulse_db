import pandas as pd
import boto3
import datetime
from io import StringIO
import sys
import os
import duckdb

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
            print(df.head(5))
            print(f"Storing file as ./raw/{year}.csv")
            df.to_csv(f"./seeds/raw_data/{year}.csv",index=False)
            # load_data_to_db(f'noaa_ghcn_{year}', df)

    except Exception as e:
        print(e)
        print("Couldn't retrieve file, doesn't exist.")
        return None
    finally:

        print("Trying to Create the File Locally..")
        return f"./seeds/raw_data/{year}.csv"
    


def load_data_to_db(name, data):
    con = duckdb.connect('./dev_database.duckdb')
    con.sql('use schema dev_sode;')
    # Note: duckdb.sql connects to the default in-memory database connection
    # you can explicitly mention what db file you want to connect it to, in case you have multiple.
    con.sql(f"CREATE TABLE {name} AS SELECT * FROM data")


def push_metadata(data, database_name, table_name):
    duckdb.default_connection.execute("SET GLOBAL pandas_analyze_sample=100000")
    conn = duckdb.connect(database=database_name,read_only=False)
    conn.execute("CREATE SCHEMA IF NOT EXISTS dev_sode;")
    df = pd.read_csv(data)
    df.fillna('')
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")

    conn.close()



def get_all_noaa_files(start_year, end_year):
    for y in range(start_year, end_year+1):
        print(f"Getting NOAA Data for year: {y}")
        data_file = get_noaa_file(y)
        push_metadata(data_file, 'dev_database.duckdb',f"ghcn_{y}")
    print("Data file retrieved, now getting metadata files")
    push_metadata('states.csv', "dev_database.duckdb", "states")
    push_metadata('stations.csv',"dev_database.duckdb", "stations")
    push_metadata('countries.csv', "dev_database.duckdb", "countries")
    print("All File Retrieved!! Check your storage location")


if __name__ == '__main__':
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    get_all_noaa_files(start_year, end_year)
 






