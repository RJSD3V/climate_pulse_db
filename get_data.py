import pandas as pd
import datetime
from io import StringIO
import boto3
import sys
import os
import duckdb
from dotenv import load_dotenv
import shutil
import gzip

load_dotenv()
def unzip(year):
    if  os.path.exists(f'./seeds/raw/{year}.csv.gz'):
         with gzip.open(f'./seeds/raw/{year}.csv.gz', 'rb') as f_in:
              with open(f'./seeds/raw/{year}.csv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                print("File created as csv")
                csv_string = f'./seeds/raw/{year}.csv'
                return csv_string
    else:
        raise(".gz file doesn't exist. Make sure the file is downloaded")

def get_noaa_file(year):
    bucket_name = 'noaa-ghcn-pds'
    key_name_prefix = 'csv.gz/'
    key= key_name_prefix+f"{year}.csv.gz"
    print(key)
   
    client = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
        region_name = 'us-east-1'
        
        )

    try:
        if os.path.exists(f'./seeds/raw/{year}.csv.gz'):
            print(f"The file {year}.csv already exists")
            return True
        else:
            print("Retrieving the object we need.")
            csv_obj = client.get_object(Bucket=bucket_name, Key=key)
            print("Parsing the body from the Object Retreived.")
            body = csv_obj['Body']
            print("Decoding the Body in UTF-8")
            # csv_string= body.read().decode('utf-8')
            print("Converting the CSV to a Pandas DataFrame")
            os.makedirs('./seeds/raw/', exist_ok=True)
            file_path=f'./seeds/raw/{year}.csv.gz'
            with open(file_path, 'wb') as f:
                f.write(body.read())
            print("Downloaded .gz file successfully")
            return True

    except Exception as e:
        print(e)
        print("Couldn't retrieve file, doesn't exist.")
        return False

        
    


def load_data_to_db(year):
    file_path=f'./seeds/raw/{year}.csv'
    table_name=f'noaa_ghcn_{year}'
    
    con = duckdb.connect('./dev_database.duckdb')
    con.sql('CREATE SCHEMA IF NOT EXISTS dev_sode;')
    # Note: duckdb.sql connects to the default in-memory database connection
    # you can explicitly mention what db file you want to connect it to, in case you have multiple.
    df= pd.read_csv(file_path,names=['station_id','date','reading_type','value','m_flag','q_flag','s_flag', 'time'])
    print(df.head(5))
    con.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")


def push_metadata(data, database_name, table_name):
    
    conn = duckdb.connect(database=database_name,read_only=False)
    conn.execute("SET GLOBAL pandas_analyze_sample=100000")
    conn.execute("CREATE SCHEMA IF NOT EXISTS dev_sode;")
    print(f"Reading CSV file {data}")
    df = pd.read_csv(data, names=['station_id','date','reading_type','value','m_flag','q_flag','s_flag', 'time'])
    df.fillna('')
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")

    conn.close()



def get_all_noaa_files(start_year, end_year):
    for y in range(start_year, end_year+1):
        print(f"Getting NOAA Data for year: {y}")
        download = get_noaa_file(y)
        csv_path=''
        if download:
            csv_path=unzip(y)
            print(csv_path)
            load_data_to_db(y)
        # push_metadata(csv_path, 'dev_database.duckdb',f"ghcn_{y}")
    print("All File Retrieved!! Check your storage location")


if __name__ == '__main__':
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    get_all_noaa_files(start_year, end_year)