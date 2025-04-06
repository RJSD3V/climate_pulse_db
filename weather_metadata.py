import boto3
import pandas as pd
import re
import os
from dotenv import load_dotenv
import duckdb


load_dotenv()

def get_states():
    bucket_name = 'noaa-ghcn-pds'
    key = 'ghcnd-states.txt'

    client = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
        region_name = 'us-east-1'
        
        )

    text_obj = client.get_object(Bucket=bucket_name, Key=key)
    text_body = text_obj['Body'].read().decode('utf-8')
    #text_csv = re.sub(r'\s+',',',text_body)

    # text_body.split('\n')
    with open('state.txt','w') as f:
        f.write(text_body)

    df_states = pd.read_fwf('state.txt',index_col=0)
    df_states.columns=['state_code','state_name']
    df_states.to_csv('states.csv')
    return 'states.csv'

def get_countries():

    bucket_name = 'noaa-ghcn-pds'
    key = 'ghcnd-countries.txt'

    client = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
        region_name = 'us-east-1'
        
        )

    text_obj = client.get_object(Bucket=bucket_name, Key=key)
    text_body = text_obj['Body'].read().decode('utf-8')
    #text_csv = re.sub(r'\s+',',',text_body)

    # text_body.split('\n')
    with open('countries.txt','w') as f:
        f.write(text_body)
    
    df_countries = pd.read_fwf('countries.txt',index_col=0)
    df_countries.columns=['country_code','country_name']
    df_countries.to_csv('countries.csv')
    return 'countries.csv'


def get_stations():

    bucket_name = 'noaa-ghcn-pds'
    key = 'ghcnd-stations.txt'

    client = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
        region_name = 'us-east-1'
        
        )

    text_obj = client.get_object(Bucket=bucket_name, Key=key)
    text_body = text_obj['Body'].read().decode('utf-8')
    #text_csv = re.sub(r'\s+',',',text_body)

    # text_body.split('\n')
    with open('stations.txt','w') as f:
        f.write(text_body)
    df_stations = pd.read_fwf('stations.txt',index_col=0)
    df_stations.columns=['station_id','latitude','longitude','state','station_name','gsn_flag','hcn_crn_flag','wmo_number']
    df_stations.to_csv('stations.csv')

    return 'stations.csv'
    

def load_data_to_db(table):
    
    table_name=table.split('.')[0]
    
    con = duckdb.connect('./dev_database.duckdb')
    con.sql('CREATE SCHEMA IF NOT EXISTS dev_sode;')
    # Note: duckdb.sql connects to the default in-memory database connection
    # you can explicitly mention what db file you want to connect it to, in case you have multiple.
    df= pd.read_csv(table)
    print(df.head(5))
    con.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")


if __name__ == '__main__':
    load_data_to_db(get_stations())
    load_data_to_db(get_states())
    load_data_to_db(get_countries())