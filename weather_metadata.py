import boto3
import pandas as pd
import re
import os
from dotenv import load_dotenv


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

    df_states = pd.read_fwf('state.txt')
    df_states.columns=['state_code','state_name']
    df_states.to_csv('states.csv')

def get_countries():

    bucket_name = 'noaa-ghcn-pds'
    key = 'ghcnd-countries.txt'

    client = boto3.client('s3')

    text_obj = client.get_object(Bucket=bucket_name, Key=key)
    text_body = text_obj['Body'].read().decode('utf-8')
    #text_csv = re.sub(r'\s+',',',text_body)

    # text_body.split('\n')
    with open('countries.txt','w') as f:
        f.write(text_body)
    
    df_countries = pd.read_fwf('countries.txt')
    df_countries.columns=['country_code','country_name']
    df_countries.to_csv('countries.csv')


def get_stations():

    bucket_name = 'noaa-ghcn-pds'
    key = 'ghcnd-stations.txt'

    client = boto3.client('s3')

    text_obj = client.get_object(Bucket=bucket_name, Key=key)
    text_body = text_obj['Body'].read().decode('utf-8')
    #text_csv = re.sub(r'\s+',',',text_body)

    # text_body.split('\n')
    with open('stations.txt','w') as f:
        f.write(text_body)
    




if __name__ == '__main__':
    get_states()