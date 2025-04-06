<img src="https://github.com/RJSD3V/climate_works_db/assets/20220544/7303deac-dcfd-4ec7-baf8-d48785b6eac3" alt="ClimateWorksLogo" width="200"/>

# ClimateWorks DB - A Climatology Essay written purely in Data. 

## The Climatology Project
This project is for those who have always wanted to understand Climate better. Its a simplistic use case of building an entire data pipeline and dashbaord based on real Climate data. 
![climatology_model](https://github.com/RJSD3V/climate_works_dbt/assets/20220544/1fe77541-9987-40f1-91b4-67a76aab30ba)

### Using the starter project

Look at the [documentation](https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn) of the data:

To start using this project, one must remember that this project uses duckdb, which is an in process OLAP database, which needs to be installed separately as a python module,and initialised using the following commands: 

``` pip install duckdb```

after installing duckdb you can initialize duckdb using the `duckdb` command, or just type duckdb followed by the db name you want to have, in this case it would be 
``` duckdb dev_database.duckdb```

Once the database file is created in your directory, you are good to go!

## The Data
We are using the NOAA GHCN-D open source data referred [here](https://docs.opendata.aws/noaa-ghcn-pds/readme.html) to do our climate analysis and build a basic data pipeline around. 

To get the data you need to run the `get_data.py` python script that ingests the data needed for analysis from the NOAA source. Data is stored in a yearlfy fashion where each file has daily readings of different metrics taken at different station locations around the world, in the form of vertical tables. The data lies in AWS S3 buckets, so needs to be ingested using the boto3 package, specific to the python implementation. The python script pulls 1 years worth of data (which is a lot, around 7GB) based on the command line argument that you provide to it. The command line argument is the year you want the data for. For example, if you want to ingest the csv data file for the year 2010, you type: 

 ` python get_data.py 2010`

 this takes a few minutes (since the file is large with millions of records) but it will download and store the file in the seeds folder (Usually reserved by dbt for csv files). In the future, the same file will include the duckdb python module and directly insert the file into your duckdb database. That is an upgrade that is 


After ingesting the files, to get the files as raw tables in your duckdb database, you simply have to run the etl_noaa.sql file in duckdb using the following: 

```
duckdb dev_database.duckdb
D create schema DEV_SODE;
D use dev_sode
D .read etl_noaa.sql
100% ▕████████████████████████████████████████████████████████████▏ 
100% ▕████████████████████████████████████████████████████████████▏ 
D show tables;
┌────────────────┐
│      name      │
│    varchar     │
├────────────────┤
│ noaa_ghcn_2022 │
│ noaa_ghcn_2023 │
└────────────────┘
```

The above commands are duckdb specific, and you can run them in order to pull the csv files into the OLAP database that is created in process using the duckdb command, create the schema and read the files using the `.read` command that executes raw SQL statements like the following: 

```
//etl_noaa.sql

CREATE TABLE IF NOT EXISTS dev_sode.noaa_ghcn_2022 as SELECT * FROM read_csv('./seeds/raw_data/2022.csv' ,AUTO_DETECT=TRUE);

CREATE TABLE IF NOT EXISTS dev_sode.noaa_ghcn_2023 as SELECT * FROM read_csv('./seeds/raw_data/2023.csv',AUTO_DETECT=TRUE);

```

Duckdb specializes in ingesting data from a variety of file formats including csv and parquet, and doing so at incredible speed. Just for context, NOAA data for each year has over 37M records each, and you could expect each file to be ingested in a matter of seconds into the in-process duckdb file. 



## Running the Data processing pipeline. 

The files we just ingested are vertical tables with climate based readings taken daily at different stations around the world. There are more than a dozen types of readings including Temperature, Humidity, precipitation, Snowfall , Sea level, etc. 

For starters we would be ingesting basic temperature and precipitation readings, and as this project develops further, there will be more in depth analysis based on climate based use cases and ingesting other location based metadata. 
 
<img src="" alt="ClimateWorksLogo" width="200"/>


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


## Data Sources

You need to ingest the data sources first. You can ingest a single source by using the get_data.py script by running it with inline command line parameter that needs the year for which you want the data for. 
