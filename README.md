# The Climatology Project
![climatology_model](https://github.com/RJSD3V/climate_works_dbt/assets/20220544/1fe77541-9987-40f1-91b4-67a76aab30ba)

### Using the starter project

To start using this project, one must remember that this project uses duckdb, which is an in process OLAP database, which needs to be installed separately as a python module,and initialised using the following commands: 

``` pip install duckdb```

after installing duckdb you can initialize duckdb using the `duckdb` command, or just type duckdb followed by the db name you want to have, in this case it would be 
``` duckdb dev_database.duckdb```

Once the database file is created in your directory, you are good to go!

Post that, you need to run the `etl_noaa.py` python script that ingests the data needed for analysis from the NOAA source. The python script pulls 2 years worth of data (which is a lot, around 7GB) but you can change it to add additional years you want data for. 

Post that, you can run the  

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
