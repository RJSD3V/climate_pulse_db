import duckdb
import os
import datetime


with duckdb.connect('dev_database.duckdb') as con:
    for i in range(1763,datetime.date.today().year-1):
        con.sql(f"CREATE TABLE IF NOT EXISTS raw_noaa_{i} as from read_csv('./raw_data/{i}.csv',auto_detect=true);")
        print(f'table for the year {i} created.')
    con.table("test").show()
    con.close()
