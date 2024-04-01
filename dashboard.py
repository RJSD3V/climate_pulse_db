import streamlit as st
import duckdb
import pandas as pd
import numpy as np



con = duckdb.connect('dev_database.duckdb')
con.sql('use dev_database.dev_sode')

df = con.sql("select * from fact_daily_climate_parameters").df()

st.markdown('# Hello World!')
st.markdown('## Look at this data: ')

st.markdown('Creating :red[Dashboard]')
st.markdown('With dbt models powering this, we can do this all day, keep iterating. Add graphs')  
st.markdown('Source Flag S --> Global Summary of the Day (NCDC DSI-9618)NOTE: “S” values are derived from hourly synoptic reports exchanged on the Global Telecommunications System (GTS). Daily values derived in this fashion may differ significantly from “true” daily data, particularly for precipitation (i.e., use with caution).')

st.markdown('## Daily Min and Max Temperature Scatter Chart')
st.markdown('> Station Details: Global Summary of the Day (NCDC DSI-9618)')
st.scatter_chart(df,x='reading_date',y=[ 'daily_max_temperature_celsius','daily_min_temperature_celsius',], color=['#AA0000','#007FFF'])
st.markdown("And thats all!")


st.markdown('## Global Precipitation Data')
st.text('Lets look at precipitation data globally.')
st.scatter_chart(df,x='reading_date',y='precipitation_mm')

# arr = np.random.normal(1, 1, size=100)
# fig, ax = plt.subplots()
# ax.hist(arr, bins=20)

# st.pyplot(fig)




# #AA0000 Red
# #7CB9E8 Blue
# #7FFFD4 Aquamarine
# #007FFF Azure
# #4FFFB0 Mint Green