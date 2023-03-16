README.md rwa_assignment_pyspark_notebook.ipynb is a notebook.
We can directly import this notebook in data bricks also. I tested it in AWS pyspark notebook

Run this note book step by step

Script needs two params. Please add below in the commented place of notebook
1. Tokens csv path 2. Output path

Logs path is already given in the code.

It writes data to two output folder in the output path
max_times - folder contains token address transferred maximum times for every month
max_value - folder contains token address transferred maximum value for every month

A short description of how you would architect a system that offers these outputs as an API for multiple time frames (1D, 1W, 1M, etc.)
Spark Job:
Spark streaming jobs monitors logs folder and reads new files
periodically and computes token value for transfers and store this data partition by year, month.
two steps

1. Store this in S3/GS
2. Store latest data in time series db(TimescaleDB/influxDB) so that

time bound queries(1D, 1W, 1M, etc.) are much faster and efficient
Historical data always present in cloud storage like - S3/GS and create Athena/big query table on top it and use for historical data analysis
backend API use time series db for all latest time periods and use data warehouse for historical analysis