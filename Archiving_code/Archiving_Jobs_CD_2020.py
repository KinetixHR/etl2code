import pyodbc
import requests
from datetime import datetime
import datetime as dt
# Configure Logging
import logging
logging.basicConfig(filename='./etl2code/logs/dw2_jobs_archive_logging.log', level=logging.INFO,
                    format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

# Azure SQL Database credentials
server = "kinetixsql.database.windows.net" 
database = "KinetixSQL" 
username = "awhelan" 
password = "5uj7*ZpE8Y$D"
DRIVER="ODBC Driver 18 for SQL Server"

# SQL Database connection string
conn_str = f'DRIVER={DRIVER};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()
# Table name to be copied
source_table_name = 'dw2_jobs'
destination_table_name = 'Arc_dw2_jobs_CD_2020'

# Drop destination table if it already exists
cursor.execute(f"IF OBJECT_ID('dbo.{destination_table_name}', 'U') IS NOT NULL DROP TABLE dbo.{destination_table_name};")

# Create destination table as a copy of the source table
cursor.execute(f"SELECT * INTO {destination_table_name} FROM {source_table_name} where closed_date < '2020/1/1';")

# Commit the transaction and close the connection
cnxn.commit()
cnxn.close()

print("Table copied successfully!")