import pyodbc
import requests
from datetime import datetime

import logging
logging.basicConfig(filename='./etl2code/logs/dw2_run_stored_procedures.log', level=logging.INFO,
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
try:
    stored_procedure_call = "EXEC EnforceDates2"
    cursor.execute(stored_procedure_call)
    next_num = cursor.fetchone()[0]
    logging.info("Stored Procedure 'EXEC EnforceDates2' ran successfully.")
except:
    logging.warning("Stored Procedure 'EXEC EnforceDates2' did not complete correctly...")