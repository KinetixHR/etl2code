

from simple_salesforce import Salesforce
import pyodbc
import requests
from datetime import datetime

# Salesforce credentials and initialization
sf_username = 'salesforceapps@kinetixhr.com '
sf_password = 'Kinetix2Password '
sf_security_token = 'your_security_token'
sf_instance = 'USA638'
session = requests.Session()
sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session) 


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

# Get today's date
effective_date = datetime.now().strftime('%Y-%m-%d')

# SOQL query to get job count from Salesforce
soql_query = "SELECT COUNT() FROM TR1__Job__c"  # Adjust for custom object if needed

try:
    # Query Salesforce for total job count
    sf_results = sf.query(soql_query)
    TR_Job_count = sf_results['totalSize']
    

    # SQL query to get the total job count from DW2_Jobs excluding end date and is deleted filters
    count_query = "SELECT COUNT(*) AS TotalJobs FROM DW2_Jobs where end_date = '9999-12-31' And is_deleted = 'False'"
    cursor.execute(count_query)
    total_jobs_result = cursor.fetchone()
    DW_Jobs_count = total_jobs_result[0] if total_jobs_result else 0

    # SQL statement to insert the data into Azure SQL Database
    insert_query = """
    INSERT INTO DW2_Jobs_Log (TR_Job_count, DW_Jobs_count, Effective_Date)
    VALUES (?, ?, ?)
    """
    cursor.execute(insert_query, TR_Job_count, DW_Jobs_count, effective_date)
    cnxn.commit()
    
    
    print(f"Successfully inserted daily Salesforce job count of {DW_Jobs_count} and Dw2 job count of {TR_Job_count} into DW2_Jobs_Log with effective date {effective_date}.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    cursor.close()
    cnxn.close()