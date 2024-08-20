import pandas as pd
import pyodbc
from simple_salesforce import Salesforce
import logging
import requests
from sqlalchemy import create_engine, Table, MetaData
import urllib
import datetime as dt
import sqlalchemy

# Salesforce credentials and initialization
sf_username = 'salesforceapps@kinetixhr.com '
sf_password = 'Kinetix2Password '
sf_security_token = 'your_security_token'
sf_instance = 'USA638'

logging.basicConfig(filename='./etl2code/logs/submittals_daily_update.log', level=logging.INFO,
                        format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")


def run_api_call(statement):

    # Salesforce credentials and initialization
    sf_username = 'salesforceapps@kinetixhr.com '
    sf_password = 'Kinetix2Password '
    sf_security_token = 'your_security_token'
    sf_instance = 'USA638'
    session = requests.Session()
    sf = Salesforce(password='Kinetix2Password', username='salesforceapps@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session) 
    #sf = Salesforce(username=sf_username, password=sf_password, security_token=sf_security_token)

    
    # Fetching data into a pandas DataFrame
    sf_data = sf.query_all(SOQL_STATEMENT)
    records = sf_data.get('records')
    df = pd.DataFrame(records)
    print(df.columns) 

    if 'attributes' in df.columns:
     df.drop(columns=['attributes'], inplace=True)
    return df
def transform_data(df):
    logging.info("Starting changing column names...")
    #records = sf_data.get('records')
    #df = pd.DataFrame(records)
    df.columns = ["ID",'NAME','CREATED_BY','JOB_ID','COMPANY_NAME','REJECTION_NOTES','REJECTION_REASON','ATS_STAGE_STATUS','CANDIDATE_OWNER','CANDIDATE','SUBMITTED_BY','NUMBER_OF_DAYS_IN_STAGE','STAGE','STAGE_START_DATE','STAGE_END_DATE','CREATED_DATE']  
    df["CREATED_DATE"] = pd.to_datetime(df['CREATED_DATE'], errors='coerce')
    df["CREATED_DATE"] = df["CREATED_DATE"].dt.strftime('%Y-%m-%d')
    df["STAGE_START_DATE"] = pd.to_datetime(df['STAGE_START_DATE'], errors='coerce')
    df["STAGE_START_DATE"] = df["STAGE_START_DATE"].dt.strftime('%Y-%m-%d')
    df["STAGE_END_DATE"] = pd.to_datetime(df['STAGE_END_DATE'], errors='coerce')
    df["STAGE_END_DATE"] = df["STAGE_END_DATE"].dt.strftime('%Y-%m-%d')
    return df





SOQL_STATEMENT = f"""SELECT Id ,
Name,
CreatedById,
TR1__Job__c,
TR1__Account__c,
TR1__Rejection_Notes__c,
TR1__Rejection_Notification__c,
TR1__Status__c,
TR1__Submittal_Contact_Owner__c,
TR1__Submittal__c,
Submitted_By__c,
TR1__Number_of_Days_in_Stage__c,
TR1__Stage__c,
TR1__Stage_Start_Date__c,
TR1__Stage_End_Date__c,
CreatedDate__c

FROM TR1__Submittal__c WHERE CreatedDate__c >= 2023-01-01"""
logging.info(SOQL_STATEMENT)

# SQL initializing
# Azure SQL Database credentials
SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"

constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))

engine = sqlalchemy.create_engine(constring,echo=False)
conn = engine.connect()


#sql table name
table_name = 'dw2_submittals'
df = run_api_call(SOQL_STATEMENT)
df = transform_data(df)

# Using SQLAlchemy to insert data
df.to_sql(table_name, con=engine, if_exists='replace', index=False)

logging.info('success')
logging.info(df.shape)
logging.info(df.columns)
logging.info("Data transferred successfully!")