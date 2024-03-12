import logging
logging.basicConfig(filename='./etl2code/logs/dw2_opens_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

import pandas as pd
import os, uuid
from io import StringIO
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import date
import pyodbc
from simple_salesforce import Salesforce
import requests

def getDataFromAPI(soql_statement):
    session = requests.Session()
    # Setting up salesforce functionality
    sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session) 

    #generator on the results page
    fetch_results = sf.bulk.TR1__Job__c.query_all(soql_statement, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
    df_opens = pd.DataFrame(all_results)
    
    return df_opens

def aggregateData(df):
    df = df.drop(columns=['attributes'])
    df.columns = ["JOB_ID", "JOB_CREATED_DATE", "JOB_OPEN_DATE"]
    df["EFFECTIVE_DATE"] = today
    #logging.info("want these:",["JOB_ID","JOB_CREATED_DATE","JOB_OPEN_DATE","EFFECTIVE_DATE"])
    #df.columns = ["JOB_ID", "JOB_CREATED_DATE","JOB_OPEN_DATE","EFFECTIVE_DATE"]
    for el in df.columns:
        df[el] = df[el].fillna("")
        #df_users["LOAD_DATE"] = today
    #df["JOB_CREATED_DATE"] = pd.to_datetime(df["JOB_CREATED_DATE"], unit='ms')
    #df["JOB_CREATED_DATE"] = df["JOB_CREATED_DATE"].dt.strftime('%Y-%m-%d')
    logging.info("Loaded in opens file & transformed data from API...:" + str(df.shape))



    df = df.groupby("EFFECTIVE_DATE")
    todays_values = df.count().values[0][0]

    to_load = {
        'EFFECTIVE_DATE': today,
        'OPEN_INVENTORY': [todays_values]
    }

    df_to_load = pd.DataFrame(to_load)

    return df_to_load


opens_success_flag = True

server = "kinetixsql.database.windows.net" 
database = "KinetixSQL" 
username = "awhelan" 
password = "5uj7*ZpE8Y$D"
cnxn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+ password)


today = date.today()
today_search_string = date.today().strftime('%m_%d_%Y')
oj_CONTAINERNAME = "dataloaderexports1/openjobs"   

soql_statement = """SELECT Id, 
CreatedDate, 
TR1__Open_Date__c

FROM TR1__Job__c 


WHERE TR1__Status__c != 'Closed' 
AND TR1__Status__c != 'On Hold' 
AND (NOT Name LIKE '%funnel%') 
AND (NOT TR1__Account_Name__c LIKE '%test%') 
AND (Record_Type_Name__c = 'RPO' OR Record_Type_Name__c = 'HRPO' OR Record_Type_Name__c = 'Permanent' OR Record_Type_Name__c = 'Consulting' OR Record_Type_Name__c = 'Retained' OR Record_Type_Name__c = 'Internal Recruitment')
AND TR1__Status__c != 'Hold' 
AND (NOT TR1__Account_Name__c LIKE '%Kinetix%') 
AND (NOT TR1__Account_Name__c LIKE '%training%')"""


try:     
    df_opens = getDataFromAPI(soql_statement)

    logging.info(df_opens.columns)
    logging.info(df_opens.head(2))
    logging.info(f"Successfully loaded jobs from API: {df_opens.shape}")




except Exception as ex:
    logging.warning("Issue with loading open Jobs from API")
    logging.warning(ex)
    opens_success_flag = False


if opens_success_flag == True:
    try:
        df_opens = aggregateData(df_opens)
    except Exception as ex:
        logging.warning("Issue loading/transforming in opens from API")
        logging.warning(ex)
        opens_success_flag = False



if opens_success_flag == True:
    cursor = cnxn.cursor()
    try: 
        for index, row in df_opens.iterrows():
            # cursor.execute("""INSERT INTO [dbo].[dw_opens] ("JOB_ID", "REQ_NUMBER","CLOSED_REASON", "COMPANY", "JOB_CREATED_DATE","JOB_NAME", "JOB_OWNER", "JOB_STAGE_TEXT", "JOB_OPEN_DATE", "JOB_STATUS","TARGET_BILLING_DATE","EFFECTIVE_DATE") values(?,?,?,?,?,?,?,?,?,?,?,?)""", row.JOB_ID, row.REQ_NUMBER, row.CLOSED_REASON, row.COMPANY, row.JOB_CREATED_DATE, row.JOB_NAME, row.JOB_OWNER, row.JOB_STAGE_TEXT, row.JOB_OPEN_DATE, row.JOB_STATUS, row.TARGET_BILLING_DATE, row.EFFECTIVE_DATE)
            cursor.execute("""INSERT INTO [dbo].[dw2_opens] ("EFFECTIVE_DATE","OPEN_INVENTORY") values(?,?)""", row.EFFECTIVE_DATE, row.OPEN_INVENTORY)
        cnxn.commit()
        cursor.close()
        logging.info("Done loading opens")
    except Exception as ex:
        logging.warning("Issue loading in opens to SQL Server")
        logging.warning(ex)
        opens_success_flag = False
        cursor.close()

if opens_success_flag == True:
    logging.info("Done loading all data into opens table successfully, exiting script.")
else:
    logging.warning("Something went wrong with loading opens, script exiting.")

