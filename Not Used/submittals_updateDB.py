import pandas as pd
from simple_salesforce import Salesforce
import logging
import requests
import sqlalchemy
import urllib
import datetime as dt

logging.basicConfig(filename='./etl2code/logs/submittals_update_logging.log', level=logging.INFO,
                    format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")



def run_api_call(statement):
    """
    This function runs the API call to Salesforce and returns a dataframe of the results.
    """
    session = requests.Session()
    # Setting up salesforce functionality
    sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session)

    # generator on the results page
    fetch_results = sf.bulk.TR1__Submittal__c.query_all(
        statement, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
    result_df = pd.DataFrame(all_results)
    result_df = result_df.drop(columns=['attributes'])
    return result_df


def transform_data(df):
    """
    Function to transform data in DF along pre-determined lines.
    """
    logging.info("Starting changing column names...")
    logging.info(f"Working with {df.shape[0]} rows")
    df.columns = ["ID",
'NAME',
'CREATED_BY',
'JOB_ID',
'COMPANY_NAME',
'REJECTION_NOTES',
'REJECTION_REASON',
'ATS_STAGE_STATUS',
'CANDIDATE_OWNER',
'CANDIDATE',
'SUBMITTED_BY',
'NUMBER_OF_DAYS_IN_STAGE',
'STAGE_END_DATE',
'STAGE_START_DATE',
'STAGE',
'CREATED_DATE']
    logging.info("Done with changing column names...")

    logging.info("Starting date transformations...")

    #df['STAGE_START_DATE'] = pd.to_datetime(df['STAGE_START_DATE'])
    #df["STAGE_START_DATE_CONVERTED"] = df["STAGE_START_DATE"].dt.tz_localize(
    #    'UTC').dt.tz_convert('US/Eastern')
    #df['STAGE_START_DATE_DT'] = df['STAGE_START_DATE'].dt.strftime('%Y-%m-%d')
    #df.drop(columns=[ 'STAGE_START_DATE'], inplace=True)

    #df['STAGE_END_DATE'] = pd.to_datetime(df['STAGE_END_DATE'])
    #df["STAGE_END_DATE_CONVERTED"] = df['STAGE_END_DATE'].dt.tz_localize(
    #    'UTC').dt.tz_convert('US/Eastern')
    #df['STAGE_END_DATE_DT'] = df['STAGE_END_DATE'].dt.strftime('%Y-%m-%d')
    # df.drop(columns=['STAGE_END_DATE'], inplace=True)
    
    #df["CREATED_DATE"] = pd.to_datetime(df['CREATED_DATE'])
    #df['CREATED_DATE_CONVERTED'] = df["CREATED_DATE"].dt.tz_localize(
    #    'UTC').dt.tz_convert('US/Eastern')
    #df["CREATED_DATE_DT"] = df["CREATED_DATE"].dt.strftime('%Y-%m-%d')
    #df["CREATED_DATE_TS"] = df["CREATED_DATE"].dt.strftime('%Y-%m-%d %H:%M:%S')
    #df.drop(columns=['CREATED_DATE'], inplace=True)

    logging.info("Done with date transformations...")

    logging.info("Starting other transformations...")
    df = df.fillna("")
    logging.info("done with other transformations...")

    #for el in df.columns:
    #   if df[el].dtype == 'int64':
    #       df[el] = df[el].astype(float)
    #    if df[el].dtype == 'float64':
    #        df[el] = df[el].astype(float)
    
   # df =df[df["CREATED_DATE_DT"] >= "01/01/2023"]
    

    return df

update_date = "1/1/2023"
date_format = "%m/%d/%Y"
update_date = dt.datetime.strptime(update_date,date_format).strftime("%Y-%m-%d")
logging.info(update_date)

SOQL_STATEMENT = f"""SELECT Id,
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
TR1__Stage_End_Date__c,
TR1__Stage_Start_Date__c,
TR1__Stage__c,
CreatedDate__c

FROM TR1__Submittal__c WHERE CreatedDate__c >= 2023-01-01"""
logging.info(SOQL_STATEMENT)

SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"

constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))

engine = sqlalchemy.create_engine(constring,echo=False)
conn = engine.connect()


try:
    df_submittals= run_api_call(SOQL_STATEMENT)
    df_submittals = transform_data(df_submittals)
    logging.info(df_submittals.shape)
    logging.info(df_submittals.columns)
    logging.info("Successfully pulled Submittals from API: %s",str(df_submittals.shape))
except Exception as e:
    logging.warning("Submittals SOQL API call did not work.")
    logging.warning(e)


try:
    df_submittals.to_sql('dw2_submittals',con = engine, index = False, if_exists='replace')
    logging.info("Submittals update completed successfully")
except Exception as e:
    logging.warning("Submittals update to SQL Server did not work")
    logging.warning(str(e))
