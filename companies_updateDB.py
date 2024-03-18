import pandas as pd
from simple_salesforce import Salesforce
import logging
import requests
import sqlalchemy
import urllib

# Configure Logging
logging.basicConfig(filename='./etl2code/logs/companies_update_logging.log', level=logging.INFO,
                    format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

def transform_data(df):
    
    df.columns = [
        'ACCOUNT_ID', 
        'ACCOUNT_MANAGER', 
        'COMPANY_ID', 
        'NAME', 
        'DO_NOT_GENERATE_HRPO_INITIAL', 
        'INDUSTRY', 
        'LAST_MODIFIED_DATE', 
        'LAST_VIEWED_DATE', 
        'WEBSITE', 
        'CREATED_DATE'
    ]
    
    df["LAST_VIEWED_DATE"] = pd.to_datetime(df["LAST_VIEWED_DATE"])

    df["LAST_MODIFIED_DATE"] = pd.to_datetime(
        df["LAST_MODIFIED_DATE"], unit='ms')
    df["LAST_MODIFIED_DATE_CONVERTED"] = df["LAST_MODIFIED_DATE"].dt.tz_localize(
        'UTC').dt.tz_convert('US/Eastern')
    df["LAST_MODIFIED_DATE_DT"] = df["LAST_MODIFIED_DATE_CONVERTED"].dt.strftime(
        '%Y-%m-%d')
    df["LAST_MODIFIED_DATE_TS"] = df["LAST_MODIFIED_DATE_CONVERTED"].dt.strftime(
        '%Y-%m-%d %H:%M:%S')
    df.drop(columns=["LAST_MODIFIED_DATE_CONVERTED",
            "LAST_MODIFIED_DATE"], inplace=True)
    
    df["CREATED_DATE"] = pd.to_datetime(df['CREATED_DATE'], unit='ms')
    df['CREATED_DATE_CONVERTED'] = df["CREATED_DATE"].dt.tz_localize(
        'UTC').dt.tz_convert('US/Eastern')
    df["CREATED_DATE_DT"] = df["CREATED_DATE_CONVERTED"].dt.strftime(
        '%Y-%m-%d')
    df["CREATED_DATE_TS"] = df["CREATED_DATE_CONVERTED"].dt.strftime(
        '%Y-%m-%d %H:%M:%S')
    df.drop(columns=['CREATED_DATE_CONVERTED', 'CREATED_DATE'], inplace=True)

    logging.info(df['CREATED_DATE_DT'].value_counts())
    logging.info(df["LAST_MODIFIED_DATE_DT"].value_counts())
    logging.info(df.columns)

    return df

def run_api_call(statement):
    """
    This function runs the API call to Salesforce and returns a dataframe of the results.
    """
    session = requests.Session()
    # Setting up salesforce functionality
    sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session)

    # generator on the results page
    fetch_results = sf.bulk.Account.query_all(
        statement, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
    result_df = pd.DataFrame(all_results)
    result_df = result_df.drop(columns=['attributes'])
    return result_df

# Define SOQL statement
SOQL_STATEMENT = """SELECT TR1_AccountId__c, Account_Manager__c, Id, Name, Do_Not_Generate_HRPO_Initial__c, Industry, LastModifiedDate, LastViewedDate, Website, CreatedDate FROM Account"""

# Define DB connection details and connect
SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"

constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))

engine = sqlalchemy.create_engine(constring,echo=False)
conn = engine.connect()
logging.info('Connected to DB!')


try:
    # Extract data from Salesforce
    df_companies= run_api_call(SOQL_STATEMENT)
    df_companies = transform_data(df_companies)
    logging.info(df_companies.shape)
    logging.info(df_companies.columns)
    logging.info("Successfully pulled Companies from API: %s",str(df_companies.shape))
except:
    logging.warn("Companies SOQL API call did not work.")

try:
    # Load Data to Azure DB
    df_companies.to_sql('dw2_companies',con = engine, index = False, if_exists='replace')
    logging.info("Companies update completed successfully")
except Exception as e:
    logging.warning("Companies update to SQL Server did not work")
    logging.warning(str(e))
