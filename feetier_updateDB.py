import pandas as pd
from simple_salesforce import Salesforce
import logging
import requests
import sqlalchemy
import urllib

# Configure logging
logging.basicConfig(filename='./etl2code/logs/feetier_update_logging.log', level=logging.INFO,
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
    fetch_results = sf.bulk.Fee_Tiers__c.query_all(
        statement, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
    result_df = pd.DataFrame(all_results)
    result_df = result_df.drop(columns=['attributes'])
    return result_df

# Define the SOQL query statement
SOQL_STATEMENT = """SELECT Id, Name, Tier__c, Quick_Name__c, KX_Active__c,Fee_Basis__c,Fee_Amount__c FROM Fee_Tiers__c"""


SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"

# Create the connection string
constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))

# Create a SQLAlchemy engine
engine = sqlalchemy.create_engine(constring,echo=False)
conn = engine.connect()


try:
    # Fetch data from Salesforce API
    df_feetier = run_api_call(SOQL_STATEMENT)

    logging.info(df_feetier.shape)
    logging.info(df_feetier.columns)
    logging.info("Successfully pulled Fee tiers from API: %s",str(df_feetier.shape))
except:
    logging.warn("Fee Tier SOQL API call did not work.")

try:
    # Write the data to a SQL table named 'dw2_feetier'
    df_feetier.to_sql('dw2_feetier',con = engine, index = False, if_exists='replace')
    logging.info("Fee Tier update completed successfully")
except Exception as e:
    logging.warning("Fee Tier update to SQL Server did not work")
    logging.warning(str(e))
