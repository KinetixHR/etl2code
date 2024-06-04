#
#
# Only run this if you want to blow the users database away and start fresh. 
#
#

# Import needed libraries
import datetime as dt
import requests
from simple_salesforce import Salesforce
import pyodbc
import pandas as pd
import sqlalchemy
import urllib
import logging
logging.basicConfig(filename='users_setup_logging.log', level=logging.INFO,
                    format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")


# Define functions that the script will use.
def run_api_call(statement):
    """
    This function runs the API call to Salesforce and returns a dataframe of the results.
    """
    session = requests.Session()
    # Setting up salesforce functionality
    sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com',
                    organizationId='00D37000000HXaI', client_id='My App', session=session)

    # generator on the results page
    fetch_results = sf.bulk.User.query_all(
        statement, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
    result_df = pd.DataFrame(all_results)
    result_df = result_df.drop(columns=['attributes'])
    return result_df

def gen_date(offset = 0):
    if offset == 0:
        today = dt.datetime.today()
    
    if offset == -1:
        today = dt.datetime.now()-dt.timedelta(1) 
    
    if offset == 1:
        today = dt.datetime.now()+dt.timedelta(1)

    day = str(today.day)
    month = str(today.month)
    year = str(today.year)
    if len(month) == 1:
        month = "0"+month
    if len(day) == 1:
        day = "0"+day
    return f"{month}/{day}/{year}"

def transform_data(df):
    """
    Function to transform data in DF along pre-determined lines.
    """
    logging.info("Starting changing column names...")

    df.columns = ["ACTIVE", 
    "ALIAS", 
    "CREATED_DATE", 
    "EMAIL", 
    "LAST_LOGIN_DATE", 
    "LAST_MODIFIED_DATE", 
    "LAST_PASSWORD_CHANGE_DATE", 
    "LAST_VIEWED_DATE", 
    "MOBILE_PHONE_NUMBER", 
    "MONTHLY_POINT_GOAL", 
    "NAME", 
    "PAYROLL_NAME", 
    "PHONE", 
    "PHOTO", 
    "PROFILE", 
    "TITLE", 
    "FULL_PHOTO", 
    "USER_ID", 
    "USER_TYPE", 
    "USERNAME", 
    "COMMISSION_ELIGIBLE", 
    "MANAGER_ID",  
    "ROLE",
    "COACH_NAME"]
    logging.info("Done with changing column names...")

    logging.info("Starting date transformations...")

    
    df["CREATED_DATE"] = pd.to_datetime(df['CREATED_DATE'], unit='ms')
    df['CREATED_DATE_CONVERTED'] = df["CREATED_DATE"].dt.tz_localize(
        'UTC').dt.tz_convert('US/Eastern')
    df["CREATED_DATE_DT"] = df["CREATED_DATE_CONVERTED"].dt.strftime(
        '%Y-%m-%d')
    df["CREATED_DATE_TS"] = df["CREATED_DATE_CONVERTED"].dt.strftime(
        '%Y-%m-%d %H:%M:%S')
    df.drop(columns=['CREATED_DATE_CONVERTED', 'CREATED_DATE'], inplace=True)

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

    
    df["LAST_LOGIN_DATE"] = pd.to_datetime(df["LAST_LOGIN_DATE"])
    df["LAST_PASSWORD_CHANGE_DATE"] = pd.to_datetime(df["LAST_PASSWORD_CHANGE_DATE"])
    df["LAST_VIEWED_DATE"] = pd.to_datetime(df["LAST_VIEWED_DATE"])
    
    df["LAST_LOGIN_DATE"] = df["LAST_LOGIN_DATE"].dt.strftime('%Y-%m-%d')
    df["LAST_PASSWORD_CHANGE_DATE"] = df["LAST_PASSWORD_CHANGE_DATE"].dt.strftime('%Y-%m-%d')
    df["LAST_VIEWED_DATE"] = df["LAST_VIEWED_DATE"].dt.strftime('%Y-%m-%d')

    logging.info("Starting Effective Date and End Date transformations")
    try:
        df["EFFECTIVE_DATE"] = gen_date()
        df["EFFECTIVE_DATE"] = pd.to_datetime(df["EFFECTIVE_DATE"])
        df["EFFECTIVE_DATE"] = df["EFFECTIVE_DATE"].dt.strftime('%Y-%m-%d')
        
        df["END_DATE"] = '9999-12-31'
        df["END_DATE"] = pd.to_datetime(df["END_DATE"],errors = 'ignore')
        #df["END_DATE"] = df["END_DATE"].dt.strftime('%Y-%m-%d')
        logging.info("Done with Eff and End Date transformations")
    
    except Exception as e:
        logging.warning("Eff and or End date transformations failed.")
        logging.warning(e)

    logging.info("Done with date transformations...")

    logging.info("Starting other transformations...")
    df = df.fillna("")
    logging.info("done with other transformations...")



    for el in df.columns:
        if df[el].dtype == 'int64':
            df[el] = df[el].astype(float)
        if df[el].dtype == 'float64':
            df[el] = df[el].astype(float)
    
    return df



# Initiate connection to Azure SQL Database
SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"

constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))

engine = sqlalchemy.create_engine(constring,echo=False)
conn = engine.connect()
try:
    conn.rollback()
    logging.info("Rollback successful")
except:
    logging.info("Rollback not needed")

# Define values that will be used to determine dates and times, as well as failure flag.
today = gen_date()
today_search_string = gen_date()

USERS_SUCCESS_FLAG = True

# Extract Data from Salesforce (TalentRover)
try:
    SOQL_STATEMENT = """SELECT IsActive, 
    Alias, 
    CreatedDate, 
    Email, 
    LastLoginDate, 
    LastModifiedDate, 
    LastPasswordChangeDate, 
    LastViewedDate, 
    MobilePhone, 
    Monthly_Goal__c, 
    Name, 
    Payroll_Name__c, 
    Phone, 
    SmallPhotoUrl, 
    ProfileId, 
    Title, 
    FullPhotoUrl, 
    Id, 
    UserType, 
    Username, 
    Commission_Eligible__c, 
    ManagerId, 
    UserRoleId,
    Coach__c
    
    FROM User """


    df_users = run_api_call(SOQL_STATEMENT)

    logging.info(df_users.shape)
    logging.info(df_users.columns)
    logging.info("Successfully pulled users from API: %s",str(df_users.shape))


except Exception as ex:
    logging.warning("Issue with loading in Users from API")
    logging.warning(ex)
    USERS_SUCCESS_FLAG = False


# Transform Data
if USERS_SUCCESS_FLAG is True:
    try:

        df_alljobs = transform_data(df_users)

        logging.info(
            "Loaded in users from API and transformed columns...: %s",str(df_users.shape))
        logging.info(df_users.columns)
        
    except Exception as ex:
        logging.warning("Issue transforming data")
        logging.warning(ex)
        USERS_SUCCESS_FLAG = False

if USERS_SUCCESS_FLAG == True:
    try: 

        df_users.to_sql('dw2_users', con=engine, if_exists='replace', index=False)
        logging.info("Done loading users")
        USERS_SUCCESS_FLAG = True


    except Exception as ex:
        logging.warning("Issue loading in users to SQL Server")
        conn.rollback()
        logging.info("Rollback successful")
        logging.warning(ex)
        USERS_SUCCESS_FLAG = False

if USERS_SUCCESS_FLAG == True:
    logging.info("Done loading all data into users table successfully, exiting script.")
else:
    logging.warning("Something went wrong with loading users, script exiting.")

