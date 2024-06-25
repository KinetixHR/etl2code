#
#
# Only run this if you want to blow the jobs database away and start fresh. 
#
#


import datetime as dt
import requests
from simple_salesforce import Salesforce
import pyodbc
import pandas as pd
import sqlalchemy
import urllib
import logging
logging.basicConfig(filename='./etl2code/logs/alljobs_logging.log', level=logging.INFO,
                    format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

# Import rest of needed libraries

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
    fetch_results = sf.bulk.TR1__Job__c.query_all(
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

    df.columns = ['JOB_ID', 'ACCOUNT_MANAGER', 'ACTION_PLAN',
               'COUNT_NET_LONG_LIST', 'AFFILIATE_VENDORS',
               'PRIMARY_JOB', 'BUDGETED_START_DATE', 'CERTIFICATION',
               'EXTERNAL_CHALLENGES', 'INTERNAL_CHALLENGES',
               'CITY', 'INDUSTRY', 'CLIENT_REQ_NUMBER',
               'CLOSED_DATE', 'CLOSED_REASON', 'COACH',
               'COMPANY', 'COMPANY_NAME', 'COMPENSATION_COMMENTS',
               'CONTACT', 'COUNT_1ST_INTERVIEWS',
               'CREATED_BY_ID',
               'CREATED_DATE', 'COUNT_ALLTIME_LONG_LIST', 'CUSTOMER_AGREEMENT',
               'DAYS_OPEN', 'IS_DELETED', 'DEPARTMENT_NAME',
               'DEPARTMENT_NUMBER', 'EDUCATION', 'ESTIMATED_END_DATE',
               'ESTIMATED_START_DATE', 'FEE_AMOUNT', 'FEE_TIER',
               'FLSA', 'FTE', 'HIRING_MANAGER',
               'INTAKE_COMPLETED', 'INTAKE_COMPLETED_DATE',
               'INTAKE_COMPLETED_TIMELINE', 'INTERNAL_ROLE', 'INVOICE_PAID',
               'INVOICE_PAID_DATE', 'JOB_FAMILY', 'NAME', 'JOB_NUMBER',
               'JOB_OWNER', 'JOB_STAGE_TEXT', 'LAST_ACTIVITY_DATE',
               'LAST_MODIFIED_BY_ID', 'LAST_MODIFIED_DATE', 'LEVEL',
               'MAXIMUM_PAY_RATE',
               'MINIMUM_PAY_RATE', 'OPEN_DATE', 'OWNER_ID',
               'PAYGRADE', 'PIPELINE_JOB', 'PO_NUMBER', 'POINT_VALUE',
               'POST_EXTERNALLY', 'PRIMARY_JOB_REQ', 'PRIMARY_SECONDARY',
               'PROJECT', 'RECORD_TYPE_NAME', 'REGIONAL_AREA',
               'SALARY_LOW', 'SALARY_MID', 'SHIFT_INFORMATION',
               'STATE_AREA', 'STATUS', 'TARGETED_BILLING_DATE',
               'COUNT_TOTAL_OFFERS', 'COUNT_TOTAL_SUBMITTALS',
               'VARIABLE_PROJECT_FTE']
    logging.info("Done with changing column names...")

    logging.info("Starting date transformations...")
    df["INVOICE_PAID_DATE"] = pd.to_datetime(df['INVOICE_PAID_DATE'], unit='ms')
    
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

    df["LAST_ACTIVITY_DATE"] = pd.to_datetime(
        df["LAST_ACTIVITY_DATE"], errors='coerce')
    df["OPEN_DATE"] = pd.to_datetime(df["OPEN_DATE"])
    df["CLOSED_DATE"] = pd.to_datetime(df["CLOSED_DATE"], errors='coerce')

    df["INTAKE_COMPLETED_DATE"] = pd.to_datetime(df['INTAKE_COMPLETED_DATE'], unit='ms')
    df['INTAKE_COMPLETED_DATE'] = df["INTAKE_COMPLETED_DATE"].dt.tz_localize('US/Eastern')
    df["INTAKE_COMPLETED_DATE"] = df["INTAKE_COMPLETED_DATE"].dt.strftime('%Y-%m-%d')

    df["INVOICE_PAID_DATE"] = df["INVOICE_PAID_DATE"].dt.strftime('%Y-%m-%d')
    df["LAST_ACTIVITY_DATE"] = df["LAST_ACTIVITY_DATE"].dt.strftime('%Y-%m-%d')
    df["OPEN_DATE"] = df["OPEN_DATE"].dt.strftime('%Y-%m-%d')
    df["CLOSED_DATE"] = df["CLOSED_DATE"].dt.strftime('%Y-%m-%d')
    
    
    logging.info("Done with date transformations...")

    logging.info("Starting other transformations...")
    #df = df.fillna("")
    logging.info("done with other transformations...")
    logging.info(df["LAST_MODIFIED_DATE_TS"].dtype)

    df["BUDGETED_START_DATE"] = pd.to_datetime(df['BUDGETED_START_DATE'],errors='coerce')
    df["ESTIMATED_START_DATE"] = pd.to_datetime(df['ESTIMATED_START_DATE'],errors='coerce')
    df["INTAKE_COMPLETED_DATE"] = pd.to_datetime(df['INTAKE_COMPLETED_DATE'],errors='coerce')
    df["LAST_ACTIVITY_DATE"] = pd.to_datetime(df["LAST_ACTIVITY_DATE"],errors = 'coerce')
    df["CLOSED_DATE"] = pd.to_datetime(df["CLOSED_DATE"],errors = 'coerce')
    df["OPEN_DATE"] = pd.to_datetime(df["OPEN_DATE"],errors = 'coerce')
    
    df["BUDGETED_START_DATE"] = df["BUDGETED_START_DATE"].dt.strftime('%Y-%m-%d')
    df["ESTIMATED_START_DATE"] = df["ESTIMATED_START_DATE"].dt.strftime('%Y-%m-%d')
    df["CLOSED_DATE"] = df["CLOSED_DATE"].dt.strftime('%Y-%m-%d')

    df['LAST_ACTIVITY_DATE'] = df["LAST_ACTIVITY_DATE"].dt.strftime('%Y-%m-%d')
    df["INTAKE_COMPLETED_DATE"] = df['INTAKE_COMPLETED_DATE'].dt.strftime('%Y-%m-%d')
    df['OPEN_DATE'] = df["OPEN_DATE"]
    
    df["EFFECTIVE_DATE"] = gen_date()
    df["EFFECTIVE_DATE"] = pd.to_datetime(df["EFFECTIVE_DATE"],errors = 'coerce')
    df["EFFECTIVE_DATE"] = df["EFFECTIVE_DATE"].dt.strftime('%Y-%m-%d')


    df["END_DATE"] = '12/31/9999'
    #df["END_DATE"] = pd.to_datetime(df["END_DATE"],errors = 'coerce')
    #df["END_DATE"] = df["END_DATE"].dt.strftime('%Y-%m-%d')


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

ALLJOBS_SUCCESS_FLAG = True

# Extract Data from Salesforce (TalentRover)
try:
    SOQL_STATEMENT = """SELECT Id, Account_Manager__c, 
KX_Action_Plan__c, 
KX_Active_Long_List_Count__c, 
Affiliate_Vendors__c, 
Primary_Job__c, 
Budgeted_Start_Date__c, 
TR1__Certification__c, 
KX_Challenges_External__c, 
KX_Challenges_Internal__c, 
TR1__City__c, 
TR1__Industry__c, 
Client_Req_Number__c, 
TR1__Closed_Date__c, 
TR1__Closed_Reason__c, 
Coach__c, 
TR1__Account__c, 
TR1__Account_Name__c, 
Compensation_Comments__c, 
TR1__Contact__c, 
KX_Count_1st_Interviews__c, 
CreatedById, 
CreatedDate, 
KX_Current_Long_List__c, 
Customer_Agreement__c, 
TR1__Days_Open__c, 
IsDeleted, 
Department_Name__c, 
Department_Number__c, 
Education__c, 
TR1__Estimated_End_Date__c, 
TR1__Estimated_Start_Date__c, 
Fee_Amount__c, 
Fee_Tier__c, 
FLSA__c, 
FTE__c, 
TR1__Hiring_Manager__c, 
Intake_Completed__c, 
Intake_Completed_Date__c, 
Intake_Completed_Timeline__c, 
Internal_Role__c, 
Invoice_Paid__c, 
Invoice_Paid_Date__c, 
Job_Family__c, 
Name, 
TR1__Job_Number__c, 
TR1__Job_Owner__c, 
Job_Stage_Text__c, 
LastActivityDate, 
LastModifiedById, 
LastModifiedDate, 
TR1__Level__c, 
TR1__Maximum_Pay_Rate__c, 
TR1__Minimum_Pay_Rate__c, 
TR1__Open_Date__c, 
OwnerId, 
Paygrade__c, 
Pipeline_Job__c, 
PO_Number__c, 
Point_Value__c, 
TR1__Post_Externally__c, 
Primary_Job_Req__c, 
Primary_Secondary__c, 
Project__c, 
Record_Type_Name__c, 
TR1__Regional_Area__c, 
TR1__Salary_Low__c, 
TR1__Salary_High__c, 
Shift_Information__c, 
TR1__State_Area__c, 
TR1__Status__c, 
Targeted_Billing_Date__c, 
TR1__Total_Offers2__c, 
TR1__Total_Submittals__c, 
Variable_Project_FTE__c 

FROM TR1__Job__c
"""


    df_alljobs = run_api_call(SOQL_STATEMENT)

    logging.info(df_alljobs.shape)
    logging.info(df_alljobs.columns)
    logging.info("Successfully pulled jobs from API: %s",str(df_alljobs.shape))


except Exception as ex:
    logging.warning("Issue with loading in All Jobs from API")
    logging.warning(ex)
    ALLJOBS_SUCCESS_FLAG = False


# Transform Data
if ALLJOBS_SUCCESS_FLAG is True:
    try:

        df_alljobs = transform_data(df_alljobs)

        logging.info(
            "Loaded in all jobs from API and transformed columns...: %s",str(df_alljobs.shape))
        logging.info(df_alljobs.columns)
        
    except Exception as ex:
        logging.warning("Issue transforming data")
        logging.warning(ex)
        ALLJOBS_SUCCESS_FLAG = False

# Load Data into the Azure SQL Server db
if ALLJOBS_SUCCESS_FLAG == True:
    try: 
        df_alljobs.to_sql('dw2_jobs', con=engine, index=False)
        logging.info("Done loading all jobs")
        ALLJOBS_SUCCESS_FLAG = True

    except Exception as ex:
        logging.warning("Issue loading in all jobs to SQL Server")
        conn.rollback()
        logging.info("Rollback successful")
        logging.warning(ex)
        ALLJOBS_SUCCESS_FLAG = False

if ALLJOBS_SUCCESS_FLAG == True:
    logging.info("Done loading all data into alljobs table successfully, exiting script.")
else:
    logging.warning("Something went wrong with loading alljobs, script exiting.")

