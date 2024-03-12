#
#
# Only run this if you want to blow the placements database away and start fresh. 
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
logging.basicConfig(filename='placements_setup_logging.log', level=logging.INFO,
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
    fetch_results = sf.bulk.TR1__Closing_Report__c.query_all(
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

    df.columns = ['ACCOUNT_ID',
        'ACTUAL_FEE_PERCENTAGE',
        'ACTUAL_INVOICE_AMOUNT_RPO_RETAINED',
        'ADJUSTED_BILL_RATE',
        'ADJUSTED_FEE_AMOUNT_DH_CONV',
        'ADJUSTMENT_AMOUNT',
        'ADJUSTMENT_REASON',
        'AFFILIATE_VENDOR',
        "APPROVED_FOR_BILLING",
        'BILLING_TERM',
        'CANDIDATE_CREDIT',
        'CANDIDATE_FULL_NAME',
        'CANDIDATE_GP_AMOUNT',
        'RETAINED_CANDIDATE_GP_AMOUNT',
        'CANDIDATE_PERCENTAGE',
        'CANDIDATE_POINTS',
        'COMPANY',
        'CONTACT',
        'CREATED_BY_ID',
        'CREATED_DATE',
        'FALLOFF',
        'FALLOFF_REASON',
        'FEE_AMOUNT',
        'FEE_TIER',
        'FLSA',
        'GP_ITEM_DESCRIPTION',
        'GROSS_FEE_PERCENTAGE',
        'GROSS_INVOICE_AMOUNT',
        'ITEM_NUMBER',
        'JOB',
        'LAST_MODIFIED_BY_ID',
        'LAST_MODIFIED_DATE',
        'NUMBER_OF_COMMISSION_RECORDS',
        'OFFER_ACCEPT_TO_BILLING_DATE',
        'OT_BILL_RATE_MULTIPLIER_PERCENTAGE',
        'OT_PAY_RATE',
        'PAY_RATE',
        'PERSON_PLACED',
        'PERSON_PLACED_ID',
        'PERSON_PLACED_SUBMIT_DATE',
        'PLACEMENT',
        'PLACEMENT_ID',
        'RECORD_TYPE',
        'RECRUITER_CREDIT',
        'RECRUITER_GP_AMOUNT',
        'RETAINED_RECRUITER_GP_AMOUNT',
        'RECRUITER_PERCENTAGE',
        'RECRUITER_POINTS',
        'RPO_PLACEMENT_FEE_AMOUNT',
        'SALARY',
        'SALARY_OFFERED',
        'BILL_RATE',
        'BOOKING_DATE',
        'BILLING_NOTES',
        'COMMISSIONABLE_DATE',
        'END_DATE',
        'ASSIGNMENT_ENDED',
        'CALCULATE_CONTRACTOR_COMMISSIONS',
        'ESTIMATED_END_DATE',
        'MARKUP',
        'OT_BILL_RATE',
        'PO_NUMBER',
        'POINT_VALUE',
        'TIMESHEET_APPROVER',
        'TIMESHEET_TYPE']
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

    
    #df["LAST_LOGIN_DATE"] = pd.to_datetime(df["LAST_LOGIN_DATE"])
    
    #df["LAST_LOGIN_DATE"] = df["LAST_LOGIN_DATE"].dt.strftime('%Y-%m-%d')
    

    logging.info("Starting Effective Date and End Date transformations")
    try:
        df["EFFECTIVE_DATE"] = gen_date()
        df["END_DATE"] = '12/31/9999'
        #df["END_DATE"] = pd.to_datetime(df["END_DATE"])
        #df["END_DATE"] = df["END_DATE"].dt.strftime('%Y-%m-%d')
        logging.info("Done with Eff and End Date transformations")
    
    except Exception as e:
        logging.warning("Eff and or End date transformations failed.")
        logging.warning(e)

    logging.info("Done with date transformations...")

    logging.info("Starting other transformations...")
    df = df.fillna("")
    logging.info("done with other transformations...")

    
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

PLACEMENTS_SUCCESS_FLAG = True

# Extract Data from Salesforce (TalentRover)
try:
    SOQL_STATEMENT = """SELECT Account_ID__c,
        TR1__Fee_Percentage__c,
        TR1__Retained_Invoice_Amount__c,
        TR1__Adjusted_Bill_Rate__c,
        TR1__Adjusted_Fee_Amount__c,
        TR1__Discount_Amount__c,
        TR1__Discount_Reason__c,
        Affiliate_Vendor__c,
        Approved_for_Billing__c,
        Billing_Termlu__c,
        TR1__Candidate_Credit__c,
        KX_Candidate_Full_Name_u__c,
        TR1__Candidate_GP_Amount__c,
        TR1__Retained_Candidate_GP_Amount__c,
        TR1__Candidate_Percentage__c,
        Candidate_Points__c,
        TR1__Account__c,
        TR1__Contact__c,
        CreatedById,
        CreatedDate,
        Falloff__c,
        Fall_Off_Reason__c,
        TR1__Fee_Amount__c,
        Fee_Tier__c,
        FLSA__c,
        GP_Item_Description__c,
        Gross_Fee_Percentage__c,
        Gross_Invoice_Amount__c,
        Item_Number__c,
        TR1__Job__c,
        LastModifiedById,
        LastModifiedDate,
        Number_of_Commission_Records__c,
        Offer_Accept_to_Placement__c,
        TR1__OT_Bill_Rate_Multiplier_Percentage__c,
        TR1__OT_Pay_Rate__c,
        TR1__Pay_Rate__c,
        TR1__Person_Placed__c,
        KX_PersonPlacedID__c,
        KX_PersonPlacedSubmitDate__c,
        Name,
        Id,
        RecordTypeId,
        TR1__Job_Credit__c,
        TR1__Job_GP_Amount__c,
        TR1__Retained_Job_GP_Amount__c,
        TR1__Job_Percentage__c,
        Recruiter_Points__c,
        TR1__Retained_Fee_Amount__c,
        TR1__Salary__c,
        Offer_Salary__c,
        TR1__Bill_Rate__c,
        TR1__Booking_Date__c,
        Billing_Notes__c,
        Commissionable_Date__c,
        TR1__End_Date__c,
        TR1__Assignment_Ended__c,
        Calculate_Contractor_Commissions__c,
        Estimated_End_Date__c,
        TR1__Markup__c,
        TR1__OT_Bill_Rate__c,
        TR1__PO_Number__c,
        Point_Value__c,
        TR1__Timesheet_Approver__c,
        TR1__Timesheet_Type__c
        
        FROM TR1__Closing_Report__c 
        """


    df_placements = run_api_call(SOQL_STATEMENT)

    logging.info(df_placements.shape)
    logging.info(df_placements.columns)
    logging.info("Successfully pulled placements from API: %s",str(df_placements.shape))


except Exception as ex:
    logging.warning("Issue with loading in placements from API")
    logging.warning(ex)
    PLACEMENTS_SUCCESS_FLAG = False


# Transform Data
if PLACEMENTS_SUCCESS_FLAG is True:
    try:

        df_alljobs = transform_data(df_placements)

        logging.info(
            "Loaded in placements from API and transformed columns...: %s",str(df_placements.shape))
        logging.info(df_placements.columns)
        
    except Exception as ex:
        logging.warning("Issue transforming data")
        logging.warning(ex)
        PLACEMENTS_SUCCESS_FLAG = False

if PLACEMENTS_SUCCESS_FLAG == True:
    try: 

        df_placements.to_sql('dw2_placements', con=engine, if_exists='replace', index=False)
        logging.info("Done loading placements")
        PLACEMENTS_SUCCESS_FLAG = True


    except Exception as ex:
        logging.warning("Issue loading in placements to SQL Server")
        conn.rollback()
        logging.info("Rollback successful")
        logging.warning(ex)
        PLACEMENTS_SUCCESS_FLAG = False

if PLACEMENTS_SUCCESS_FLAG == True:
    logging.info("Done loading all data into placements table successfully, exiting script.")
else:
    logging.warning("Something went wrong with loading placements, script exiting.")

