import pandas as pd
import os, uuid
from io import StringIO
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import date
import pyodbc
import urllib
import sqlalchemy
from simple_salesforce import Salesforce
import requests
import pandas as pd
import datetime as dt
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, Column, update, String
from sqlalchemy.orm import sessionmaker
import numpy

# Configure Logging
import logging
logging.basicConfig(filename='./etl2code/logs/UAT_Logs/UAT_Jobs_Loading_SANDBOX_Log.log', level=logging.INFO,
                    format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")



def gen_date(offset = 0):

    """
    This may not be necessary...
    """
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

def query_jobs_object(query_statement):
     # Setting up salesforce functionality
     session = requests.Session()
     sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session)

     fetch_results = sf.bulk.TR1__Job__c.query_all(query_statement, lazy_operation=True)

     # Read results into DataFrame
     all_results = []
     for list_results in fetch_results:
        all_results.extend(list_results)
     df = pd.DataFrame(all_results)
     try:
        df = df.drop(columns=['attributes'])
     except:
        print("")

     return df

def compare_and_find_updated_reqs(req_list_,new_dataframe,existing_dataframe):
    """
    This is a beast of a function. It takes a list of requisitions to search for.
    Then it looks at a dataframe of just the reqs that have updated info (new_dataframe)
    Then it looks at existing dataframe info (existing_dataframe) which is what lives in teh 
    Azure SQL Server.

    First, a check is made that the two DFs have the same amount of columns. If not,
    failure is immenent. 
    
    To compare the data, we loop over the req_list_, turning each line of data into a string.
    Once we have our two strings, we compare their lengths. If the lengths are
    SIGNIFICANTLY different, then we count that as a legit change and send that new row
    into a resultuing dataframe for insertion and updating operations in the Azure DB

    """

    # first define some variables
    Ids_to_update = []
    no_change_counter = 0

    # Do checks 
    logging.info(f"Is there a difference between two column sets?{set(new_dataframe.columns) - set(existing_dataframe.columns)}")
    
    if set(set(new_dataframe.columns) - set(existing_dataframe.columns)) != set():
        logging.warning("WARNING! This comparison is failing due to dissimlar dataframe sizes")
        logging.info( set(new_dataframe.columns) - set(existing_dataframe.columns))
        return None

    if set(new_dataframe.columns) != set(existing_dataframe.columns):
        logging.warning("WARNING! This comparison is failing due to dissimlar dataframe sizes")
        logging.info( set(new_dataframe.columns) - set(existing_dataframe.columns))
        return None

    #logging.info(new_dataframe.shape)
    #logging.info(existing_dataframe.shape)
    new_dataframe_last_modified_info = new_dataframe[["JOB_ID","LAST_MODIFIED_BY_ID","LAST_MODIFIED_DATE_DT","LAST_MODIFIED_DATE_TS"]]
    new_dataframe = new_dataframe.drop(columns =["LAST_MODIFIED_BY_ID","LAST_MODIFIED_DATE_DT","LAST_MODIFIED_DATE_TS"])
    existing_dataframe = existing_dataframe.drop(columns =["LAST_MODIFIED_BY_ID","LAST_MODIFIED_DATE_DT","LAST_MODIFIED_DATE_TS"])
    
    #logging.info(new_dataframe.shape)
    #logging.info(existing_dataframe.shape)
    
    # Loop over reqs
    for el in req_list_:
        
        df_new_data = new_dataframe[new_dataframe["JOB_ID"] == el]
        df_new_data = df_new_data.fillna(value = numpy.nan)
        df_old_data = existing_dataframe[existing_dataframe["JOB_ID"] == el]
        df_old_data = df_old_data.fillna(value = numpy.nan)


        df_new_string = df_new_data.to_string(header = False,index = False, index_names = False)
        df_old_string = df_old_data.to_string(header = False,index = False, index_names = False)

        logging.info("New, then Old strings:")
        logging.info(df_new_string)
        logging.info(df_old_string)
        
        if df_new_string != df_old_string:
            Ids_to_update.append(el)
        
        else:
            no_change_counter += 1
        

        #if abs(len(df_new_string) - len(df_new_string)) < 1:
        #    no_change_counter += 1
        
        #if abs(len(df_new_string) - len(df_old_string)) >= 1:
        #    Ids_to_update.append(el)

    logging.info(f"NUMBER OF ROWS WITHOUT CHANGES: {len(req_list_) - len(Ids_to_update)}")
    logging.info(f"AMOUNT OF IDS TO UPDATE: {len(Ids_to_update)}")
    
    data_to_return =  new_dataframe[new_dataframe["JOB_ID"].isin(Ids_to_update)]

    #data_to_return.join(new_dataframe_last_modified_info, on = "JOB_ID", how = 'left')
    data_to_return = data_to_return.merge(new_dataframe_last_modified_info,on='JOB_ID')
    logging.info(data_to_return.columns)
    logging.info(data_to_return["LAST_MODIFIED_DATE_DT"])

    return data_to_return

def req_list_generator(dframe_):
    """
    This may not be necessary....
    """
    req_list__ = dframe_["JOB_ID"].tolist()
    logging.info(req_list__)
    return req_list__

def transform_data(df):
    """
    Function to transform data in DF along pre-determined lines.
    MAKE ANY FUTURE TRANSFORMATIONS HERE!
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
    df["INTAKE_COMPLETED_DATE"] = pd.to_datetime(df['INTAKE_COMPLETED_DATE'], unit='ms')
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
    df = df.fillna("")
    logging.info("done with other transformations...")
    logging.info(df["LAST_MODIFIED_DATE_TS"].dtype)

    df["EFFECTIVE_DATE"] = gen_date()

    df["BUDGETED_START_DATE"] = pd.to_datetime(df['BUDGETED_START_DATE'],errors='coerce')
    df["CLOSED_DATE"] = pd.to_datetime(df["CLOSED_DATE"],errors = 'coerce')
    #df["LAST_MODIFIED_DATE"] = pd.to_datetime(df['LAST_MODIFIED_DATE'], unit='ms',errors = 'coerce')
    #df["CREATED_DATE"] = pd.to_datetime(df['CREATED_DATE'], unit='ms',errors = 'coerce')
    df["LAST_ACTIVITY_DATE"] = pd.to_datetime(df["LAST_ACTIVITY_DATE"],errors = 'coerce')
    df["OPEN_DATE"] = pd.to_datetime(df["OPEN_DATE"],errors = 'coerce')
    df["EFFECTIVE_DATE"] = pd.to_datetime(df["EFFECTIVE_DATE"],errors = 'coerce')

    df["BUDGETED_START_DATE"] = df["BUDGETED_START_DATE"].dt.strftime('%Y-%m-%d')
    df["CLOSED_DATE"] = df["CLOSED_DATE"].dt.strftime('%Y-%m-%d')
    #df["LAST_MODIFIED_DATE"] = df["LAST_MODIFIED_DATE"].dt.strftime('%Y-%m-%d')
    #df["CREATED_DATE"] = df["CREATED_DATE"].dt.strftime('%Y-%m-%d')
    df['LAST_ACTIVITY_DATE'] = df["LAST_ACTIVITY_DATE"].dt.strftime('%Y-%m-%d')
    df['OPEN_DATE'] = df["OPEN_DATE"].dt.strftime('%Y-%m-%d')



    df["END_DATE"] = '12/31/9999'
    df["END_DATE"] = pd.to_datetime(df["END_DATE"],errors = 'coerce')
    df["END_DATE"] = df["END_DATE"].dt.strftime('%Y-%m-%d')

    for el in df.columns:
        if df[el].dtype == 'int64':
            df[el] = df[el].astype(float)
        if df[el].dtype == 'float64':
            df[el] = df[el].astype(float)
    return df

# Define dates for SOQL update statement, so we can just look
# at reqs that have modfified dates in the past day.

today = gen_date()
yesterday = gen_date(-1)

date_format = "%m/%d/%Y"
soql_today =dt.datetime.strptime(gen_date(),date_format).strftime("%Y-%m-%d")
soql_yesterday = dt.datetime.strptime(gen_date(-1),date_format).strftime("%Y-%m-%d")
soql_tomorrow = dt.datetime.strptime(gen_date(1),date_format).strftime("%Y-%m-%d")

logging.info(f"Dates for SOQL update statement {(today,yesterday)},{(soql_today,soql_yesterday)}")

SOQL_STATEMENT = f"""SELECT Id, Account_Manager__c, 
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

    WHERE Id = 'a0WUt000000dWwiMAE'"""



# Grab updated req information from salesforce
df_updates = query_jobs_object(SOQL_STATEMENT)
df_updates = transform_data(df_updates)
logging.info(df_updates.shape)

#Get data from SQL Server
SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"
constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))
engine = sqlalchemy.create_engine(constring,echo=False)

logging.info("Starting to grab all jobs from SQL Server...")
df_existing = pd.read_sql("SELECT * FROM dbo.dw2_jobs_sandbox WHERE END_DATE LIKE '%9999%'",con = engine)
logging.info(df_existing.shape)

# Now that we have both dataframes, we can compare them using compare_and_find_updated_reqs()
logging.info("Starting Comparison")
req_list = req_list_generator(df_updates)
updated_req_df = compare_and_find_updated_reqs(req_list,df_updates,df_existing)
logging.info(updated_req_df.shape)

# making further modifications to the resulting dataframe
# (this dataframe includes JUST the reqs that need to be inserted)
updated_req_df["END_DATE"] = '9999-12-31'
updated_req_df["EFFECTIVE_DATE"] = gen_date()
updated_req_df["EFFECTIVE_DATE"] = pd.to_datetime(updated_req_df["EFFECTIVE_DATE"],errors = 'coerce')
updated_req_df.to_csv(f"~/etl2code/Daily Extracts/Jobs daily file for {gen_date().replace('/','_')} - SANDBOX.csv")

# END_DATE in Azure needs to be backdated, this code handles that
date_to_update = dt.datetime.now()-dt.timedelta(1)
date_to_update = str(date_to_update.strftime("%Y-%m-%d"))



# Here is the big chunk of code to interact with the Azure SQL Server DB
# Define the table and its columns
metadata = MetaData()
your_table = Table('dw2_jobs_sandbox', metadata,
                Column('JOB_ID', String(60), primary_key=True),
                Column('END_DATE', String(60)))

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# List of IDs to update
ids_to_update = updated_req_df["JOB_ID"].to_list()

# New value to set for the 'column_to_update'
new_value = date_to_update

# Construct an update statement
update_statement = update(your_table).where( (your_table.c.JOB_ID.in_(ids_to_update)) & (your_table.c.END_DATE == "9999-12-31") ).values(END_DATE=new_value)
logging.info(update_statement)


# Execute the update statement
try:
    conn = engine.connect()
    result = conn.execute(update_statement)
    conn.commit()
    conn.close()

    session.commit()
    logging.info(f"Updated {result.rowcount} rows.")
    
except Exception as e:
    session.rollback()
    logging.warning(f"Error updating the rows: {str(e)}")
finally:
    session.close()
    logging.info("Done updating existing jobs in database")


# Now that we've updated the rows that will be effected
# We are going to append the new information now. 
logging.info("Adding new and updated records to DB")
logging.info(f"Adding {updated_req_df.shape[0]} new and updated reqs...")
try:
    logging.info(list(updated_req_df.columns))
    if 'index' in updated_req_df.columns:
        updated_req_df = updated_req_df.drop(columns = ["index"])
    
    updated_req_df.to_sql('dw2_jobs_sandbox',con = engine, if_exists = 'append', index= False)
    logging.info("Done adding new and updated records to database.")

except Exception as e:
    logging.warning(f"Adding new and updated jobs into DB !FAILED! {str(e)}")

