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

import logging
logging.basicConfig(filename='./etl2code/logs/UAT_Logs/Placements_Loading_SANDBOX_Log.log', level=logging.INFO,
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
    sf = Salesforce(password='Kinetix2Password', username='salesforceapps@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session)
    
    fetch_results = sf.bulk.TR1__Closing_Report__c.query_all(query_statement, lazy_operation=True)

    # Read results into DataFrame
    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
        df = pd.DataFrame(all_results)
    try:
        df = df.drop(columns=['attributes'])
    except:
        logging.warning("Atribute not in API result.")
        
    logging.info(df)
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
        logging.info( set(existing_dataframe.columns) - set(new_dataframe.columns) )
        return None

    if set(new_dataframe.columns) != set(existing_dataframe.columns):
        logging.warning("WARNING! This comparison is failing due to dissimlar dataframe sizes")
        logging.info( set(new_dataframe.columns) - set(existing_dataframe.columns))
        logging.info( set(existing_dataframe.columns) - set(new_dataframe.columns) )
        return None

    #logging.info(new_dataframe.shape)
    #logging.info(existing_dataframe.shape)
    new_dataframe_last_modified_info = new_dataframe[["PLACEMENT_ID","LAST_MODIFIED_BY_ID","LAST_MODIFIED_DATE_DT","LAST_MODIFIED_DATE_TS"]]
    new_dataframe = new_dataframe.drop(columns =["LAST_MODIFIED_BY_ID","LAST_MODIFIED_DATE_DT","LAST_MODIFIED_DATE_TS"])
    existing_dataframe = existing_dataframe.drop(columns =["LAST_MODIFIED_BY_ID","LAST_MODIFIED_DATE_DT","LAST_MODIFIED_DATE_TS"])
    
    #logging.info(new_dataframe.shape)
    #logging.info(existing_dataframe.shape)

    # Loop over reqs
    for el in req_list_:
        
        df_new_data = new_dataframe[new_dataframe["PLACEMENT_ID"] == el]
        df_new_data = df_new_data.fillna(value = numpy.nan)
        df_old_data = existing_dataframe[existing_dataframe["PLACEMENT_ID"] == el]
        df_old_data = df_old_data.fillna(value = numpy.nan)

        df_new_string = df_new_data.to_string(header = False,index = False, index_names = False)
        df_old_string = df_old_data.to_string(header = False,index = False, index_names = False)
        no_change_list = []
        if df_old_data.shape[0] != 0:
            if df_new_string != df_old_string:
                logging.info([f"Change detected in Job with ID: {el}"])
                logging.info(["API: ",df_new_string])
                logging.info(["DW2: ",df_old_string])
                cols_list = df_new_data.columns

                for col in cols_list:
                    if df_new_data[col].values[0] != df_old_data[col].values[0]:
                        if (pd.isna(df_new_data[col].values[0]) == True):

                            if (pd.isna(df_old_data[col].values[0]) == True):
                                pass
                            else:
                                if "DATE" in col:
                                    logging.info(f"Found possible discrepancy in {col} with old info {df_old_data[col].values} and new info {df_new_data[col].values}")
                                else:
                                    logging.info(f"Found discrepancy in {col} with old info {df_old_data[col].values} and new info {df_new_data[col].values}")
                        else:
                            if "DATE" in col:
                                logging.info(f"Found possible discrepancy in {col} with old info {df_old_data[col].values} and new info {df_new_data[col].values}")
                            else:
                                logging.info(f"Found discrepancy in {col} with old info {df_old_data[col].values} and new info {df_new_data[col].values}")
        else:
            logging.info(f"{el} is likely a new req")
            pass
        if df_new_string != df_old_string:
            Ids_to_update.append(el)
        
        else:
            no_change_counter += 1
            no_change_list.append(el)

    logging.info(f"NUMBER OF ROWS WITHOUT CHANGES: {len(req_list_) - len(Ids_to_update)}")
    logging.info(f"No Change List: {no_change_list}")
    logging.info(f"AMOUNT OF IDS TO UPDATE: {len(Ids_to_update)}")
    
    return new_dataframe[new_dataframe["PLACEMENT_ID"].isin(Ids_to_update)]

def req_list_generator(dframe_):
    """
    This may not be necessary....
    """
    #print(dframe_.columns)
    req_list__ = dframe_["PLACEMENT_ID"].tolist()
    logging.info(req_list__)
    return req_list__
    
    req_list__ = str(req_list__)
    req_list__ = req_list.replace('[','(')
    req_list__ = req_list.replace(']',')')
    return req_list__

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
        'BILLING_DATE',
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
        'TIMESHEET_TYPE',
        "IS_DELETED"]
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
        df["EFFECTIVE_DATE"] = pd.Timestamp.today()
        df["EFFECTIVE_DATE"] = df["EFFECTIVE_DATE"].dt.strftime('%m-%d-%Y')
        df["END_DATE"] = '9999-12-31'
        #df["END_DATE"] = '12/31/9999'
        #df["END_DATE"] = pd.to_datetime(df["END_DATE"], errors = "ignore")
        #df["END_DATE"] = df["END_DATE"].dt.strftime('%m-%d-%Y')
        #df["END_DATE"] = df["END_DATE"].dt.strftime('%Y-%m-%d')
        logging.info("Done with Eff and End Date transformations")
    
    except Exception as e:
        logging.warning("Eff and or End date transformations failed.")
        logging.warning(e)

    logging.info("Done with date transformations...")

    logging.info("Starting other transformations...")
    #df = df.fillna("")
    logging.info("done with other transformations...")

    
    return df

def transform_data_dw2(df):
    """
    Function to transform data in DF along pre-determined lines.
    """
    logging.info("Starting changing column names...")
    logging.info(df.columns)
    df.columns = ['ACCOUNT_ID', 'ACTUAL_FEE_PERCENTAGE',
       'ACTUAL_INVOICE_AMOUNT_RPO_RETAINED', 'ADJUSTED_BILL_RATE',
       'ADJUSTED_FEE_AMOUNT_DH_CONV', 'ADJUSTMENT_AMOUNT', 'ADJUSTMENT_REASON',
       'AFFILIATE_VENDOR', 'APPROVED_FOR_BILLING', 'BILLING_TERM',
       'CANDIDATE_CREDIT', 'CANDIDATE_FULL_NAME', 'CANDIDATE_GP_AMOUNT',
       'RETAINED_CANDIDATE_GP_AMOUNT', 'CANDIDATE_PERCENTAGE',
       'CANDIDATE_POINTS', 'COMPANY', 'CONTACT', 'CREATED_BY_ID', 'FALLOFF',
       'FALLOFF_REASON', 'FEE_AMOUNT', 'FEE_TIER', 'FLSA',
       'GP_ITEM_DESCRIPTION', 'GROSS_FEE_PERCENTAGE', 'GROSS_INVOICE_AMOUNT',
       'ITEM_NUMBER', 'JOB', 'LAST_MODIFIED_BY_ID',
       'NUMBER_OF_COMMISSION_RECORDS', 'OFFER_ACCEPT_TO_BILLING_DATE',
       'OT_BILL_RATE_MULTIPLIER_PERCENTAGE', 'OT_PAY_RATE', 'PAY_RATE',
       'PERSON_PLACED', 'PERSON_PLACED_ID', 'PERSON_PLACED_SUBMIT_DATE',
       'PLACEMENT', 'PLACEMENT_ID', 'RECORD_TYPE', 'RECRUITER_CREDIT',
       'RECRUITER_GP_AMOUNT', 'RETAINED_RECRUITER_GP_AMOUNT',
       'RECRUITER_PERCENTAGE', 'RECRUITER_POINTS', 'RPO_PLACEMENT_FEE_AMOUNT',
       'SALARY', 'SALARY_OFFERED', 'BILL_RATE', 'BILLING_DATE',
       'BILLING_NOTES', 'COMMISSIONABLE_DATE', 'END_DATE', 'ASSIGNMENT_ENDED',
       'CALCULATE_CONTRACTOR_COMMISSIONS', 'ESTIMATED_END_DATE', 'MARKUP',
       'OT_BILL_RATE', 'PO_NUMBER', 'POINT_VALUE', 'TIMESHEET_APPROVER',
       'TIMESHEET_TYPE', 'IS_DELETED', 'CREATED_DATE_DT', 'CREATED_DATE_TS',
       'LAST_MODIFIED_DATE_DT', 'LAST_MODIFIED_DATE_TS', 'EFFECTIVE_DATE']
    logging.info("Done with changing column names...")

    logging.info("Starting date transformations...")

    
    #df["CREATED_DATE"] = pd.to_datetime(df['CREATED_DATE'], unit='ms')
    #df['CREATED_DATE_CONVERTED'] = df["CREATED_DATE"].dt.tz_localize(
    #    'UTC').dt.tz_convert('US/Eastern')
    #df["CREATED_DATE_DT"] = df["CREATED_DATE_CONVERTED"].dt.strftime(
    #    '%Y-%m-%d')
    #df["CREATED_DATE_TS"] = df["CREATED_DATE_CONVERTED"].dt.strftime(
    #    '%Y-%m-%d %H:%M:%S')
    #df.drop(columns=['CREATED_DATE_CONVERTED', 'CREATED_DATE'], inplace=True)

    #df["LAST_MODIFIED_DATE"] = pd.to_datetime(
    #    df["LAST_MODIFIED_DATE"], unit='ms')
    #df["LAST_MODIFIED_DATE_CONVERTED"] = df["LAST_MODIFIED_DATE"].dt.tz_localize(
    #    'UTC').dt.tz_convert('US/Eastern')
    #df["LAST_MODIFIED_DATE_DT"] = df["LAST_MODIFIED_DATE_CONVERTED"].dt.strftime(
    #    '%Y-%m-%d')
    #df["LAST_MODIFIED_DATE_TS"] = df["LAST_MODIFIED_DATE_CONVERTED"].dt.strftime(
    #    '%Y-%m-%d %H:%M:%S')
    #df.drop(columns=["LAST_MODIFIED_DATE_CONVERTED",
    #        "LAST_MODIFIED_DATE"], inplace=True)

    
    #df["LAST_LOGIN_DATE"] = pd.to_datetime(df["LAST_LOGIN_DATE"])
    #df["LAST_LOGIN_DATE"] = df["LAST_LOGIN_DATE"].dt.strftime('%Y-%m-%d')
    

    logging.info("Starting Effective Date and End Date transformations")
    try:
        df["EFFECTIVE_DATE"] = pd.Timestamp.today()
        df["EFFECTIVE_DATE"] = df["EFFECTIVE_DATE"].dt.strftime('%m-%d-%Y')
        df["END_DATE"] = '9999-12-31'
        #df["END_DATE"] = '12/31/9999'
        #df["END_DATE"] = pd.to_datetime(df["END_DATE"], errors = "ignore")
        #df["END_DATE"] = df["END_DATE"].dt.strftime('%m-%d-%Y')
        #df["END_DATE"] = df["END_DATE"].dt.strftime('%Y-%m-%d')
        logging.info("Done with Eff and End Date transformations")
    
    except Exception as e:
        logging.warning("Eff and or End date transformations failed.")
        logging.warning(e)

    logging.info("Done with date transformations...")

    logging.info("Starting other transformations...")
    #df = df.fillna("")
    logging.info("done with other transformations...")

    
    return df


# Define dates for SOQL update statement, so we can just look
# at reqs that have modfified dates in the past day.
today = gen_date()
yesterday = gen_date(-1)

date_format = "%m/%d/%Y"
soql_today = dt.datetime.strptime(gen_date(),date_format).strftime("%Y-%m-%d")
soql_yesterday = dt.datetime.strptime(gen_date(-1),date_format).strftime("%Y-%m-%d")
logging.info(f"Dates for SOQL update statement {(today,yesterday)},{(soql_today,soql_yesterday)}")

SOQL_STATEMENT = f"""SELECT Account_ID__c,
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
        TR1__Timesheet_Type__c,
        IsDeleted
        
        FROM TR1__Closing_Report__c 
        
        WHERE ((LastModifiedDate > {soql_yesterday}T05:00:00Z AND LastModifiedDate < {soql_today}T05:00:00Z) OR (CreatedDate > {soql_yesterday}T05:00:00Z AND CreatedDate < {soql_today}T05:00:00Z))"""


# Grab updated req information from salesforce
df_updates = query_jobs_object(SOQL_STATEMENT)
try:
    df_updates = transform_data(df_updates)
    logging.info(df_updates.shape)
except:
    logging.info("No updated data in user's file.")
    exit()


#Get data from SQL Server
SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"
constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))
engine = sqlalchemy.create_engine(constring,echo=False)

logging.info("Starting to grab placements from SQL Server...")
df_existing = pd.read_sql("SELECT * FROM dbo.dw2_placements WHERE END_DATE = '9999-12-31'",con = engine)
df_existing = transform_data_dw2(df_existing)
logging.info(df_existing.shape)

# Now that we have both dataframes, we can compare them using compare_and_find_updated_reqs()
logging.info("Starting Comparison")
req_list = req_list_generator(df_updates)
updated_req_df = compare_and_find_updated_reqs(req_list,df_updates,df_existing)
logging.info(updated_req_df.shape)

# making further modifications to the resulting dataframe
# (this dataframe includes JUST the reqs that need to be inserted)
#updated_req_df["EFFECTIVE_DATE"] = gen_date()
#updated_req_df["EFFECTIVE_DATE"] = pd.to_datetime(updated_req_df["EFFECTIVE_DATE"],errors = 'coerce')
updated_req_df["END_DATE"] = '9999-12-31'
updated_req_df["EFFECTIVE_DATE"] = gen_date()
updated_req_df["EFFECTIVE_DATE"] = pd.to_datetime(updated_req_df["EFFECTIVE_DATE"],errors = 'coerce')
updated_req_df.to_csv(f"~/etl2code/Daily Extracts/Placements daily file for {gen_date().replace('/','_')}.csv")
# END_DATE in Azure needs to be backdated, this code handles that
date_to_update = dt.datetime.now()-dt.timedelta(1)
date_to_update = str(date_to_update.strftime("%Y-%m-%d"))

#for col in updated_req_df.columns:
#    if "OFFER" in col:
#       logging.info([col,updated_req_df[col].values])

# Here is the big chunk of code to interact with the Azure SQL Server DB
# Define the table and its columns
metadata = MetaData()
your_table = Table('dw2_placements', metadata,
                Column('PLACEMENT_ID', String(60), primary_key=True),
                Column('END_DATE', String(60)))

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# List of IDs to update
ids_to_update = updated_req_df["PLACEMENT_ID"].to_list()

# New value to set for the 'column_to_update'
new_value = date_to_update

# Construct an update statement
update_statement = update(your_table).where(your_table.c.PLACEMENT_ID.in_(ids_to_update)).values(END_DATE=new_value)

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
    logging.info("Done updating existing placements in database")


# Now that we've updated the rows
# we are going to append the new information now. 
logging.info("Adding new and updated records to DB")
try:
    logging.info(list(updated_req_df.columns))
    if 'index' in updated_req_df.columns:
        updated_req_df = updated_req_df.drop(columns = ["index"])
    
    updated_req_df.to_sql('dw2_placements',con = engine, if_exists = 'append', index= False)
    logging.info("Done adding new and updated records to database.")

except Exception as e:
    logging.warning(f"Adding new and updated placements into DB !FAILED! {str(e)}")

