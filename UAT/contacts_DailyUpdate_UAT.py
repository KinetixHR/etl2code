import pandas as pd
from simple_salesforce import Salesforce
import logging
import requests
import sqlalchemy
import urllib
import datetime as dt
import os
from sqlalchemy import create_engine, MetaData, Table, Column, update, String, text
from sqlalchemy.orm import sessionmaker
import time

# Configute Logging
logging.basicConfig(filename='./etl2code/logs/contacts_update_logging.log', level=logging.INFO,
                    format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")


SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"
constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))
engine = sqlalchemy.create_engine(constring,echo=False)

def run_api_call(statement):
    """
    This function runs the API call to Salesforce and returns a dataframe of the results.
    """
    session = requests.Session()
    # Setting up salesforce functionality
    sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session)

    # generator on the results page
    fetch_results = sf.bulk.Contact.query_all(
        statement, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
    result_df = pd.DataFrame(all_results)
    
    try:
        result_df = result_df.drop(columns=['attributes'])
    except:
        logging.warning("No ATTRIBUTES column in results")
    
    return result_df

def transform_data(df):
    """
    Function to transform data in DF along pre-determined lines.
    """
    logging.info("Starting changing column names...")

    df.columns = ['ACTIVE',
            'CALL_LIST_COUNT', 
            'CANDIDATE_SOURCE', 
            'CANDIDATE_STATUS', 
            'COMMUNITY_CONTACT_TYPE', 
            'CONTACT_ID', 
            'CONTACT_OWNER', 
            'CONTACT_RECORD_TYPE_ID', 
            'CONTACT_SOURCE', 
            'CREATED_BY', 
            'CREATED_DATE', 
            'CURRENT_INCOME', 
            'DEFAULT_PHONE', 
            'DESIRED_INCOME', 
            'DO_NOT_CALL', 
            'EMAIL', 
            'EMAIL_BOUNCED_REASON', 
            'EMAIL_OPT_OUT', 
            'FULL_NAME', 
            'FUNCTION', 
            'HOME_ADDRESS', 
            'HOME_PHONE', 
            'INDUSTRY_EXPERIENCE', 
            'INTERNAL_EXTERNAL', 
            'LAST_EMAIL', 
            'LAST_MODIFIED_BY', 
            'LAST_MODIFIED_DATE', 
            'LAST_PLACEMENT_DATE', 
            'MOBILE_PHONE', 
            'NAME', 
            'OWNER_NAME', 
            'PHONE', 
            'PRIMARY_BACKGROUND', 
            'RECENT_RESUME_VERSION', 
            'REGIONAL_AREA', 
            'SOURCE', 
            'STATE_AREA', 
            'SUBJECTS_JOB_TITLE', 
            'TITLE', 
            'COMPANY_ID']
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

def to_sql_implementation():
        try:
            logging.info(f"Starting to append {df_contacts.shape[0]} rows into DB")
            df_contacts.to_sql('dw2_contacts',con = engine, index = False, if_exists='append', chunksize = 10000)
            logging.info(f"appended {df_contacts.shape[0]} rows into DB")
        
        except Exception as e:
            logging.warning("Contacts update to SQL Server did not work")
            logging.warning(str(e))

def remove_ids_from_db(id_list):
    metadata = MetaData()

    # Reflect the table
    your_table = Table('dw2_contacts', metadata, autoload_with=engine)

    # Define the list of IDs to be removed
    ids_to_remove = id_list
    with engine.connect() as connection:
        delete_query = your_table.delete().where(your_table.c.CONTACT_ID.in_(ids_to_remove))
        connection.execute(delete_query)

    logging.info(f"Rows related to the {len(ids_to_remove)} IDs detedcted today deleted successfully.")

# Define dates, database details, and finally connect to DB
date_format = "%m/%d/%Y"
soql_today = dt.datetime.strptime(gen_date(),date_format).strftime("%Y-%m-%d")
soql_yesterday = dt.datetime.strptime(gen_date(-1),date_format).strftime("%Y-%m-%d")


SERVER = "kinetixsql.database.windows.net"
DATABASE = "KinetixSQL"
USERNAME = "awhelan"
PASSWORD = "5uj7*ZpE8Y$D"

constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))
logging.info("Connecting to Azure DB")
engine = sqlalchemy.create_engine(constring,echo=False)
conn = engine.connect()
logging.info("CONNECTED to Azure DB")



# Extract / Transform
try:
    logging.info(f"Calling api and transforming data")
    SOQL_STATEMENT = f"""SELECT Active__c,
        TR1__Call_List_Count__c, 
        Candidate_Source__c, 
        TR1__Candidate_Status__c, 
        Community_Contact_Type__c, 
        Id, 
        OwnerId, 
        RecordTypeId, 
        Contact_Source__c, 
        CreatedById, 
        CreatedDate, 
        Current_Income__c, 
        KX_Default_Phone__c, 
        Desired_Income__c, 
        DoNotCall, 
        Email, 
        EmailBouncedReason, 
        HasOptedOutOfEmail, 
        KX_Full_Name_c__c, 
        TR1__Function__c, 
        Home_address_2__c, 
        HomePhone, 
        TR1__Industry_Experience__c, 
        Internal_External__c, 
        Last_Email__c, 
        LastModifiedById, 
        LastModifiedDate, 
        TR1__Last_Placement_Date__c, 
        MobilePhone, 
        Name, 
        TR1__Owner_Name__c, 
        Phone, 
        TR1__Primary_Background__c, 
        TR1__Recent_Resume_Version__c, 
        TR1__Regional_Area__c, 
        TR1__Source__c, 
        TR1__State_Area__c, 
        TR1__Subjects_Job_Titles__c, 
        Title, 
        AccountId 
        
        FROM Contact
        
        WHERE (CreatedDate > {soql_yesterday}T05:00:00Z) AND (CreatedDate < {soql_today}T05:00:00Z)"""
    
    df_contacts= run_api_call(SOQL_STATEMENT)
    df_contacts = transform_data(df_contacts)
    logging.info(f"successfully called api and transformed data")

except Exception as e:
    logging.warning("Something went wrong with calling API for today's contacts info")
    logging.critical(e)

# Remove old info and Load
try:
    logging.info(f"Making updates to dw2contacts table for today's contacts")
    logging.info(f"Planning to remove rows related to {len(df_contacts['CONTACT_ID'].to_list())} ID's")
    remove_ids_from_db(df_contacts["CONTACT_ID"].to_list())
    logging.info("old data removed from DB, next up: adding in today's info")
    to_sql_implementation()
    logging.info("Script completed successfully")

except Exception as e:
    logging.warning("Something went wrong with updating the database")
    logging.critical(e)