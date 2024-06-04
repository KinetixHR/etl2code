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
logging.basicConfig(filename='./etl2code/logs/users_dailyupdates_logging.log', level=logging.INFO,
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

    fetch_results = sf.bulk.User.query_all(query_statement, lazy_operation=True)

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
        return None

    if set(new_dataframe.columns) != set(existing_dataframe.columns):
        logging.warning("WARNING! This comparison is failing due to dissimlar dataframe sizes")
        logging.info( set(new_dataframe.columns) - set(existing_dataframe.columns))
        return None

    # Loop over reqs
    for el in req_list_:
        
        df_new_data = new_dataframe[new_dataframe["USER_ID"] == el]
        df_old_data = existing_dataframe[existing_dataframe["USER_ID"] == el]


        #df_new_data = new_dataframe[new_dataframe["JOB_ID"] == el]
        df_new_data = df_new_data.fillna(value = numpy.nan)
        #df_old_data = existing_dataframe[existing_dataframe["JOB_ID"] == el]
        df_old_data = df_old_data.fillna(value = numpy.nan)

        df_new_string = df_new_data.to_string(header = False,index = False, index_names = False)
        df_old_string = df_old_data.to_string(header = False,index = False, index_names = False)

        if df_new_string != df_old_string:
            Ids_to_update.append(el)
        
        else:
            no_change_counter += 1

    logging.info(f"NUMBER OF ROWS WITHOUT CHANGES: {len(req_list_) - len(Ids_to_update)}")
    logging.info(f"AMOUNT OF IDS TO UPDATE: {len(Ids_to_update)}")
    
    return new_dataframe[new_dataframe["USER_ID"].isin(Ids_to_update)]

def req_list_generator(dframe_):
    """
    This may not be necessary....
    """
    req_list__ = dframe_["USER_ID"].tolist()
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
    'COACH_NAME'
    ]
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
        df["END_DATE"] = pd.to_datetime(df["END_DATE"], errors = 'ignore')
        #df["END_DATE"] = df["END_DATE"].dt.strftime('%Y-%m-%d')
        logging.info("Done with Eff and End Date transformations")
    
    except Exception as e:
        logging.warning("Eff and/or End date transformations failed.")
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

# Define dates for SOQL update statement, so we can just look
# at reqs that have modfified dates in the past day.
today = gen_date()
yesterday = gen_date(-1)

date_format = "%m/%d/%Y"
soql_today = dt.datetime.strptime(gen_date(),date_format).strftime("%Y-%m-%d")
soql_yesterday = dt.datetime.strptime(gen_date(-1),date_format).strftime("%Y-%m-%d")
logging.info(f"Dates for SOQL update statement {(today,yesterday)},{(soql_today,soql_yesterday)}")

SOQL_STATEMENT = SOQL_STATEMENT = f"""SELECT IsActive, 
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
    
    FROM User
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

logging.info("Starting to grab users from SQL Server...")
df_existing = pd.read_sql("SELECT * FROM dbo.dw2_users",con = engine)
logging.info(df_existing.shape)

# Now that we have both dataframes, we can compare them using compare_and_find_updated_reqs()
logging.info("Starting Comparison")
req_list = req_list_generator(df_updates)
updated_req_df = compare_and_find_updated_reqs(req_list,df_updates,df_existing)
logging.info(updated_req_df.shape)

# making further modifications to the resulting dataframe
# (this dataframe includes JUST the reqs that need to be inserted)
#updated_req_df["END_DATE"] = '9999-12-31'
#updated_req_df["EFFECTIVE_DATE"] = gen_date()
#updated_req_df["EFFECTIVE_DATE"] = pd.to_datetime(updated_req_df["EFFECTIVE_DATE"],errors = 'coerce')

# END_DATE in Azure needs to be backdated, this code handles that
date_to_update = dt.datetime.now()-dt.timedelta(1)
date_to_update = str(date_to_update.strftime("%Y-%m-%d"))



# Here is the big chunk of code to interact with the Azure SQL Server DB
# Define the table and its columns
metadata = MetaData()
your_table = Table('dw2_users', metadata,
                Column('USER_ID', String(60), primary_key=True),
                Column('END_DATE', String(60)))

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# List of IDs to update
ids_to_update = updated_req_df["USER_ID"].to_list()

# New value to set for the 'column_to_update'
new_value = date_to_update

# Construct an update statement
update_statement = update(your_table).where(your_table.c.USER_ID.in_(ids_to_update)).values(END_DATE=new_value)

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
    logging.info("Done updating existing users in database")


# Now that we've updated the rows that will be effected
# We are going to append the new information now. 
logging.info("Adding new and updated records to DB")
try:
    logging.info(list(updated_req_df.columns))
    if 'index' in updated_req_df.columns:
        updated_req_df = updated_req_df.drop(columns = ["index"])
    
    updated_req_df.to_sql('dw2_users',con = engine, if_exists = 'append', index= False)
    logging.info("Done adding new and updated records to database.")

except Exception as e:
    logging.warning(f"Adding new and updated users into DB !FAILED! {str(e)}")

