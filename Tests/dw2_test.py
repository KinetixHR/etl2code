

from simple_salesforce import Salesforce
import pyodbc
import requests
from datetime import datetime
import datetime as dt
# Configure Logging
import logging
logging.basicConfig(filename='./etl2code/logs/dw2_test_logging.log', level=logging.INFO,
                    format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

# Salesforce credentials and initialization
sf_username = 'salesforceapps@kinetixhr.com '
sf_password = 'Kinetix2Password '
sf_security_token = 'your_security_token'
sf_instance = 'USA638'
session = requests.Session()
sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session) 
#sf = Salesforce(username=sf_username, password=sf_password, security_token=sf_security_token)


# Azure SQL Database credentials
server = "kinetixsql.database.windows.net" 
database = "KinetixSQL" 
username = "awhelan" 
password = "5uj7*ZpE8Y$D"
DRIVER="ODBC Driver 18 for SQL Server"

# SQL Database connection string
conn_str = f'DRIVER={DRIVER};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

# Get today's date
effective_date = datetime.now().strftime('%Y-%m-%d')
# Contacts update date
start_date = "1/1/2021"
date_format = "%m/%d/%Y"
update_date = dt.datetime.strptime(start_date,date_format).strftime("%Y-%m-%d")
# submittals starts on or after 1-1-2023
start_date1 = "1/1/2023"
date_format = "%m/%d/%Y"
update_date1 = dt.datetime.strptime(start_date,date_format).strftime("%Y-%m-%d")
try: 
    # SOQL query to get job count from Salesforce
    soql_job_query = f"""SELECT COUNT() FROM TR1__Job__c where TR1__Closed_Date__c > 2019-12-31 Or TR1__Closed_Date__c = null"""
    soql_Placement_query = f"""SELECT count() FROM TR1__Closing_Report__c WHERE TR1__Job__r.TR1__Closed_Date__c >2019-12-31 Or TR1__Job__r.TR1__Closed_Date__c = null"""
    soql_Submittal_query = f"""SELECT COUNT() FROM TR1__Submittal__c  WHERE CreatedDate__c >= 2023-01-01 """
    soql_contacts_Query = f"""SELECT COUNT() FROM Contact WHERE CreatedDate > {update_date}T00:00:00Z """
except Exception as e:
    print(f"An error occurred: {e}")
    #------------------------------------------------------------------------

try:
    # Query Salesforce for total soql count
    sf_Job_query = sf.query(soql_job_query)
    TR_Job_count = sf_Job_query['totalSize']
    sf_Place_query = sf.query(soql_Placement_query)
    TR_Place_count = sf_Place_query['totalSize']
    sf_Sub_query = sf.query(soql_Submittal_query)
    TR_Sub_count = sf_Sub_query ['totalSize']
    sf_Cont_query = sf.query(soql_contacts_Query)
    TR_Cont_count = sf_Cont_query['totalSize']
    
    
    #------------------------------------------------------------------------

    # SQL query to get the total Jobs count from DW2 excluding end date and is deleted filters
    count_Job_query = "SELECT COUNT(JOB_ID) AS TotalJobs FROM DW2_Jobs where (end_date = '9999-12-31 00:00:00.000' And is_deleted = 'False') And (CLOSED_DATE > '2020-1-1' OR CLOSED_DATE is NULL) ;"
    cursor.execute(count_Job_query)
    total_jobs_result = cursor.fetchone()
    DW_Jobs_count = total_jobs_result[0] if total_jobs_result else 0
       
    #------------------------------------------------------------------------
    # SQL query to get the total Placement  count from DW2 excluding end date 
    count_Place_query = "select  count( PLACEMENT_ID) FROM dw2_placements a INNER JOIN dw2_jobs b on a.job = b.JOB_ID where (b.CLOSED_DATE >  '2020-1-1'  OR b.CLOSED_DATE is NULL) and (b.END_DATE = '9999-12-31' AND b.IS_DELETED = 'False') AND a.END_DATE = '9999-12-31';"
    cursor.execute(count_Place_query)
    total_place_result = cursor.fetchone()
    DW_Place_count = total_place_result[0] if total_place_result else 0
    
    #------------------------------------------------------------------------
    
    # SQL query to get the total Submittal count from DW2 
    count_Sub_query = "select COUNT(*) from dw2_submittals"
    cursor.execute(count_Sub_query)
    total_sub_result = cursor.fetchone()
    DW_Sub_count = total_sub_result[0] if total_sub_result else 0
    
    #------------------------------------------------------------------------
    # SQL query to get the total Contact  count from DW2 excluding end date and is deleted filters
    count_Cont_query = f"select count(*) from dw2_contacts where IS_DELETED = '0' AND CREATED_DATE_DT >= '2021-01-01'"
    cursor.execute(count_Cont_query)
    total_Cont_result = cursor.fetchone()
    DW_Cont_count = total_Cont_result[0] if total_Cont_result else 0

    #------------------------------------------------------------------------
    # Checking the JOB counts
    #------------------------------------------------------------------------ 
    if TR_Job_count == DW_Jobs_count:
        print(f"The DW2_Job values are equal.")
        Job_check = "Equal"
    elif TR_Job_count > DW_Jobs_count:
        job = TR_Job_count - DW_Jobs_count
        print(f"The DW2_Job values are not equal- TR has {job} more data ")
        logging.warning(f"WARNING!! Dw2_Jobs count mismatch Today - TR has {job} more data ")
        Job_check = "Un-Equal"  
    elif TR_Job_count < DW_Jobs_count:
        job = DW_Jobs_count-TR_Job_count 
        print(f"The DW2_Job values are not equal- DW has {job} more data ")
        logging.warning(f"WARNING!! Dw2_Jobs count mismatch Today - DW has {job} more data ")
        Job_check = "Un-Equal" 
    #------------------------------------------------------------------------
    # Checking the PLACEMENT counts
    #------------------------------------------------------------------------ 
    if TR_Place_count == DW_Place_count:
        print(f"The DW2_Placement values are equal.")
        Placement_check = "Equal"
    elif TR_Place_count > DW_Place_count:
        Plac = TR_Place_count - DW_Place_count
        print(f"The DW2_Placements values are not equal- TR has {Plac} more data ")
        logging.warning(f"WARNING!! Dw2_Placements count mismatch Today - TR has {Plac} more data ")
        Placement_check = "Un-Equal"  
    elif TR_Place_count < DW_Place_count:
        Plac =  DW_Place_count -TR_Place_count
        print(f"The DW2_Placement values are not equal- DW has {Plac} more data ")
        logging.warning(f"WARNING!! Dw2_Placement count mismatch Today - DW has {Plac} more data ")
        Placement_check = "Un-Equal" 

    #------------------------------------------------------------------------
    # Checking the SUBMITTAL counts
    #------------------------------------------------------------------------ 
    if TR_Sub_count == DW_Sub_count:
        print("The DW2_Submittals values are equal.")
        Sub_check = "Equal"
    elif TR_Sub_count > DW_Sub_count:
        Subm = TR_Sub_count - DW_Sub_count
        print(f"The DW2_Submittals values are not equal- TR has {Subm} more data ")
        logging.warning(f"WARNING!! Dw2_Submittals count mismatch Today - TR has {Subm} more data ")
        Sub_check = "Un-Equal"  
    elif TR_Sub_count < DW_Sub_count:
        Subm =  DW_Sub_count -TR_Sub_count
        print(f"The DW2_Submittals values are not equal- DW has {Subm} more data ")
        logging.warning(f"WARNING!! Dw2_Submittals count mismatch Today - DW has {Subm} more data ")
        Sub_check = "Un-Equal" 
    #------------------------------------------------------------------------
    # Checking the CONTACTS counts
    #------------------------------------------------------------------------ 
    if TR_Cont_count == DW_Cont_count:
        print("The DW2_Contacts values are equal.")
        Cont_check = "Equal"
    elif TR_Cont_count > DW_Cont_count:
        cont = TR_Cont_count - DW_Cont_count
        print(f"The DW2_Contacts values are not equal- TR has {cont} more data ")
        logging.warning(f"WARNING!! DW2_Contacts count mismatch Today - TR has {cont} more data ")
        Cont_check = "Un-Equal"  
    elif TR_Cont_count < DW_Cont_count:
        cont =   DW_Cont_count-TR_Cont_count 
        print(f"The DW2_Contacts values are not equal- DW has {cont} more data ")
        logging.warning(f"WARNING!! DW2_Contacts count mismatch Today - DW has {cont} more data ")
        Cont_check = "Un-Equal"   
    
    #------------------------------------------------------------------------
    # Inserting into DW
    #------------------------------------------------------------------------   
    # SQL statement to insert the data into Azure SQL Database
    insert_query = """
    INSERT INTO DW2_Total_Count (TR_Job_count, DW_Jobs_count,Job_check,TR_Place_count,DW_Place_count,Placement_check,TR_Sub_count,DW_Sub_count,Sub_check,TR_Cont_count,DW_Cont_count,Cont_check,Effective_Date)
    VALUES (?, ?, ?,?, ?, ?,?, ?, ?,?,?, ?, ?)
    """
    cursor.execute(insert_query, TR_Job_count, DW_Jobs_count,Job_check,TR_Place_count,DW_Place_count,Placement_check,TR_Sub_count,DW_Sub_count,Sub_check,TR_Cont_count,DW_Cont_count,Cont_check,effective_date)
    cnxn.commit()
    
    
    #------------------------------------------------------------------------
    # Printing the results
    #------------------------------------------------------------------------ 
    print(f"Successfully inserted daily DW2_job count of {DW_Jobs_count} and Salesforce Jobs count of {TR_Job_count} into DW2_Total_Count with effective date {effective_date}.")
    
    print(f"Successfully inserted daily DW2_Placements count of {DW_Place_count} and Salesforce Placemets count of {TR_Place_count} into DW2_Total_Count with effective date {effective_date}.")
    
    print(f"Successfully inserted daily DW2_submittals count of {DW_Sub_count} and Salesforce Submittals count of {TR_Sub_count} into DW2_Total_Count with effective date {effective_date}.")
    
    print(f"Successfully inserted daily DW2_Contacts count of {DW_Cont_count} and Salesforce Contacts count of {TR_Cont_count} into DW2_Total_Count with effective date {effective_date}.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    cursor.close()
    cnxn.close()