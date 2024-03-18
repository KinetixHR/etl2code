import pandas as pd
import sqlalchemy
import urllib


#Azure SQL Database credentials
SERVER = "kinetixsql.database.windows.net" 
DATABASE = "KinetixSQL" 
USERNAME = "awhelan" 
PASSWORD = "5uj7*ZpE8Y$D"
DRIVER="ODBC Driver 18 for SQL Server"
constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))
engine = sqlalchemy.create_engine(constring,echo=False)

df = pd.read_excel("Dw2_Jobs_03-14-2024-Extract.xlsx")
df.to_sql('dw2_jobs', con=engine, if_exists='replace', index=False)