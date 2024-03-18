import pandas as pd
import sqlalchemy
import urllib

today = pd.to_datetime("today")

#Azure SQL Database credentials
SERVER = "kinetixsql.database.windows.net" 
DATABASE = "KinetixSQL" 
USERNAME = "awhelan" 
PASSWORD = "5uj7*ZpE8Y$D"
DRIVER="ODBC Driver 18 for SQL Server"
constring = "mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus("DRIVER=ODBC Driver 18 for SQL Server;SERVER={0};PORT=1433;DATABASE={1};UID={2};PWD={3};TDS_Version=8.0;".format(SERVER, DATABASE, USERNAME, PASSWORD)))
engine = sqlalchemy.create_engine(constring,echo=False)

df = pd.read_sql("SELECT * FROM dw2_jobs",con = engine)
df.to_csv(f"jobs_table_extract for {today}.csv")

df = pd.read_sql("SELECT * FROM dw2_users",con = engine)
df.to_csv(f"users_table_extract for {today}.csv")

df = pd.read_sql("SELECT * FROM dw2_placements",con = engine)
df.to_csv(f"placements_table_extract for {today}.csv")
