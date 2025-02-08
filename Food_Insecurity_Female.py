import pandas as pd
import pyodbc
from sqlalchemy import create_engine

# ✅ Load CSV File
csv_path = r"C:/Users/adrie/Documents/Projects Data/data engineer/SDG_Cleaned.csv"
df = pd.read_csv(csv_path, low_memory=False)

# ✅ Standardize Column Names
df.columns = df.columns.str.replace(" ", "_").str.replace("(", "").str.replace(")", "").str.upper()

# ✅ Filter for Relevant Data
df_filtered = df[df['ITEM'] == '2.1.2 Number of severely food insecure people (female) (15 years old and over)']

# ✅ Select Only Needed Columns
years = ['Y1974', 'Y1975', 'Y1976']
df_selected = df_filtered[['AREA'] + years]

# ✅ Convert to Long Format (Unpivot)
df_melted = df_selected.melt(id_vars=['AREA'], var_name='YEAR', value_name='VALUE')

# ✅ Replace NaN values with 0
df_melted['VALUE'] = df_melted['VALUE'].fillna(0)

# ✅ Convert YEAR to String (SQL Server does not like 'Y1974' as an identifier)
df_melted['YEAR'] = df_melted['YEAR'].astype(str)

# ✅ Rename Columns to Match SQL Table
df_melted.rename(columns={'AREA': 'COUNTRY'}, inplace=True)

# ✅ Connect to SQL Server
server = 'localhost'
database = 'DataWarehouse'
driver = 'ODBC Driver 17 for SQL Server'

conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
)
cursor = conn.cursor()
print("🚀 Connection made")

# ✅ Create New Table for Food Insecurity Data
create_table_query = """
DROP TABLE IF EXISTS Food_Insecurity_Female;

CREATE TABLE Food_Insecurity_Female (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    COUNTRY VARCHAR(255) NOT NULL,
    YEAR VARCHAR(10) NOT NULL,
    VALUE FLOAT NOT NULL DEFAULT 0
);
"""
cursor.execute(create_table_query)
conn.commit()

# ✅ Insert Data into SQL Server
engine = create_engine(
    f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server',
    fast_executemany=True
)

df_melted.to_sql(
    "Food_Insecurity_Female",
    con=engine,
    if_exists="append",
    index=False,
    chunksize=1000
)

# ✅ Close Connection
cursor.close()
conn.close()

print("🚀 Data successfully loaded into SQL Server")
