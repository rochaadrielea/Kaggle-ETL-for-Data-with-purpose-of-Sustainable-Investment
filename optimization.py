import pandas as pd
import pyodbc
from sqlalchemy import create_engine

# ✅ Load CSV File
csv_path = r"C:/Users/adrie/Documents/Projects Data/data engineer/SDG_Cleaned.csv"
df = pd.read_csv(csv_path, low_memory=False)

# ✅ Standardize Column Names
df.columns = df.columns.str.replace(" ", "_").str.replace("(", "").str.replace(")", "").str.upper()

# ✅ Keep only the necessary columns
valid_columns = [
    "AREA_CODE", "AREA_CODE_M49", "AREA", "ITEM_CODE", "ITEM",
    "ELEMENT_CODE", "ELEMENT", "UNIT", "Y1974", "Y1975", "Y1976"
]

df = df[valid_columns]  # Keep only these columns

# ✅ Replace NaN values with 0
df.fillna(0, inplace=True)

# ✅ Connect to SQL Server
server = 'localhost'
database = 'DataWarehouse'
driver = 'ODBC Driver 17 for SQL Server'

conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
)
cursor = conn.cursor()
print("🚀 Connection made")

# ✅ Drop Table if Exists & Create New Table
create_table_query = """
DROP TABLE IF EXISTS SDG_Cleaned_Data;

CREATE TABLE SDG_Cleaned_Data (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    AREA_CODE INT NOT NULL,
    AREA_CODE_M49 VARCHAR(10) NOT NULL,
    AREA VARCHAR(255) NOT NULL,
    ITEM_CODE VARCHAR(50) NOT NULL,
    ITEM VARCHAR(255) NOT NULL,
    ELEMENT_CODE INT NOT NULL,
    ELEMENT VARCHAR(255) NOT NULL,
    UNIT VARCHAR(50) NOT NULL,
    Y1974 FLOAT NOT NULL DEFAULT 0,
    Y1975 FLOAT NOT NULL DEFAULT 0,
    Y1976 FLOAT NOT NULL DEFAULT 0
);
"""

cursor.execute(create_table_query)
conn.commit()

# ✅ Insert Data into SQL Server
engine = create_engine(
    f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server',
    fast_executemany=True
)

df.to_sql(
    "SDG_Cleaned_Data",
    con=engine,
    if_exists="append",
    index=False,
    method=None,  # Avoids "multi" issue
    chunksize=1000  # Inserts 1000 rows at a time
)

# ✅ Close Connection
cursor.close()
conn.close()

print("🚀 Data successfully loaded into SQL Server")

