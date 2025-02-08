import sqlite3
import pandas as pd

# Path to your CSV file
csv_file = 'C:/Users/adrie/Documents/Projects Data/data engineer/SDG_Cleaned.csv'

# Path to your SQLite database
db_file = 'datawarehouse.db'

# Load CSV data into pandas DataFrame
df = pd.read_csv(csv_file)

# Connect to the SQLite database
conn = sqlite3.connect(db_file)

# Insert the DataFrame into SQLite database (if_exists='replace' will replace the table if it exists)
df.to_sql('AllEntities', conn, if_exists='replace', index=False)

# Close the connection
conn.close()
