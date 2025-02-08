import sqlite3

# Connect to SQLite Database
sqlite_db = r"C:/Users/adrie/Documents/Projects Data/data engineer/datawarehouse.db"
sqlite_conn = sqlite3.connect(sqlite_db)
sqlite_cursor = sqlite_conn.cursor()

# Check column names in Dim_Area
sqlite_cursor.execute("PRAGMA table_info(Dim_Area);")
columns = sqlite_cursor.fetchall()

print("✅ Columns in Dim_Area:")
for column in columns:
    print(column)

sqlite_conn.close()
