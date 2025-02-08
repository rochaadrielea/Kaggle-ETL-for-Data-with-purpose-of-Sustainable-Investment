import sqlite3
import pandas as pd

# Connect to SQLite database (it will be created if it doesn't exist)
conn = sqlite3.connect('datawarehouse.db')  # Change the name as needed
cursor = conn.cursor()

# Step 1: Create Tables in SQLite
cursor.execute("""
CREATE TABLE IF NOT EXISTS Dim_Area (
    AreaKey INTEGER PRIMARY KEY AUTOINCREMENT,
    AreaCode INTEGER NOT NULL,
    AreaCodeM49 TEXT NOT NULL,
    Area TEXT NOT NULL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Dim_Item (
    ItemKey INTEGER PRIMARY KEY AUTOINCREMENT,
    ItemCode TEXT NOT NULL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Dim_Time (
    TimeKey INTEGER PRIMARY KEY AUTOINCREMENT,
    Year INTEGER NOT NULL,
    ForecastFlag INTEGER NOT NULL  -- Use INTEGER for Boolean in SQLite
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Fact_SDG_Measurements (
    FactID INTEGER PRIMARY KEY AUTOINCREMENT,
    AreaKey INTEGER,
    ItemKey INTEGER,
    TimeKey INTEGER,
    Value REAL,
    MeasurementNote TEXT,
    FOREIGN KEY (AreaKey) REFERENCES Dim_Area(AreaKey),
    FOREIGN KEY (ItemKey) REFERENCES Dim_Item(ItemKey),
    FOREIGN KEY (TimeKey) REFERENCES Dim_Time(TimeKey)
);
""")

# Commit changes and close connection to the database
conn.commit()
print("✅ Tables created successfully in SQLite database")

# Step 2: Close connection
cursor.close()
conn.close()
