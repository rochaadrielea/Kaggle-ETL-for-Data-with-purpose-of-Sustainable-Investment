import pandas as pd
import pyodbc

# ✅ Database Connection
server = 'localhost'  # Your SQL Server instance
database = 'DataWarehouse'  # Database name
conn = pyodbc.connect(
    f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
)
cursor = conn.cursor()

# ✅ Step 1: Create Tables if Not Exists
create_tables_query = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Area' AND xtype='U')
BEGIN
    CREATE TABLE Dim_Area (
        AreaKey INT IDENTITY(1,1) PRIMARY KEY,
        AreaCode INT NOT NULL,
        AreaCodeM49 VARCHAR(10) NOT NULL,
        Area VARCHAR(255) NOT NULL
    );
END

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Item' AND xtype='U')
BEGIN
    CREATE TABLE Dim_Item (
        ItemKey INT IDENTITY(1,1) PRIMARY KEY,
        ItemCode VARCHAR(50) NOT NULL
    );
END

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Time' AND xtype='U')
BEGIN
    CREATE TABLE Dim_Time (
        TimeKey INT IDENTITY(1,1) PRIMARY KEY,
        Year INT NOT NULL,
        ForecastFlag BIT NOT NULL
    );
END

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Fact_SDG_Measurements' AND xtype='U')
BEGIN
    CREATE TABLE Fact_SDG_Measurements (
        FactID INT IDENTITY(1,1) PRIMARY KEY,
        AreaKey INT FOREIGN KEY REFERENCES Dim_Area(AreaKey),
        ItemKey INT FOREIGN KEY REFERENCES Dim_Item(ItemKey),
        TimeKey INT FOREIGN KEY REFERENCES Dim_Time(TimeKey),
        Value FLOAT NULL,
        MeasurementNote VARCHAR(1000) NULL  -- Increased the size to 1000
    );
END
"""

cursor.execute(create_tables_query)
conn.commit()
print("✅ Tables checked/created successfully.")

# ✅ Step 2: Load the CSV File
csv_path = r"C:/Users/adrie/Documents/Projects Data/data engineer/SDG_Data/SDG_BulkDownloads_E_All_Data.csv"
df = pd.read_csv(csv_path, low_memory=False)

# ✅ Step 3: Clean Data (Replace invalid values)
df.replace(["Not Available", "NA", "NaN"], None, inplace=True)

# ✅ Step 4: Insert Data into `Dim_Area`
df_areas = df[['Area Code', 'Area Code (M49)', 'Area']].drop_duplicates()
for _, row in df_areas.iterrows():
    cursor.execute("""
        INSERT INTO Dim_Area (AreaCode, AreaCodeM49, Area) VALUES (?, ?, ?)
    """, row['Area Code'], row["Area Code (M49)"], row["Area"])
conn.commit()

# ✅ Step 5: Insert Data into `Dim_Item`
df_items = df[['Item Code']].drop_duplicates()
for _, row in df_items.iterrows():
    cursor.execute("""
        INSERT INTO Dim_Item (ItemCode) VALUES (?)
    """, row['Item Code'])
conn.commit()

# ✅ Step 6: Insert Time Dimension Data (2023-2024)
years = [2023, 2024]
for year in years:
    cursor.execute("INSERT INTO Dim_Time (Year, ForecastFlag) VALUES (?, ?)", year, 0)
    cursor.execute("INSERT INTO Dim_Time (Year, ForecastFlag) VALUES (?, ?)", year, 1)
conn.commit()

# ✅ Step 7: Fetch Keys for Faster Lookups
cursor.execute("SELECT AreaKey, AreaCode FROM Dim_Area")
area_map = {row[1]: row[0] for row in cursor.fetchall()}

cursor.execute("SELECT ItemKey, ItemCode FROM Dim_Item")
item_map = {row[1]: row[0] for row in cursor.fetchall()}

cursor.execute("SELECT TimeKey, Year, ForecastFlag FROM Dim_Time")
time_map = {(row[1], row[2]): row[0] for row in cursor.fetchall()}

# ✅ Step 8: Insert Data into Fact Table (Handling Float Errors and Truncating MeasurementNote)
max_length = 1000  # Ensure MeasurementNote is not too long

for _, row in df.iterrows():
    area_key = area_map.get(row['Area Code'])
    item_key = item_map.get(row['Item Code'])

    for year in years:
        value_col = f"Y{year}"
        forecast_col = f"Y{year}F"

        if value_col in df.columns:
            time_key = time_map.get((year, 0))
            value = row[value_col]

            # ✅ Ensure value is a valid float or set to None
            if pd.notnull(value):
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    value = None  # Set invalid values to NULL
            else:
                value = None

            measurement_note = row.get(f'Y{year}N', None)
            if measurement_note and len(str(measurement_note)) > max_length:
                measurement_note = str(measurement_note)[:max_length]  # Truncate if too long
            measurement_note = str(measurement_note) if pd.notnull(measurement_note) else None  # Ensure it's a string or NULL

            cursor.execute("""
                INSERT INTO Fact_SDG_Measurements (AreaKey, ItemKey, TimeKey, Value, MeasurementNote)
                VALUES (?, ?, ?, ?, ?)
            """, area_key, item_key, time_key, value, measurement_note)

        if forecast_col in df.columns:
            time_key = time_map.get((year, 1))
            forecast_value = row[forecast_col]

            # ✅ Ensure forecast_value is a valid float or set to None
            if pd.notnull(forecast_value):
                try:
                    forecast_value = float(forecast_value)
                except (ValueError, TypeError):
                    forecast_value = None
            else:
                forecast_value = None

            forecast_note = row.get(f'Y{year}N', None)
            if forecast_note and len(str(forecast_note)) > max_length:
                forecast_note = str(forecast_note)[:max_length]  # Truncate if too long
            forecast_note = str(forecast_note) if pd.notnull(forecast_note) else None  # Ensure it's a string or NULL

            cursor.execute("""
                INSERT INTO Fact_SDG_Measurements (AreaKey, ItemKey, TimeKey, Value, MeasurementNote)
                VALUES (?, ?, ?, ?, ?)
            """, area_key, item_key, time_key, forecast_value, forecast_note)

conn.commit()

# ✅ Step 9: Close Connection
cursor.close()
conn.close()

print("🚀 Data successfully loaded into SQL Server")
