import sqlite3

# Connect to the database
conn = sqlite3.connect("datawarehouse.db")  # Update with actual DB file
cursor = conn.cursor()

# Get all column names from AllEntities
cursor.execute("PRAGMA table_info(AllEntities);")
columns = [row[1].strip() for row in cursor.fetchall()]  # Extract column names and strip spaces

# Debug: Print extracted column names
print("Extracted Columns:", columns)

# Filter only columns where the name starts with "Y" and the year part is > 2020
filtered_columns = [
    col for col in columns if col.startswith('Y') and col[1:5].isdigit() and int(col[1:5]) > 2020
]

# Debug: Print filtered year columns
print("Filtered Year Columns:", filtered_columns)

# Include important columns like 'id' and 'name' (adjust based on your dataset)
important_columns = ['Area Code', 'Area', 'Item Code', 'Item']  # Add other necessary columns
final_columns = important_columns + filtered_columns

# Create new table if we have valid columns
if filtered_columns:
    column_list = ", ".join(final_columns)
    
    cursor.execute("DROP TABLE IF EXISTS table2020ormore;")  # Drop old table if it exists
    create_table_query = f"CREATE TABLE table2020ormore AS SELECT {column_list} FROM AllEntities;"
    cursor.execute(create_table_query)

    print(f"✅ New table 'table2020ormore' created with columns: {column_list}")
else:
    print("⚠️ No columns found for years > 2020. Check column filtering logic.")

# Commit the changes and close connection
conn.commit()
conn.close()
