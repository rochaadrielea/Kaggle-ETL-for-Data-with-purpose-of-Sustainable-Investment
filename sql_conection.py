import pyodbc

# Connection string using Windows Authentication
conn_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=master;Trusted_Connection=yes;TrustServerCertificate=yes;"

try:
    # Establish connection
    conn = pyodbc.connect(conn_string)
    print("✅ Connected to SQL Server successfully!")

    # Create cursor
    cursor = conn.cursor()

    # Test query: Get all databases
    cursor.execute("SELECT name FROM sys.databases")

    # Print databases
    print("📌 Databases on the server:")
    for row in cursor.fetchall():
        print(row.name)

    # Close connection
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ Error: {e}")
