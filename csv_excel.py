import pandas as pd

# Path to your CSV file
csv_file = r'C:/Users/adrie/Documents/Projects Data/data engineer/SDG_Cleaned.csv'

# Load the CSV into a DataFrame
df = pd.read_csv(csv_file)

# Path to save the new Excel file
excel_file = r'C:/Users/adrie/Documents/Projects Data/data engineer/SDG_Cleaned.xlsx'

# Convert and save the DataFrame as an Excel file
df.to_excel(excel_file, index=False)

print(f"CSV has been converted to Excel and saved as: {excel_file}")
