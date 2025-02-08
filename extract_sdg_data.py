import os
import zipfile
import pandas as pd

# 📌 Set the base directory to the correct Linux-style path in WSL
base_dir = "c:/Users/adrie/Documents/Projects Data/data engineer"

# Unzip File
zip_file = os.path.join(base_dir, "SDG_BulkDownloads_E_All_Data.zip")
extract_folder = os.path.join(base_dir, "SDG_Data")

if not os.path.exists(extract_folder):  # Check if already extracted
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
    print(f"✅ Extracted files to: {extract_folder}")
else:
    print(f"✅ Folder already exists: {extract_folder}")

# Load CSV
csv_files = [f for f in os.listdir(extract_folder) if f.endswith(".csv")]

if len(csv_files) == 0:
    raise FileNotFoundError("❌ No CSV files found in the extracted folder.")

csv_path = os.path.join(extract_folder, csv_files[0])  # Load first CSV
df_sdg = pd.read_csv(csv_path)

print(f"✅ Loaded CSV file: {csv_path}")
print(df_sdg.head())

# Save a cleaned version (Optional)
cleaned_csv = os.path.join(base_dir, "SDG_Cleaned.csv")
df_sdg.to_csv(cleaned_csv, index=False)
print(f"✅ Cleaned CSV saved as: {cleaned_csv}")
