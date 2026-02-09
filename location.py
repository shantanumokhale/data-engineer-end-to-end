# 1. Install required libraries for this step
%pip install geopy tqdm pandas

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm
import os
from pathlib import Path

# --- 1. Load Data & Setup Geocoder ---

# Use relative paths so it works in Docker/Databricks
# Looks for the file created by your first script
input_dir = Path("mastere")
input_file_path = input_dir / "nagpur_data.csv"
# input_dir = Path("/dbfs/mnt/my_project/mastere") # this is work when we  import file in databricks

if not input_file_path.exists():
    print(f"❌ Error: {input_file_path} not found. Run the generator script first!")
    exit()

# Load the CSV file
df = pd.read_csv(input_file_path)

# Ensure 'unique_number' is the index (matching your first script's output)
if 'unique_number' in df.columns:
    df.set_index('unique_number', inplace=True)

print(f"Loaded {len(df)} records from {input_file_path}")

# Initialize the geolocator
geolocator = Nominatim(user_agent="nagpur_data_eng_project")
# 1 second delay per request to respect Nominatim's Free Tier policy
reverse_service = RateLimiter(geolocator.reverse, min_delay_seconds=1.0)

# --- 2. Process the DataFrame ---

print("\nStarting Reverse Geocoding (1 request per second)...")

# Combine Lat/Long into a single column for processing
df['coords'] = df['latitude'].astype(str) + ", " + df['longitude'].astype(str) 

# Apply reverse geocoding with a progress bar
tqdm.pandas(desc="Fetching Addresses")
df['address_raw'] = df['coords'].progress_apply(reverse_service)

# Extract just the readable address text
df['Address'] = df['address_raw'].apply(lambda loc: loc.address if loc else "Address not found")

# Extract the postcode
df['Pincode'] = df['address_raw'].apply(lambda loc: loc.raw.get('address', {}).get('postcode') if loc else "N/A")

# --- 3. Clean up and Save ---

# Drop temporary columns and original coordinates to keep the 'Silver' layer clean
df_final = df.drop(columns=['address_raw', 'coords', 'latitude', 'longitude'])

output_file_name = input_dir / "consumer_master_data_geocoded.csv"
df_final.to_csv(output_file_name, encoding='utf-8-sig', index=True)

print(f"\n✅ Success! Enriched data saved as: {output_file_name}")
print("\n--- Preview ---")
print(df_final[['name', 'Address', 'Pincode']].head())
