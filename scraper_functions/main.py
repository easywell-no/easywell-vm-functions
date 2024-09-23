# main.py

import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv(dotenv_path='./scraper_functions/.env')

# Print all environment variables to check if they were loaded
for key, value in os.environ.items():
    if "SUPABASE" in key:
        print(f"{key}: {value}")

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if SUPABASE_URL is None or SUPABASE_KEY is None:
    raise Exception("Environment variables not loaded properly.")

# Create the Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("Supabase client created successfully!")

from update_wellbore import update_wellbore_data

# Call the update function
update_wellbore_data(supabase)
