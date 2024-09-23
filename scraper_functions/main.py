import os
from dotenv import load_dotenv
from update_wellbore import update_wellbore_data
from scrape_factpages import scrape_fact_pages
from supabase import create_client, Client

print("Hello World")

# Load environment variables
load_dotenv(dotenv_path='./.env')

# Access Supabase credentials from environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Print the loaded values to verify
print(f"Loaded SUPABASE_URL: {SUPABASE_URL}")
print(f"Loaded SUPABASE_KEY: {SUPABASE_KEY[:5]}...")

if SUPABASE_URL is None or SUPABASE_KEY is None:
    raise Exception("Environment variables not loaded properly.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

if __name__ == "__main__":
    print("Starting the update of wellbore_data...")
    update_wellbore_data(supabase)  # Pass the supabase client to the function
    print("Completed updating wellbore_data.")
