import os
from dotenv import load_dotenv
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