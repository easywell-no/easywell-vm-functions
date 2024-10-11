import os
import logging
from supabase import create_client, Client

def get_supabase_client() -> Client:
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY')  # Use service key instead of anon key

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        error_msg = "Environment variables SUPABASE_URL or SUPABASE_SERVICE_KEY not set."
        logging.error(error_msg)
        raise EnvironmentError(error_msg)

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        logging.info("Supabase client created successfully with service role key!")
        return supabase
    except Exception as e:
        logging.error(f"Failed to create Supabase client: {e}", exc_info=True)
        raise