# utils/get_supabase_client.py

import os
import logging
from supabase import create_client, Client

def get_supabase_client() -> Client:
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        error_msg = "Environment variables SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set."
        logging.error(error_msg)
        raise EnvironmentError(error_msg)

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        logging.info("Supabase client created successfully with Service Role Key!")
        return supabase
    except Exception as e:
        logging.error(f"Failed to create Supabase client: {e}", exc_info=True)
        raise
