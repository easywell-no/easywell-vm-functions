#!/usr/bin/env python3

import os
import json
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def load_env_variables():
    load_dotenv()

def get_supabase_client() -> Client:
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')

    if not SUPABASE_URL or not SUPABASE_KEY:
        error_msg = "Environment variables SUPABASE_URL or SUPABASE_KEY not set."
        logging.error(error_msg)
        raise EnvironmentError(error_msg)

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logging.info("Supabase client created successfully!")
        return supabase
    except Exception as e:
        logging.error(f"Failed to create Supabase client: {e}", exc_info=True)
        raise

def query_wellbore_data(supabase: Client):
    data = {}

    # Number of EXPLORATION, DEVELOPMENT, OTHER, and total number of wells
    try:
        response = supabase.table("wellbore_data").select("type").execute()
        if response.error:
            raise Exception(response.error.message)
        type_counts = {}
        for record in response.data:
            well_type = record.get('type') or 'UNKNOWN'
            type_counts[well_type] = type_counts.get(well_type, 0) + 1
        data['wellbore_data'] = {}
        data['wellbore_data']['type_counts'] = type_counts
    except Exception as e:
        logging.error(f"Error fetching type counts from wellbore_data: {e}")
        raise

    # Total number of wells
    try:
        response = supabase.table("wellbore_data").select("id", count='exact').execute()
        if response.error:
            raise Exception(response.error.message)
        total_wells = response.count
        data['wellbore_data']['total_wells'] = total_wells
    except Exception as e:
        logging.error(f"Error fetching total wells from wellbore_data: {e}")
        raise

    # Number of TRUE and FALSE in needs_rescrape
    try:
        response = supabase.table("wellbore_data").select("needs_rescrape").execute()
        if response.error:
            raise Exception(response.error.message)
        needs_rescrape_counts = {"True": 0, "False": 0, "NULL": 0}
        for record in response.data:
            value = record.get('needs_rescrape')
            if value is True:
                needs_rescrape_counts["True"] += 1
            elif value is False:
                needs_rescrape_counts["False"] += 1
            else:
                needs_rescrape_counts["NULL"] += 1
        data['wellbore_data']['needs_rescrape'] = needs_rescrape_counts
    except Exception as e:
        logging.error(f"Error fetching needs_rescrape counts from wellbore_data: {e}")
        raise

    # Number of waiting, reserved, completed in status
    try:
        response = supabase.table("wellbore_data").select("status").execute()
        if response.error:
            raise Exception(response.error.message)
        status_counts = {}
        for record in response.data:
            status = record.get('status') or 'UNKNOWN'
            status_counts[status] = status_counts.get(status, 0) + 1
        data['wellbore_data']['status_counts'] = status_counts
    except Exception as e:
        logging.error(f"Error fetching status counts from wellbore_data: {e}")
        raise

    return data

def query_wellbore_history(supabase: Client):
    data = {}
    try:
        response = supabase.table("wellbore_history").select("wlbwellborename").execute()
        if response.error:
            raise Exception(response.error.message)
        unique_wells = set()
        for record in response.data:
            name = record.get('wlbwellborename')
            if name:
                unique_wells.add(name)
        data['wellbore_history'] = {'unique_wells': len(unique_wells)}
    except Exception as e:
        logging.error(f"Error fetching unique wells from wellbore_history: {e}")
        raise
    return data

def main():
    setup_logging()
    load_env_variables()

    try:
        supabase = get_supabase_client()
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        return

    try:
        data = {}
        data.update(query_wellbore_data(supabase))
        data.update(query_wellbore_history(supabase))
        print(json.dumps(data))
    except Exception as e:
        logging.error(f"Error querying the database: {e}")
        print(json.dumps({"error": str(e)}))
    finally:
        supabase.close()

if __name__ == "__main__":
    main()
