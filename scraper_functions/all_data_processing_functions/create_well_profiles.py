# create_well_profiles.py

import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_well_profile(wlbwellborename):
    # Fetch data from well_coordinates
    well_coordinates_response = supabase.table('well_coordinates').select('*').eq('wlbwellborename', wlbwellborename).execute()
    well_coordinates = well_coordinates_response.data

    # Fetch data from well_history
    well_history_response = supabase.table('well_history').select('*').eq('wlbwellborename', wlbwellborename).execute()
    well_history = well_history_response.data

    # Fetch data from well_lito
    well_lito_response = supabase.table('well_lito').select('*').eq('wlbwellborename', wlbwellborename).execute()
    well_lito = well_lito_response.data

    # Fetch data from well_casings
    well_casings_response = supabase.table('well_casings').select('*').eq('wlbwellborename', wlbwellborename).execute()
    well_casings = well_casings_response.data

    # Fetch data from well_mud
    well_mud_response = supabase.table('well_mud').select('*').eq('wlbwellborename', wlbwellborename).execute()
    well_mud = well_mud_response.data

    # Combine all data into a dictionary
    well_profile = {
        'wlbwellborename': wlbwellborename,
        'well_coordinates': well_coordinates,
        'well_history': well_history,
        'well_lito': well_lito,
        'well_casings': well_casings,
        'well_mud': well_mud
    }

    return well_profile
