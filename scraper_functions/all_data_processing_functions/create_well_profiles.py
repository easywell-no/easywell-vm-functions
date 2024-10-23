# create_well_profiles.py

import os
from supabase import create_client, Client
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_html_text(html_text):
    """
    Cleans HTML text by removing tags and decoding entities.
    """
    soup = BeautifulSoup(html_text, 'html.parser')
    text = soup.get_text(separator='\n')  # Preserve line breaks
    text = text.strip()
    return text

def get_well_profile(wlbwellborename):
    # Fetch data from well_coordinates
    well_coordinates_response = supabase.table('well_coordinates').select(
        'wlbwellborename, wlbdrillingoperator, wlbproductionlicence, wlbwelltype, wlbpurposeplanned, wlbcontent, wlbentrydate, wlbcompletiondate, wlbfield, wlbnsdecdeg, wlbewdecdeg'
    ).eq('wlbwellborename', wlbwellborename).execute()
    well_coordinates = well_coordinates_response.data

    # Fetch and clean well_history
    well_history_response = supabase.table('well_history').select('wlbhistory').eq('wlbwellborename', wlbwellborename).execute()
    well_history_data = well_history_response.data

    well_history = []
    if well_history_data:
        for item in well_history_data:
            if 'wlbhistory' in item and item['wlbhistory']:
                cleaned_text = clean_html_text(item['wlbhistory'])
                well_history.append(cleaned_text)

    # Fetch data from well_lito
    well_lito_response = supabase.table('well_lito').select(
        'lsutopdepth, lsubottomdepth, lsuname'
    ).eq('wlbwellborename', wlbwellborename).execute()
    well_lito = well_lito_response.data

    # Fetch data from well_casings
    well_casings_response = supabase.table('well_casings').select(
        'wlbcasingtype, wlbcasingdiameter, wlbcasingdepth'
    ).eq('wlbwellborename', wlbwellborename).execute()
    well_casings = well_casings_response.data

    # Combine all data into a dictionary
    well_profile = {
        'well_coordinates': well_coordinates,
        'well_history': well_history,
        'well_lito': well_lito,
        'well_casings': well_casings
    }

    return well_profile
