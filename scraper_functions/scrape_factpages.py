import logging
from supabase import Client
import sys
import os

# Add path to factpage_scraper_functions
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, 'factpage_scraper_functions'))

from factpage_scrape_wellbore_history import scrape_wellbore_history
from factpage_scrape_lithostratigraphy import scrape_lithostratigraphy
from factpage_scrape_casing_and_tests import scrape_casing_and_tests

def scrape_factpages(supabase: Client):
    # Get the list of exploration wells to scrape
    response = supabase.table('wellbore_data').select('wlbwellborename', 'wlbfactpageurl').eq('wlbwelltype', 'EXPLORATION').execute()

    # Check if the response has data
    if hasattr(response, 'data') and response.data:
        wellbores = response.data
        logging.info(f"Fetched {len(wellbores)} exploration wells to scrape.")

        for record in wellbores:
            wlbwellborename = record['wlbwellborename']
            factpage_url = record['wlbfactpageurl']

            if not factpage_url:
                logging.warning(f"No factpage URL for {wlbwellborename}")
                continue

            logging.info(f"Scraping factpage for {wlbwellborename}")

            try:
                scrape_wellbore_history(supabase, wlbwellborename, factpage_url)
                scrape_lithostratigraphy(supabase, wlbwellborename, factpage_url)
                scrape_casing_and_tests(supabase, wlbwellborename, factpage_url)
            except Exception as e:
                logging.error(f"An error occurred while scraping {wlbwellborename}: {e}")
    else:
        logging.error(f"Error fetching wellbore data or no data returned: {response}")
