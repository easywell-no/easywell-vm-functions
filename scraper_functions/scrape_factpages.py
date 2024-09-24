import logging
from supabase import Client
import sys
import os

# Correct the path to factpage_scraper_functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'factpage_scraper_functions')))

from factpage_scrape_wellbore_history import scrape_wellbore_history

def scrape_factpages(supabase: Client):
    # Get the list of wellbores to scrape
    wellbores = supabase.table('wellbore_data').select('wlbwellborename', 'wlbfactpageurl').execute()
    if wellbores.error:
        logging.error(f"Error fetching wellbore data: {wellbores.error}")
        return

    for record in wellbores.data:
        wlbwellborename = record['wlbwellborename']
        factpage_url = record['wlbfactpageurl']

        if not factpage_url:
            logging.warning(f"No factpage URL for {wlbwellborename}")
            continue

        logging.info(f"Scraping factpage for {wlbwellborename}")

        try:
            scrape_wellbore_history(supabase, wlbwellborename, factpage_url)
        except Exception as e:
            logging.error(f"An error occurred while scraping {wlbwellborename}: {e}")
