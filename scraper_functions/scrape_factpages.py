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
from factpage_scrape_drilling_fluid import scrape_drilling_fluid
from factpage_scrape_general_info import scrape_general_info

def scrape_factpages(supabase: Client):
    logging.info("Starting scrape_factpages process.")

    try:
        # Select only exploration wells that need rescraping and are in 'waiting' status
        response = supabase.table('wellbore_data')\
            .select('wlbwellborename, wlbfactpageurl')\
            .eq('needs_rescrape', True)\
            .eq('status', 'waiting')\
            .eq('wlbwelltype', 'EXPLORATION')\
            .execute()

        wells_to_scrape = response.data
        if not wells_to_scrape:
            logging.info("No exploration wells require rescraping at this time.")
            return

        logging.info(f"Found {len(wells_to_scrape)} exploration wells to scrape.")
    except Exception as e:
        logging.error(f"Error querying exploration wells: {e}", exc_info=True)
        return

    for well in wells_to_scrape:
        wlbwellborename = well.get('wlbwellborename')
        factpage_url = well.get('wlbfactpageurl')

        if not factpage_url:
            logging.warning(f"No factpage URL for wellbore '{wlbwellborename}'. Skipping.")
            continue

        logging.info(f"Scraping wellbore '{wlbwellborename}'.")

        try:
            # Perform scraping for the well
            scrape_wellbore_history(supabase, wlbwellborename, factpage_url)
            scrape_lithostratigraphy(supabase, wlbwellborename, factpage_url)
            scrape_casing_and_tests(supabase, wlbwellborename, factpage_url)
            scrape_drilling_fluid(supabase, wlbwellborename, factpage_url)
            scrape_general_info(supabase, wlbwellborename, factpage_url)

            # Mark as completed after scraping
            supabase.table('wellbore_data')\
                .update({'status': 'completed', 'needs_rescrape': False})\
                .eq('wlbwellborename', wlbwellborename)\
                .execute()

            logging.info(f"Successfully scraped and updated well '{wlbwellborename}'.")

        except Exception as scrape_exception:
            logging.error(f"Failed to scrape wellbore '{wlbwellborename}': {scrape_exception}", exc_info=True)
            # Mark as error if scraping fails
            supabase.table('wellbore_data')\
                .update({'status': 'error', 'needs_rescrape': False})\
                .eq('wlbwellborename', wlbwellborename)\
                .execute()

    logging.info("Completed scrape_factpages process.")
