import logging
from supabase import Client
import os
from all_scrape_and_store_functions.factpage_scraper_functions.factpage_scrape_wellbore_history import scrape_wellbore_history
from all_scrape_and_store_functions.factpage_scraper_functions.factpage_scrape_lithostratigraphy import scrape_lithostratigraphy
from all_scrape_and_store_functions.factpage_scraper_functions.factpage_scrape_casing_and_tests import scrape_casing_and_tests
from all_scrape_and_store_functions.factpage_scraper_functions.factpage_scrape_drilling_fluid import scrape_drilling_fluid
from all_scrape_and_store_functions.factpage_scraper_functions.factpage_scrape_general_info import scrape_general_info

def scrape_factpages(supabase: Client):
    logging.info("Starting scrape_factpages process.")

    try:
        # Initialize list to collect all wells to scrape
        wells_to_scrape = []
        page_size = 1000  # Supabase default limit
        current_start = 0

        while True:
            response = supabase.table('wellbore_data')\
                .select('wlbwellborename, wlbfactpageurl, wlbwelltype')\
                .eq('needs_rescrape', True)\
                .eq('status', 'waiting')\
                .range(current_start, current_start + page_size - 1)\
                .execute()
            if not response.data:
                break
            wells_to_scrape.extend(response.data)
            logging.info(f"Fetched {len(response.data)} wells in batch starting at {current_start}.")
            if len(response.data) < page_size:
                # No more data
                break
            current_start += page_size

        if not wells_to_scrape:
            logging.info("No wells require rescraping at this time.")
            return

        logging.info(f"Found total of {len(wells_to_scrape)} wells to scrape.")
    except Exception as e:
        logging.error(f"Error querying wells: {e}", exc_info=True)
        return

    for well in wells_to_scrape:
        wlbwellborename = well.get('wlbwellborename')
        factpage_url = well.get('wlbfactpageurl')
        well_type = well.get('wlbwelltype')

        if not factpage_url:
            logging.warning(f"No factpage URL for wellbore '{wlbwellborename}'. Skipping.")
            continue

        # If the well is not of type 'EXPLORATION', mark as completed immediately
        if well_type != 'EXPLORATION':
            try:
                supabase.table('wellbore_data')\
                    .update({'status': 'completed', 'needs_rescrape': False})\
                    .eq('wlbwellborename', wlbwellborename)\
                    .execute()
                logging.info(f"Non-exploration well '{wlbwellborename}' marked as completed and skipped.")
            except Exception as e:
                logging.error(f"Exception while updating non-exploration well '{wlbwellborename}': {e}", exc_info=True)
            continue

        logging.info(f"Reserving wellbore '{wlbwellborename}' for scraping.")

        try:
            # Set the status to 'reserved' before starting scraping
            supabase.table('wellbore_data')\
                .update({'status': 'reserved'})\
                .eq('wlbwellborename', wlbwellborename)\
                .execute()
            logging.info(f"Status set to 'reserved' for well '{wlbwellborename}'.")
        except Exception as e:
            logging.error(f"Failed to set status to 'reserved' for well '{wlbwellborename}': {e}")
            continue  # Skip to the next well

        try:
            # Perform scraping for the well
            scrape_wellbore_history(supabase, wlbwellborename, factpage_url)
            scrape_lithostratigraphy(supabase, wlbwellborename, factpage_url)
            scrape_casing_and_tests(supabase, wlbwellborename, factpage_url)
            scrape_drilling_fluid(supabase, wlbwellborename, factpage_url)
            scrape_general_info(supabase, wlbwellborename, factpage_url)

            # Mark as completed after successful scraping
            supabase.table('wellbore_data')\
                .update({'status': 'completed', 'needs_rescrape': False})\
                .eq('wlbwellborename', wlbwellborename)\
                .execute()
            logging.info(f"Successfully scraped and updated well '{wlbwellborename}'.")
        except Exception as scrape_exception:
            logging.error(f"Failed to scrape wellbore '{wlbwellborename}': {scrape_exception}", exc_info=True)
            # Mark as error if scraping fails
            try:
                supabase.table('wellbore_data')\
                    .update({'status': 'error', 'needs_rescrape': False})\
                    .eq('wlbwellborename', wlbwellborename)\
                    .execute()
                logging.info(f"Set status to 'error' for well '{wlbwellborename}'.")
            except Exception as e:
                logging.error(f"Exception while setting status to 'error' for well '{wlbwellborename}': {e}", exc_info=True)

    logging.info("Completed scrape_factpages process.")
