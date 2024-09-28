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
        # Step 1: Select wells that need rescraping, are in 'waiting' status, and are exploration wells
        response = supabase.table('wellbore_data')\
            .select('wlbwellborename, wlbfactpageurl, wlbwelltype')\
            .eq('needs_rescrape', True)\
            .eq('status', 'waiting')\
            .execute()

        if response.data:
            wells_to_scrape = response.data
            logging.info(f"Found {len(wells_to_scrape)} wells needing rescrape.")
        else:
            logging.info("No wells require rescraping at this time.")
            return

    except Exception as e:
        logging.error(f"Error querying wells for rescrape: {e}", exc_info=True)
        return

    for well in wells_to_scrape:
        wlbwellborename = well.get('wlbwellborename')
        factpage_url = well.get('wlbfactpageurl')
        well_type = well.get('wlbwelltype')

        if not factpage_url:
            logging.warning(f"No factpage URL for wellbore '{wlbwellborename}'. Skipping.")
            continue

        logging.info(f"Processing wellbore '{wlbwellborename}'.")

        # Only process exploration wells
        if well_type != 'EXPLORATION':
            try:
                logging.info(f"Well '{wlbwellborename}' is not an exploration well. Marking as 'completed'.")
                non_exploration_update_response = supabase.table('wellbore_data')\
                    .update({'status': 'completed', 'needs_rescrape': False})\
                    .eq('wlbwellborename', wlbwellborename)\
                    .execute()

                if non_exploration_update_response.status_code == 200:
                    logging.info(f"Marked well '{wlbwellborename}' as completed.")
                else:
                    logging.error(f"Failed to mark well '{wlbwellborename}' as completed. Status: {non_exploration_update_response.status_code}")

            except Exception as update_exception:
                logging.error(f"Failed to update non-exploration well '{wlbwellborename}' to 'completed': {update_exception}", exc_info=True)
            continue

        try:
            # Step 2: Reserve the well for scraping by updating its status to 'reserved'
            update_status_response = supabase.table('wellbore_data')\
                .update({'status': 'reserved'})\
                .eq('wlbwellborename', wlbwellborename)\
                .execute()

            # Check if the update was successful
            if update_status_response.status_code == 200:
                logging.info(f"Reserved wellbore '{wlbwellborename}' for scraping.")
            else:
                logging.error(f"Failed to reserve wellbore '{wlbwellborename}'. Status: {update_status_response.status_code}")
                continue  # Skip to the next well

            # Step 3: Perform scraping
            try:
                scrape_wellbore_history(supabase, wlbwellborename, factpage_url)
                scrape_lithostratigraphy(supabase, wlbwellborename, factpage_url)
                scrape_casing_and_tests(supabase, wlbwellborename, factpage_url)
                scrape_drilling_fluid(supabase, wlbwellborename, factpage_url)
                scrape_general_info(supabase, wlbwellborename, factpage_url)
            except Exception as scrape_exception:
                logging.error(f"Scraping failed for wellbore '{wlbwellborename}': {scrape_exception}", exc_info=True)

                # Step 4a: On scraping failure, update status to 'error' and set needs_rescrape to False
                try:
                    error_update_response = supabase.table('wellbore_data')\
                        .update({'status': 'error', 'needs_rescrape': False})\
                        .eq('wlbwellborename', wlbwellborename)\
                        .execute()

                    if error_update_response.status_code == 200:
                        logging.info(f"Updated wellbore '{wlbwellborename}' status to 'error' due to scraping failure.")
                    else:
                        logging.error(f"Failed to update status to 'error' for wellbore '{wlbwellborename}'. Status: {error_update_response.status_code}")
                except Exception as update_exception:
                    logging.error(f"Failed to update status to 'error' for wellbore '{wlbwellborename}': {update_exception}", exc_info=True)
                continue  # Move to the next well

            # Step 4b: On successful scraping, update needs_rescrape to False and status to 'completed'
            try:
                complete_update_response = supabase.table('wellbore_data')\
                    .update({'needs_rescrape': False, 'status': 'completed'})\
                    .eq('wlbwellborename', wlbwellborename)\
                    .execute()

                if complete_update_response.status_code == 200:
                    logging.info(f"Marked well '{wlbwellborename}' as completed.")
                else:
                    logging.error(f"Failed to mark well '{wlbwellborename}' as completed. Status: {complete_update_response.status_code}")

            except Exception as update_exception:
                logging.error(f"Failed to update wellbore '{wlbwellborename}' after scraping: {update_exception}", exc_info=True)

        except Exception as e:
            logging.error(f"Unexpected error processing wellbore '{wlbwellborename}': {e}", exc_info=True)

    logging.info("Completed scrape_factpages process.")
