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
        # Select wells that need rescraping and are in 'waiting' status
        response = supabase.table('wellbore_data')\
            .select('wlbwellborename, wlbfactpageurl, wlbwelltype')\
            .eq('needs_rescrape', True)\
            .eq('status', 'waiting')\
            .execute()

        wells_to_scrape = response.data
        if not wells_to_scrape:
            logging.info("No wells require rescraping at this time.")
            return

        logging.info(f"Found {len(wells_to_scrape)} wells to scrape.")
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
                update_response = supabase.table('wellbore_data')\
                    .update({'status': 'completed', 'needs_rescrape': False})\
                    .eq('wlbwellborename', wlbwellborename)\
                    .execute()
                if update_response.error:
                    logging.error(f"Failed to update non-exploration well '{wlbwellborename}': {update_response.error}")
                else:
                    logging.info(f"Non-exploration well '{wlbwellborename}' marked as completed and skipped.")
            except Exception as e:
                logging.error(f"Exception while updating non-exploration well '{wlbwellborename}': {e}", exc_info=True)
            continue

        logging.info(f"Reserving wellbore '{wlbwellborename}' for scraping.")

        try:
            # Set the status to 'reserved' before starting scraping
            update_response = supabase.table('wellbore_data')\
                .update({'status': 'reserved'})\
                .eq('wlbwellborename', wlbwellborename)\
                .execute()
            if update_response.error:
                logging.error(f"Failed to set status to 'reserved' for well '{wlbwellborename}': {update_response.error}")
                continue  # Skip to the next well

            # Perform scraping for the well
            scrape_wellbore_history(supabase, wlbwellborename, factpage_url)
            scrape_lithostratigraphy(supabase, wlbwellborename, factpage_url)
            scrape_casing_and_tests(supabase, wlbwellborename, factpage_url)
            scrape_drilling_fluid(supabase, wlbwellborename, factpage_url)
            scrape_general_info(supabase, wlbwellborename, factpage_url)

            # Mark as completed after successful scraping
            update_response = supabase.table('wellbore_data')\
                .update({'status': 'completed', 'needs_rescrape': False})\
                .eq('wlbwellborename', wlbwellborename)\
                .execute()
            if update_response.error:
                logging.error(f"Failed to set status to 'completed' for well '{wlbwellborename}': {update_response.error}")
            else:
                logging.info(f"Successfully scraped and updated well '{wlbwellborename}'.")
        except Exception as scrape_exception:
            logging.error(f"Failed to scrape wellbore '{wlbwellborename}': {scrape_exception}", exc_info=True)
            # Mark as error if scraping fails
            try:
                update_response = supabase.table('wellbore_data')\
                    .update({'status': 'error', 'needs_rescrape': False})\
                    .eq('wlbwellborename', wlbwellborename)\
                    .execute()
                if update_response.error:
                    logging.error(f"Failed to set status to 'error' for well '{wlbwellborename}': {update_response.error}")
            except Exception as e:
                logging.error(f"Exception while setting status to 'error' for well '{wlbwellborename}': {e}", exc_info=True)

    logging.info("Completed scrape_factpages process.")
