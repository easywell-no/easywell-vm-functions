# generate_report.py

import logging
import os

# Importing modules from all_generate_report_functions
from all_generate_report_functions import (
    input_handler,
    data_retrieval,
    ai_insights,
    report_compilation,
    report_delivery
)

# Correctly import the get_supabase_client function
from utils.get_supabase_client import get_supabase_client

# ------------------------
# Configure Logging
# ------------------------

# Ensure the logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    filename='logs/generate_report.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Optionally, also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

# ------------------------
# Main Function
# ------------------------

def main():
    logging.info("Report generation process started.")

    # Initialize Supabase client
    try:
        supabase = get_supabase_client()
    except Exception as e:
        logging.error(f"Failed to initialize Supabase client: {e}")
        return

    # Stage 1: Receive Input Coordinates
    user_input = input_handler.get_user_input()
    if user_input is None:
        logging.error("No valid input received. Exiting.")
        return
    input_lat, input_lon = user_input['latitude'], user_input['longitude']

    # Stage 2: Data Retrieval
    # Step 2.1: Get 5 closest wells based on location
    try:
        nearby_wells = data_retrieval.get_nearby_wells(
            supabase, input_lat, input_lon, radius_km=50, limit=5
        )
        if not nearby_wells:
            logging.error("No nearby wells found. Exiting.")
            return
        logging.info(f"Retrieved {len(nearby_wells)} nearby wells.")
    except Exception as e:
        logging.error(f"Error retrieving nearby wells: {e}")
        return

    # Step 2.2: Use nearby wells for similarity search to get additional wells
    try:
        similar_wells = data_retrieval.get_similar_wells(
            supabase, nearby_wells, top_k=5
        )
        logging.info(f"Retrieved {len(similar_wells)} similar wells.")
    except Exception as e:
        logging.error(f"Error retrieving similar wells: {e}")
        return

    # Step 2.3: Combine wells and ensure no duplicates
    all_well_names = list(set(nearby_wells + similar_wells))
    logging.info(f"Total wells to use in report: {len(all_well_names)}")

    # Step 2.4: Retrieve well profiles
    try:
        well_profiles = data_retrieval.get_well_profiles(supabase, all_well_names)
        if not well_profiles:
            logging.error("Failed to retrieve well profiles. Exiting.")
            return
    except Exception as e:
        logging.error(f"Error retrieving well profiles: {e}")
        return

    # Stage 3: AI-Driven Insights
    try:
        ai_insight_text = ai_insights.generate_ai_insights(well_profiles, input_lat, input_lon)
        if not ai_insight_text:
            logging.error("Failed to generate AI insights. Exiting.")
            return
    except Exception as e:
        logging.error(f"Error generating AI insights: {e}")
        return

    # Stage 4: Report Compilation
    try:
        report = report_compilation.compile_report(well_profiles, ai_insight_text)
        if not report:
            logging.error("Failed to compile the report. Exiting.")
            return
    except Exception as e:
        logging.error(f"Error compiling report: {e}")
        return

    # Stage 5: Report Delivery
    try:
        report_delivery.deliver_report(report)
    except Exception as e:
        logging.error(f"Error delivering report: {e}")
        return

    logging.info("Report generation process completed successfully.")

if __name__ == "__main__":
    main()
