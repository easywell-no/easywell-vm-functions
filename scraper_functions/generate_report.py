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
    well_names = data_retrieval.fetch_well_names(supabase, input_lat, input_lon)
    if not well_names:
        logging.error("No wells found within the specified criteria. Exiting.")
        return

    # Stage 3: Get Well Profiles
    well_profiles = data_retrieval.get_well_profiles(well_names, supabase)
    if not well_profiles:
        logging.error("Failed to retrieve well profiles. Exiting.")
        return

    # Stage 4: AI-Driven Insights
    ai_insight_text = ai_insights.generate_ai_insights(well_profiles)
    if not ai_insight_text:
        logging.error("Failed to generate AI insights. Exiting.")
        return

    # Stage 5: Report Compilation
    report = report_compilation.compile_report(well_profiles, ai_insight_text)
    if not report:
        logging.error("Failed to compile the report. Exiting.")
        return

    # Stage 6: Report Delivery
    report_delivery.deliver_report(report)

    logging.info("Report generation process completed successfully.")

if __name__ == "__main__":
    main()
