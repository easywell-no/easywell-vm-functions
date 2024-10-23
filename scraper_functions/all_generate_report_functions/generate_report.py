# generate_report.py

import logging
import os
import sys

# Adjust sys.path to include parent directory for utils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import input_handler
import data_retrieval
import ai_insights
import report_compilation
import report_delivery

from utils.get_supabase_client import get_supabase_client

# ------------------------
# Configure Logging
# ------------------------

# Ensure the logs directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    filename='logs/generate_report.log',
    level=logging.DEBUG,  # Set to DEBUG to capture detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Set to DEBUG
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
    # Step 2.1: Get 3 closest wells based on location
    try:
        nearby_wells = data_retrieval.get_nearby_wells(
            supabase, input_lat, input_lon, radius_km=50, limit=3
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
            supabase, nearby_wells, top_k=3
        )
        if not similar_wells:
            logging.error("No similar wells found. Exiting.")
            return
        logging.info(f"Retrieved {len(similar_wells)} similar wells.")
    except Exception as e:
        logging.error(f"Error retrieving similar wells: {e}")
        return

    # Stage 2.3: Retrieve well profiles for nearby wells
    try:
        nearby_well_profiles = data_retrieval.get_well_profiles(supabase, nearby_wells)
        if not nearby_well_profiles:
            logging.error("Failed to retrieve nearby well profiles. Exiting.")
            return
    except Exception as e:
        logging.error(f"Error retrieving nearby well profiles: {e}")
        return

    # Retrieve well profiles for similar wells
    try:
        similar_well_profiles = data_retrieval.get_well_profiles(supabase, similar_wells)
        if not similar_well_profiles:
            logging.error("Failed to retrieve similar well profiles. Exiting.")
            return
    except Exception as e:
        logging.error(f"Error retrieving similar well profiles: {e}")
        return

    # Combine all well profiles for AI insights
    all_well_profiles = nearby_well_profiles + similar_well_profiles

    # Stage 3: AI-Driven Insights
    try:
        ai_insight_text = ai_insights.generate_ai_insights(all_well_profiles, input_lat, input_lon)
        if not ai_insight_text:
            logging.error("Failed to generate AI insights. Exiting.")
            return
    except Exception as e:
        logging.error(f"Error generating AI insights: {e}")
        return

    # Stage 4: Report Compilation
    try:
        report = report_compilation.compile_report(
            nearby_well_profiles,
            similar_well_profiles,
            ai_insight_text
        )
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
