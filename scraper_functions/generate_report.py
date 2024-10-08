# generate_report.py

import logging
import os

# Importing modules from all_generate_report_functions
from all_generate_report_functions import (
    input_handler,
    data_retrieval,
    semantic_search,
    data_prioritazation,
    ai_insights,
    report_compilation,
    report_delivery
)

# Import the supabase client from utils
from utils import get_supabase_client

# ------------------------
# Configure Logging
# ------------------------

logging.basicConfig(
    filename='logs/generate_report.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
    raw_data = data_retrieval.fetch_well_data(supabase, input_lat, input_lon)
    if raw_data is None:
        logging.error("Data retrieval failed. Exiting.")
        return

    # Stage 3: Semantic Search
    similar_wells = semantic_search.perform_semantic_search(supabase, raw_data, input_lat, input_lon)

    # Stage 4: Data Prioritization
    prioritized_data = data_prioritazation.prioritize_data(raw_data['closest_wells'], similar_wells)

    # Stage 5: AI-Driven Insights
    ai_insight_text = ai_insights.generate_ai_insights(prioritized_data)

    # Stage 6: Report Compilation
    report = report_compilation.compile_report(raw_data, ai_insight_text)

    # Stage 7: Report Delivery
    report_delivery.deliver_report(report)

    logging.info("Report generation process completed.")

if __name__ == "__main__":
    main()
