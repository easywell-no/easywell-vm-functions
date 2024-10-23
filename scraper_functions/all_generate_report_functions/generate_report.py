# generate_report.py

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

def main():
    print("Report generation process started.")

    # Initialize Supabase client
    try:
        supabase = get_supabase_client()
    except Exception as e:
        print(f"Failed to initialize Supabase client: {e}")
        return

    # Stage 1: Receive Input Coordinates
    user_input = input_handler.get_user_input()
    if user_input is None:
        print("No valid input received. Exiting.")
        return
    input_lat, input_lon = user_input['latitude'], user_input['longitude']

    # Stage 2: Data Retrieval
    # Step 2.1: Get 3 closest wells based on location
    nearby_wells = data_retrieval.get_nearby_wells(
        supabase, input_lat, input_lon, radius_km=50, limit=3
    )
    if not nearby_wells:
        print("No nearby wells found. Exiting.")
        return

    # Step 2.2: Use nearby wells for similarity search to get additional wells
    similar_wells = data_retrieval.get_similar_wells(
        supabase, nearby_wells, top_k=3
    )
    if not similar_wells:
        print("No similar wells found. Exiting.")
        return

    # Stage 2.3: Retrieve well profiles for nearby wells
    nearby_well_profiles = data_retrieval.get_well_profiles(supabase, nearby_wells)
    if not nearby_well_profiles:
        print("Failed to retrieve nearby well profiles. Exiting.")
        return

    # Retrieve well profiles for similar wells
    similar_well_profiles = data_retrieval.get_well_profiles(supabase, similar_wells)
    if not similar_well_profiles:
        print("Failed to retrieve similar well profiles. Exiting.")
        return

    # Combine all well profiles for AI insights
    all_well_profiles = nearby_well_profiles + similar_well_profiles

    # Stage 3: AI-Driven Insights
    ai_insight_text = ai_insights.generate_ai_insights(all_well_profiles, input_lat, input_lon)
    if not ai_insight_text:
        print("Failed to generate AI insights. Exiting.")
        return

    # Stage 4: Report Compilation
    report = report_compilation.compile_report(
        nearby_well_profiles,
        similar_well_profiles,
        ai_insight_text
    )
    if not report:
        print("Failed to compile the report. Exiting.")
        return

    # Stage 5: Report Delivery
    report_delivery.deliver_report(report)

    print("Report generation process completed successfully.")

if __name__ == "__main__":
    main()
