# all_generate_report_functions/ai_insights.py

import logging
import os
import openai
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Set OpenAI API key
openai.api_key = os.getenv('OPENAI_API_KEY')

def generate_ai_insights(well_profiles):
    """
    Generate AI-driven insights based on well profiles.
    Args:
        well_profiles (Dict[str, Dict]): Dictionary of well profiles.
    Returns:
        str: AI-generated insights text.
    """
    logging.info("Stage 4: AI-Driven Insights generation started.")

    # Construct the prompt
    prompt = "You are a geologist tasked with performing a risk analysis for a new well drilling project. " \
             "Based on the following information from nearby wells, provide a comprehensive risk assessment.\n\n"

    for well_name, profile in well_profiles.items():
        prompt += f"Well Name: {well_name}\n"
        prompt += f"Distance: {profile.get('distance_km', 'N/A')} km\n"
        prompt += f"General Info: {profile.get('general_info', 'N/A')}\n"
        prompt += f"Wellbore History: {profile.get('wellbore_history', [])}\n"
        prompt += f"Lithostratigraphy: {profile.get('lithostratigraphy', [])}\n"
        prompt += f"Casing and Tests: {profile.get('casing_and_tests', [])}\n"
        prompt += f"Drilling Fluid: {profile.get('drilling_fluid', [])}\n"
        prompt += "---\n"

    prompt += "\nProvide a detailed risk analysis for the new well based on the above information."

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant specialized in geological risk analysis."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.7,
        )
        ai_insight_text = response.choices[0].message['content'].strip()
        logging.info("AI-driven insights generated successfully.")
    except Exception as e:
        logging.error(f"Failed to generate AI insights: {e}")
        ai_insight_text = "Failed to generate AI insights due to an error."

    logging.info("Stage 4: AI-Driven Insights generation completed.")
    return ai_insight_text
