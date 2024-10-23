# ai_insights.py

import logging
import openai
import os
import sys

# Adjust sys.path to include utils directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(parent_dir, 'utils')
sys.path.append(utils_dir)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
if not OPENAI_API_KEY:
    logging.error("OPENAI_API_KEY is not set. Please set it in your environment variables.")
openai.api_key = OPENAI_API_KEY

def summarize_text(text, max_tokens=100):
    """
    Summarizes the given text using OpenAI's summarization capability.
    """
    prompt = f"Summarize the following well profile:\n\n{text}\n\nSummary:"
    try:
        logging.info("Summarizing text with OpenAI API.")
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.5
        )
        summary = response['choices'][0]['message']['content'].strip()
        logging.info(f"Summary generated for well profile: {summary[:100]}...")
        return summary
    except Exception as e:
        logging.error(f"Error summarizing text: {e}")
        return "Summary not available due to an error."

def get_summarized_well_profiles(well_profiles):
    summarized_profiles = []
    for profile in well_profiles:
        logging.info(f"Summarizing well profile for well: {profile['wlbwellborename']}")
        summary = summarize_text(profile['well_profile'])
        summarized_profiles.append({
            'wlbwellborename': profile['wlbwellborename'],
            'summary': summary
        })
    return summarized_profiles

def construct_combined_prompt(combined_profiles, user_location):
    """
    Constructs a prompt for generating a pre-well analysis report.
    """
    context_sections = []
    for profile in combined_profiles:
        section = f"Well {profile['wlbwellborename']} Summary:\n{profile['summary']}"
        context_sections.append(section)

    context = "\n\n".join(context_sections)

    prompt = f"""
You are an expert petroleum engineer specializing in drilling operations.

Using the information from the wells provided below, generate a detailed pre-well analysis report for a new drilling location at latitude {user_location['latitude']} and longitude {user_location['longitude']}. The report should:

- Identify potential risks.
- Describe expected geological formations.
- Highlight any drilling challenges based on the data from these wells.
- Provide recommendations for safe and efficient drilling operations.

Well Summaries:
{context}

Pre-Well Analysis Report:
"""
    logging.debug(f"Constructed prompt length: {len(prompt)} characters.")
    return prompt.strip()

def generate_pre_well_analysis_report(prompt):
    try:
        logging.info("Generating pre-well analysis report with OpenAI API.")
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1000,  # Adjusted for a more detailed report
            temperature=0.7
        )
        report = response['choices'][0]['message']['content'].strip()
        logging.info(f"AI-generated report length: {len(report)} characters.")
        return report
    except Exception as e:
        logging.error(f"Error generating pre-well analysis report: {e}")
        return "Report not available due to an error."

def generate_ai_insights(well_profiles, input_lat, input_lon):
    """
    Generates AI-driven insights using the well profiles.
    """
    logging.info("Starting AI insights generation.")
    # Summarize well profiles
    summarized_profiles = get_summarized_well_profiles(well_profiles)
    logging.info("Well profiles summarized.")

    # Construct prompt
    user_location = {'latitude': input_lat, 'longitude': input_lon}
    prompt = construct_combined_prompt(summarized_profiles, user_location)
    logging.info("Prompt constructed for AI insights.")

    # Log the prompt length
    logging.debug(f"AI prompt length: {len(prompt)} characters.")

    # Generate report
    report = generate_pre_well_analysis_report(prompt)
    logging.info("AI insights generated.")

    return report
