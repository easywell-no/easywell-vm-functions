# ai_insights.py

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
    print("OPENAI_API_KEY is not set. Please set it in your environment variables.")
openai.api_key = OPENAI_API_KEY

def summarize_text(text, max_tokens=150):
    """
    Summarizes the given text using OpenAI's summarization capability.
    """
    prompt = f"Summarize the following well profile in a concise manner, focusing on geological formations, drilling challenges, and any issues encountered:\n\n{text}\n\nSummary:"
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.5
        )
        summary = response['choices'][0]['message']['content'].strip()
        return summary
    except Exception as e:
        print(f"Error summarizing text: {e}")
        return "Summary not available due to an error."

def get_summarized_well_profiles(well_profiles):
    summarized_profiles = []
    for profile in well_profiles:
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

Using the information from the wells provided below, generate a detailed and structured pre-well analysis report for a new drilling location at latitude {user_location['latitude']} and longitude {user_location['longitude']}. The report should be in **Markdown** format and include the following sections:

1. **Introduction**: Brief overview of the proposed drilling site.
2. **Potential Risks**: Identify and elaborate on potential risks based on the data from nearby and similar wells.
3. **Expected Geological Formations**: Describe the expected geological formations and any associated challenges.
4. **Drilling Challenges**: Highlight any drilling challenges observed in the nearby wells, including issues like lost circulation, stuck pipe incidents, high-pressure zones, etc.
5. **Recommendations**: Provide actionable recommendations for safe and efficient drilling operations, considering the identified risks and challenges.
6. **Conclusion**: Summarize the key findings and emphasize critical points for the drilling team.

Ensure that the report references specific wells and data where appropriate, and provides insights into why certain wells are more relevant to the proposed drilling operation.

Well Summaries:
{context}

Pre-Well Analysis Report:
"""
    return prompt.strip()

def generate_pre_well_analysis_report(prompt):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2500,
            temperature=0.7
        )
        report = response['choices'][0]['message']['content'].strip()
        return report
    except Exception as e:
        print(f"Error generating pre-well analysis report: {e}")
        return "Report not available due to an error."

def generate_ai_insights(well_profiles, input_lat, input_lon):
    """
    Generates AI-driven insights using the well profiles.
    """
    # Summarize well profiles
    summarized_profiles = get_summarized_well_profiles(well_profiles)

    # Construct prompt
    user_location = {'latitude': input_lat, 'longitude': input_lon}
    prompt = construct_combined_prompt(summarized_profiles, user_location)

    # Generate report
    report = generate_pre_well_analysis_report(prompt)

    return report
