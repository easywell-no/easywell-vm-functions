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
    prompt = (
        "You are an engineer specialized in drilling operations of oil and gas wells. "
        "Analyze the following well profiles and provide a comprehensive risk analysis report. "
        "Extract all drilling-related issues, specify the formations where they occurred, and indicate which well they pertain to. "
        "Identify other encountered issues and cite the source information from the provided data using quotes for direct references.\n\n"
        "This information will be the foundation of a detailed report for a new well in the same area as these wells.\n\n"
    )

    for well_name, profile in well_profiles.items():
        prompt += f"---\n"
        prompt += f"**Well Name**: {well_name}\n"
        prompt += f"**Distance**: {profile.get('distance_km', 'N/A')} km\n"

        general_info = profile.get('general_info') or {}
        prompt += f"**General Info**: {general_info}\n"

        wellbore_history = profile.get('wellbore_history', [])
        prompt += f"**Wellbore History**:\n"
        for history in wellbore_history:
            content = history.get('content', 'N/A').replace('\n', ' ')
            prompt += f"  - {content}\n"

        lithostratigraphy = profile.get('lithostratigraphy', [])
        prompt += f"**Lithostratigraphy**:\n"
        for litho in lithostratigraphy:
            unit = litho.get('lithostratigraphic_unit', 'N/A')
            depth = litho.get('top_depth_m_md_rkb', 'N/A')
            prompt += f"  - {unit} at {depth} m\n"

        casing_and_tests = profile.get('casing_and_tests', [])
        prompt += f"**Casing and Tests**:\n"
        for casing in casing_and_tests:
            prompt += f"  - {casing}\n"  # You can format this further if needed

        drilling_fluid = profile.get('drilling_fluid', [])
        prompt += f"**Drilling Fluid**:\n"
        for fluid in drilling_fluid:
            prompt += f"  - {fluid}\n"

    prompt += (
        "\nBased on the above information, provide a detailed risk analysis for the new well. "
        "For each identified issue, specify the formation, the well it pertains to, and include quotes from the raw data as references."
    )

    try:
        # Use the correct model name
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",  # Ensure this model name is correct and accessible
            messages=[
                {"role": "system", "content": "You are a helpful assistant specialized in geological risk analysis."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=16384,  # Adjusted to fit within model limits
            temperature=0.2,
        )
        # Access the response correctly
        ai_insight_text = response.choices[0].message['content'].strip()
        logging.info("AI-driven insights generated successfully.")
    except openai.error.OpenAIError as e:  # General exception for OpenAI errors
        logging.error(f"OpenAI Error: {e}")
        ai_insight_text = "Failed to generate AI insights due to an OpenAI error."
    except Exception as e:
        logging.error(f"Failed to generate AI insights: {e}")
        ai_insight_text = "Failed to generate AI insights due to an unexpected error."

    logging.info("Stage 4: AI-Driven Insights generation completed.")
    return ai_insight_text
