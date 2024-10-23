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

def construct_combined_prompt(nearby_profiles, similar_profiles, user_location):
    """
    Constructs a prompt for generating a pre-well analysis report.
    """
    context_sections = []
    # Include nearby wells with distances
    context_sections.append("**Nearby Wells:**")
    for profile in nearby_profiles:
        well_name = profile['wlbwellborename']
        distance = profile['distance']
        well_profile = profile['well_profile']
        section = f"Well {well_name} (Distance: {distance:.2f} km):\n{well_profile}"
        context_sections.append(section)

    # Include similar wells
    context_sections.append("\n**Similar Wells (Semantic Search):**")
    for profile in similar_profiles:
        well_name = profile['wlbwellborename']
        similarity_score = profile.get('similarity_score', 0)
        well_profile = profile['well_profile']
        section = f"Well {well_name} (Similarity Score: {similarity_score:.2f}):\n{well_profile}"
        context_sections.append(section)

    context = "\n\n".join(context_sections)

    prompt = f"""
You are an expert petroleum engineer specializing in drilling operations.

Using the information from the wells provided below, generate a detailed and structured pre-well analysis report for a new drilling location at latitude {user_location['latitude']} and longitude {user_location['longitude']}. The report should be in **Markdown** format and include the following sections:

1. **Introduction**: Brief overview of the proposed drilling site.

2. **Wells Used in Analysis**: List of nearby wells and similar wells used in the analysis.

3. **Expected Geological Formations**: Based **only on the nearby wells**, list the expected geological formations with approximate depths. Present this information in a clear manner, such as a list or table.

4. **Potential Risks**: Identify and elaborate on potential risks based on the data from nearby and similar wells. Tie each risk to specific wells and formations, including depths where available.

5. **Drilling Challenges**: Highlight any drilling challenges observed in the nearby wells, including issues like lost circulation, stuck pipe incidents, high-pressure zones, etc. Reference the specific wells and formations where these challenges occurred.

6. **Comparison of Similar Wells**: Explain why the wells in the semantic search are similar to the nearby wells and how they are relevant to the proposed drilling operation. Use logical reasoning, such as similar formations, drilling conditions, or proximity.

7. **Recommendations**: Provide actionable recommendations for safe and efficient drilling operations, tying each recommendation to specific formations, depths, or situations, and referencing relevant wells.

8. **Conclusion**: Provide a detailed summary of the key findings and emphasize critical points for the drilling team.

Ensure that the report references specific wells and data where appropriate, and provides insights into why certain wells are more relevant to the proposed drilling operation.

Well Data:
{context}

Pre-Well Analysis Report:
"""
    return prompt.strip()

def generate_pre_well_analysis_report(prompt):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=5000,  # Increased max_tokens to allow for more detailed output
            temperature=0.5
        )
        report = response['choices'][0]['message']['content'].strip()
        return report
    except Exception as e:
        print(f"Error generating pre-well analysis report: {e}")
        return "Report not available due to an error."

def generate_ai_insights(nearby_profiles, similar_profiles, input_lat, input_lon):
    """
    Generates AI-driven insights using the well profiles.
    """
    # Construct prompt
    user_location = {'latitude': input_lat, 'longitude': input_lon}
    prompt = construct_combined_prompt(nearby_profiles, similar_profiles, user_location)

    # Generate report
    report = generate_pre_well_analysis_report(prompt)

    return report
