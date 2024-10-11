# all_generate_report_functions/report_compilation.py

import logging

def compile_report(well_profiles, ai_insight_text):
    """
    Compile the report content from well profiles and AI-generated insights.
    Args:
        well_profiles (Dict[str, Dict]): Dictionary of well profiles.
        ai_insight_text (str): AI-generated insights text.
    Returns:
        Dict: Compiled report content.
    """
    logging.info("Stage 4: Report Compilation started.")

    report = {
        'title': 'Pre-Well Drilling Report',
        'summary': 'This report provides a comprehensive analysis of the proposed drilling site, including nearby wells and AI-driven risk assessments.',
        'wells': well_profiles,
        'ai_insights': ai_insight_text
    }

    logging.info("Report content compiled successfully.")
    logging.info("Stage 4: Report Compilation completed.")
    return report
