# all_generate_report_functions/report_compilation.py

import logging
from typing import List, Dict

def compile_report(well_profiles: List[Dict], ai_insight_text: str) -> Dict:
    """
    Compiles the report content.
    """
    logging.info("Compiling the report.")

    report = {
        'title': 'Pre-Well Drilling Analysis Report',
        'summary': 'This report provides a comprehensive analysis of the proposed drilling site, including nearby wells and AI-driven risk assessments.',
        'wells': {},
        'ai_insights': ai_insight_text
    }

    for profile in well_profiles:
        well_name = profile['wlbwellborename']
        well_data = {
            'well_profile': profile['well_profile']
        }
        report['wells'][well_name] = well_data

    return report
