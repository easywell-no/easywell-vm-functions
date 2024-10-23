# report_compilation.py

import logging
from typing import List, Dict

def compile_report(nearby_well_profiles: List[Dict], similar_well_profiles: List[Dict], ai_insight_text: str) -> Dict:
    """
    Compiles the report content.
    """
    logging.info("Compiling the report.")

    report = {
        'title': 'Pre-Well Drilling Analysis Report',
        'summary': 'This report provides a comprehensive analysis of the proposed drilling site, including nearby wells and AI-driven risk assessments.',
        'nearby_wells': {},
        'similar_wells': {},
        'ai_insights': ai_insight_text
    }

    for profile in nearby_well_profiles:
        well_name = profile['wlbwellborename']
        well_data = {
            'well_profile': profile['well_profile']
        }
        report['nearby_wells'][well_name] = well_data

    for profile in similar_well_profiles:
        well_name = profile['wlbwellborename']
        well_data = {
            'well_profile': profile['well_profile']
        }
        report['similar_wells'][well_name] = well_data

    return report
