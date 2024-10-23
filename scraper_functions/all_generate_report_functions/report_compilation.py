# report_compilation.py

def compile_report(nearby_well_profiles, similar_well_profiles, ai_insight_text):
    """
    Compiles the report content.
    """
    report = {
        'title': 'Pre-Well Drilling Analysis Report',
        'summary': 'This report provides a comprehensive analysis of the proposed drilling site, including nearby wells and AI-driven risk assessments.',
        'nearby_wells': nearby_well_profiles,
        'similar_wells': similar_well_profiles,
        'ai_insights': ai_insight_text
    }
    return report
