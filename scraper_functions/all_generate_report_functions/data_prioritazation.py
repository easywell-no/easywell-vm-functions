# all_generate_report_functions/data_prioritazation.py

import logging
from typing import List, Dict

def prioritize_data(closest_wells: List[Dict], similar_wells: List[Dict]) -> List[Dict]:
    """
    Prioritize wells based on proximity and semantic similarity.

    Args:
        closest_wells (List[Dict]): List of closest wells based on distance.
        similar_wells (List[Dict]): List of wells similar in wellbore history.

    Returns:
        List[Dict]: Prioritized list of wells.
    """
    logging.info("Stage 4: Data Prioritization started.")

    # Create a dictionary to hold combined scores
    well_scores = {}

    # Add closest wells with a proximity score
    for well in closest_wells:
        well_name = well['well_name']
        distance = well['distance_km']
        proximity_score = max(0, 1 - (distance / 100))  # Adjust scaling as needed
        if well_name not in well_scores:
            well_scores[well_name] = {'score': proximity_score, 'well_data': well}
        else:
            well_scores[well_name]['score'] += proximity_score

    # Add similar wells with a similarity score
    for well in similar_wells:
        well_name = well['wlbwellborename']
        similarity_score = well['similarity']  # This should be between 0 and 1
        if well_name not in well_scores:
            well_scores[well_name] = {'score': similarity_score, 'well_data': well}
        else:
            well_scores[well_name]['score'] += similarity_score

    # Convert to list and sort by score
    prioritized_wells = sorted(well_scores.values(), key=lambda x: x['score'], reverse=True)

    logging.info("Stage 4: Data Prioritization completed.")
    return prioritized_wells
