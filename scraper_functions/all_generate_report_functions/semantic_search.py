# all_generate_report_functions/semantic_search.py

import logging
from utils.openai_helpers import get_embedding
from supabase import Client
from typing import Dict, List

def perform_semantic_search(supabase: Client, raw_data: Dict, input_lat: float, input_lon: float) -> List[Dict]:
    """
    Perform a semantic search to find wells similar to the target well based on wellbore history.

    Args:
        supabase (Client): Supabase client.
        raw_data (Dict): Data retrieved in Stage 2, including 'well_profiles'.
        input_lat (float): Input latitude (unused in this function but kept for consistency).
        input_lon (float): Input longitude (unused in this function but kept for consistency).

    Returns:
        List[Dict]: A list of similar wells with their metadata and similarity scores.
    """
    logging.info("Stage 3: Semantic Search started.")

    # Extract the well profiles from raw_data
    well_profiles = raw_data.get('well_profiles', {})
    if not well_profiles:
        logging.error("No well profiles available for semantic search.")
        return []

    # Prepare the target well's content (e.g., concatenated wellbore history of the first well)
    first_well_name = next(iter(well_profiles))
    target_well_history = well_profiles[first_well_name].get('wellbore_history', [])

    if not target_well_history:
        logging.error(f"No wellbore history available for well {first_well_name}.")
        return []

    # Concatenate all sections of the wellbore history
    target_content = ' '.join([section['content'] for section in target_well_history if section.get('content')])

    # Generate embedding for the target content
    try:
        target_embedding = get_embedding(target_content)
    except Exception as e:
        logging.error(f"Error generating embedding for target content: {e}")
        return []

    # Perform semantic search in the wellbore_history_embeddings table
    try:
        # Convert embedding to the format expected by Supabase (list of floats)
        embedding_list = target_embedding

        # Perform the RPC call to the database function
        response = supabase.rpc('match_wellbore_history', {
            'embedding': embedding_list,
            'match_count': 10  # Adjust the number of results as needed
        }).execute()

        similar_wells = response.data
        logging.info(f"Found {len(similar_wells)} similar wells via semantic search.")

    except Exception as e:
        logging.error(f"Error performing semantic search: {e}")
        return []

    logging.info("Stage 3: Semantic Search completed.")
    return similar_wells
