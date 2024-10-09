# vectorize_wellbore_history.py

import logging
from supabase import create_client
from utils.openai_helpers import get_embedding
import os

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # Use service role for admin privileges
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def vectorize_wellbore_history():
    logging.info("Starting vectorization of wellbore history.")

    # Fetch all records from wellbore_history
    response = supabase.table('wellbore_history').select('*').execute()
    records = response.data

    logging.info(f"Fetched {len(records)} records from wellbore_history.")

    for record in records:
        wlbwellborename = record['wlbwellborename']
        section = record['section']
        content = record['content']

        # Check if the embedding already exists
        existing = supabase.table('wellbore_history_embeddings').select('id').match({
            'wlbwellborename': wlbwellborename,
            'section': section
        }).execute()

        if existing.data:
            logging.info(f"Embedding already exists for {wlbwellborename} - {section}, skipping.")
            continue

        # Generate embedding
        try:
            embedding = get_embedding(content)
        except Exception as e:
            logging.error(f"Error generating embedding for {wlbwellborename} - {section}: {e}")
            continue

        # Insert into wellbore_history_embeddings
        data = {
            'wlbwellborename': wlbwellborename,
            'section': section,
            'content': content,
            'embedding': embedding
        }
        try:
            supabase.table('wellbore_history_embeddings').insert(data).execute()
            logging.info(f"Inserted embedding for {wlbwellborename} - {section}.")
        except Exception as e:
            logging.error(f"Error inserting embedding for {wlbwellborename} - {section}: {e}")

    logging.info("Completed vectorization of wellbore history.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    vectorize_wellbore_history()
