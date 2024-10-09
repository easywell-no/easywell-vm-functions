# utils/get_embedding.py

import logging
from typing import List
import openai  # Ensure you have the OpenAI Python SDK installed (pip install openai)

def get_embedding(text: str, model: str = "text-embedding-ada-002") -> List[float]:
    try:
        response = openai.Embedding.create(
            model=model,
            input=text
        )
        embedding = response['data'][0]['embedding']
        return embedding
    except Exception as e:
        logging.error(f"Error in get_embedding: {e}")
        raise e
