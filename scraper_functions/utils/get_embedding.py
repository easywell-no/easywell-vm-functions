# utils/get_embedding.py

import logging
from typing import List
import openai

def get_embedding(text: str, model: str = "text-embedding-ada-002") -> List[float]:
    """
    Generate an embedding for the given text using OpenAI's API.

    Args:
        text (str): The text to embed.
        model (str): The OpenAI embedding model to use.

    Returns:
        List[float]: The embedding vector.
    """
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
