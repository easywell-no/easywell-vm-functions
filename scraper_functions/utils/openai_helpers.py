# utils/openai_helpers.py

import openai
import os

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

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
            input=text,
            model=model
        )
        embedding = response['data'][0]['embedding']
        return embedding
    except Exception as e:
        raise e
