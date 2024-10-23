# get_embedding.py

import logging
import os
from typing import List
import openai
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Set OpenAI API key
openai.api_key = os.getenv('OPENAI_API_KEY')

def get_embedding(text: str, model: str = "text-embedding-ada-002", retries: int = 3) -> List[float]:
    """
    Generate an embedding for the given text using OpenAI's API.

    Args:
        text (str): The text to embed.
        model (str): The OpenAI embedding model to use.
        retries (int): Number of retries in case of RateLimitError.

    Returns:
        List[float]: The embedding vector.
    """
    try:
        if not openai.api_key:
            logging.error("OpenAI API key is not set. Please set the OPENAI_API_KEY environment variable.")
            raise EnvironmentError("OpenAI API key is not set.")

        response = openai.Embedding.create(
            model=model,
            input=text
        )
        embedding = response['data'][0]['embedding']
        if not isinstance(embedding, list) or not all(isinstance(x, (float, int)) for x in embedding):
            raise ValueError("Embedding is not a list of floats.")
        return embedding
    except openai.error.RateLimitError as e:
        if retries > 0:
            logging.error(f"OpenAI API rate limit exceeded: {e}. Retrying in 10 seconds. Retries left: {retries}")
            import time
            time.sleep(10)
            return get_embedding(text, model, retries - 1)
        else:
            logging.error("Max retries exceeded due to rate limits.")
            raise e
    except openai.error.OpenAIError as e:
        logging.error(f"An OpenAI error occurred: {e}")
        raise e
    except Exception as e:
        logging.error(f"Error in get_embedding: {e}")
        raise e
