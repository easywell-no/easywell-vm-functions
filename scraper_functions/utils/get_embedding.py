# utils/get_embedding.py

def get_embedding(text: str, model: str = "text-embedding-ada-002") -> List[float]:
    try:
        response = openai.Embedding.create(
            model=model,
            input=text
        )
        embedding = response['data'][0]['embedding']
        if not isinstance(embedding, list) or not all(isinstance(x, (float, int)) for x in embedding):
            raise ValueError("Embedding is not a list of floats.")
        return embedding
    except Exception as e:
        logging.error(f"Error in get_embedding: {e}")
        raise e
