import os
from dotenv import load_dotenv

load_dotenv()  # reads

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_ENV = os.getenv("PINECONE_ENV")
PINECONE_INDEX = os.getenv("PINECONE_INDEX", "rare-dx")
PINECONE_DIM = int(os.getenv("PINECONE_DIM", "3072"))
NAMESPACE = os.getenv("NAMESPACE", "diseases")
EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-large")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
