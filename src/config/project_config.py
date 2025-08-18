import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

    
@dataclass
class OpenAIConfig:
    api_key: str = os.getenv("OPENAI_API_KEY", "")
    embedding_model: str = "text-embedding-3-small"
    embedding_dimension: int = 1536
    
@dataclass
class PineconeConfig:
    api_key: str = os.getenv("PINECONE_API_KEY", "")
    environment: str = os.getenv("PINECONE_ENVIRONMENT", "us-east-1")
    index_name: str = "vectordb"
    dimension: int = 1536
    
@dataclass
class Neo4jConfig:
    uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user: str = os.getenv("NEO4J_USER", "neo4j")
    password: str = os.getenv("NEO4J_PASSWORD", "")
    
@dataclass
class ProjectConfig:
    openai: OpenAIConfig = field(default_factory = OpenAIConfig)
    pinecone: PineconeConfig = field(default_factory=PineconeConfig)
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    
    