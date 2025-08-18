import sys
import pandas as pd
from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional,LiteralString, cast
from pathlib import Path

from src.utils.exception import CustomException
from src.utils.logging import logger
from src.config.project_config import Neo4jConfig

class Neo4jService:
    """
    Service class for loading Orphanet data into Neo4j
    """
    def __init__(self, config: Neo4jConfig) :
         self.config = config
         self.driver = None
         self.logger = logger
         self._connect()
    
    def _connect(self):
        """Establish connection to Neo4j using project config"""
        try:
            self.driver = GraphDatabase.driver(
                self.config.uri,
                auth=(self.config.user, self.config.password),
                max_connection_lifetime=3600,
                max_connection_pool_size=50,
                connection_acquisition_timeout=60
                )
            self.logger.info(f"Connected to Neo4j at {self.config.uri}")
        except Exception as e:
            self.driver = None
            raise CustomException(f"Neo4j connection failed: {e}", e)
        
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            self.logger.info("Neo4j connection closed")
    
    def test_connection(self):
        if not self.driver:
            raise CustomException("Driver connection was not established", None)
        try:
            with self.driver.session() as session:
             result = session.run("RETURN 1 as test")
             record = result.single()
             success = record and record["test"] == 1
             self.logger.info("Neo4j connection test successful")
            return success
        except Exception as e:
            self.logger.error(f"Neo4j connection test failed: {e}")
            raise CustomException(f"Neo4j connection test failed: {e}", e)

        
    def execute_query(self, query:str, parameters: Optional[Dict[str,Any]]= None) -> List[Dict]:
        if not self.driver:
            raise CustomException("Driver connection was not established", None)
        try:
            with self.driver.session() as session:
                result = session.run(cast("LiteralString",query), parameters or {})
                records = [record.data() for record in result]
                self.logger.debug(f"Ouery excuted successfully, returned {len(records)}")
                return records
        except Exception as e:
            raise CustomException(f"Query excution failed: {e}", e)
        
    def execute_write_transcation(self, query: str, parameters: Optional[Dict[str, Any]]= None) -> Dict:
        if not self.driver:
            raise CustomException("Driver connection was not established", None)
        try:
            with self.driver.session() as session:
                result = session.execute_write(self._run_query, query, parameters or {})
                return result
        except Exception as e:
            raise CustomException(f"Write transaction failed: {e}", e)
    
    @staticmethod
    def _run_query(tx, query:str, parameters: Dict[str, Any]):
        result = tx.run(query, parameters)
        summary = result.consume()
        return {
            'nodes_created': summary.counters.nodes_created,
            'nodes_deleted': summary.counters.nodes_deleted,
            'relationships_created': summary.counters.relationships_created,
            'relationships_deleted': summary.counters.relationships_deleted,
            'properties_set': summary.counters.properties_set,
            'indexes_added': summary.counters.indexes_added,
            'constraints_added': summary.counters.constraints_added
        }
    
    def clear_database(self, confirm = False):
        if not confirm:
            raise CustomException("Database clear operation requires confirmation", None)
        
        self.logger.warning("CLEARING ENTIRE NEO4J DATABASE")
        
        try:
            # Delete all relationships first
            self.execute_write_transcation("MATCH ()-[r]-() DELETE r")
            # Then delete all nodes
            self.execute_write_transcation("MATCH (n) DELETE n")
            self.logger.info("Database cleared successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to clear database: {e}")
            raise CustomException(f"Database clear failed: {e}", e)
                
                
                
if __name__ == "__main__":
    
    try:
        neo4j_config = Neo4jConfig()
        service = Neo4jService(neo4j_config)
        
        if service.test_connection():
            print("✅ Neo4j service initialized successfully!")
        
        service.close()
        
    except CustomException as e:
        print(f"Service initialization failed: {e}")