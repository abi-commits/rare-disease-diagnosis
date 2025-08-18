from typing import List, Dict, Any
from src.services.neo4j_service import Neo4jService
from src.config.project_config import Neo4jConfig
from src.utils.logging import logger
from src.utils.exception import CustomException


class SchemaManager:
    """Manages Neo4j schema creation for Orphanet data
    Handles constraints, indexes, and schema validation
    """
    def __init__(self, neo4j_service: Neo4jService):
        self.service = neo4j_service
        self.logger = logger
        
    def create_all_constraints(self) -> Dict[str, Any]:
        """
        Create all necessary constraints for Orphanet data
        Returns:
            Dict with constraint creation results
        """
        self.logger.info("Creating Neo4j constraints for Orphanet schema")
        
        constraints = [
            # DISEASE CONSTRAINTS
            {
                'name': 'disease_orphacode_unique',
                'query': """
                CREATE CONSTRAINT disease_orphacode_unique IF NOT EXISTS
                FOR (d:Disease) REQUIRE d.orphacode IS UNIQUE
                """
            },
            {
                'name': 'disease_id_unique',
                'query': """
                CREATE CONSTRAINT disease_id_unique IF NOT EXISTS
                FOR (d:Disease) REQUIRE d.id IS UNIQUE"""
            },
            # GENE CONSTRAINTS
            {
                'name': 'gene_id_unique',
                'query': """
                CREATE CONSTRAINT gene_id_unique IF NOT EXISTS
                FOR (g:Gene) REQUIRE g.gene_id IS UNIQUE
                """
            },
            {
                'name': 'gene_symbol_unique',
                'query': """
                CREATE CONSTRAINT gene_symbol_unique IF NOT EXISTS
                FOR (g:Gene) REQUIRE g.symbol IS UNIQUE
                """
            },
            
            # HPO Term constraints
            {
                'name': 'hpo_id_unique',
                'query': """
                CREATE CONSTRAINT hpo_id_unique IF NOT EXISTS
                FOR (h:HPOTerm) REQUIRE h.id IS UNIQUE
                """
            }
        ]
        
        results = {}
        for constraint in constraints:
            try:
                result = self.service.execute_write_transcation(constraint['query'])
                results[constraint['name']] = {
                    'success': True,
                    'constraints_added': result.get('constraints_added', 0)
                }
                self.logger.info(f"Created constraint: {constraint['name']}")
            except Exception as e:
                results[constraint['name']] = { 
                    'success': False,
                    'error': str(e)
                }
                self.logger.warning(f"Constraint {constraint['name']} may already exist or failed: {e}")
        return results
    
    def create_all_indexes(self) -> Dict[str, Any]:
        indexes = [
            # Disease indexes
            {
                'name': 'disease_name_index',
                'query': """
                CREATE INDEX disease_name_index IF NOT EXISTS
                FOR (d:Disease) ON (d.name)
                """
            },
            {
                'name': 'disease_disorder_type_index',
                'query': """
                CREATE INDEX disease_disorder_type_index IF NOT EXISTS
                FOR (d:Disease) ON (d.disorder_type)
                """
            },
            {
                'name': 'disease_disorder_group_index',
                'query': """
                CREATE INDEX disease_disorder_group_index IF NOT EXISTS
                FOR (d:Disease) ON (d.disorder_group)
                """
            },
            
            # Gene indexes
            {
                'name': 'gene_symbol_index',
                'query': """
                CREATE INDEX gene_symbol_index IF NOT EXISTS
                FOR (g:Gene) ON (g.symbol)
                """
            },
            {
                'name': 'gene_name_index',
                'query': """
                CREATE INDEX gene_name_index IF NOT EXISTS
                FOR (g:Gene) ON (g.name)
                """
            },
            {
                'name': 'gene_hgnc_index',
                'query': """
                CREATE INDEX gene_hgnc_index IF NOT EXISTS
                FOR (g:Gene) ON (g.ref_hgnc)
                """
            },
            
            # HPO Term indexes
            {
                'name': 'hpo_name_index',
                'query': """
                CREATE INDEX hpo_name_index IF NOT EXISTS
                FOR (h:HPOTerm) ON (h.name)
                """
            },
            {
                'name': 'hpo_obsolete_index',
                'query': """
                CREATE INDEX hpo_obsolete_index IF NOT EXISTS
                FOR (h:HPOTerm) ON (h.obsolete)
                """
            }
        ]
        
        results = {}
        for index in indexes:
            try:
                result = self.service.execute_write_transcation(index['query'])
                results[index['name']] = {
                    'success': True,
                    'indexes_added': result.get('indexes_added', 0)
                }
                self.logger.info(f"Created index: {index['name']}")
                
            except Exception as e:
                results[index['name']] = {
                    'success': False,
                    'error': str(e)
                }
                self.logger.warning(f"⚠️ Index {index['name']} may already exist or failed: {e}")
        return results
    
    def setup_complete_schema(self) -> Dict[str, Any]:
        try:
            constraint_results = self.create_all_constraints()
            index_results = self.create_all_indexes()
            
            total_constraints = sum(1 for r in constraint_results.values() if r['success'])
            total_indexes = sum(1 for r in index_results.values() if r['success'])
            
            results = {
                'constraints': constraint_results,
                'indexes': index_results,
                'summary': {
                    'constraints_created': total_constraints,
                    'indexes_created': total_indexes,
                    'total_constraints_attempted': len(constraint_results),  
                    'total_indexes_attempted': len(index_results)  
                }
            }
            self.logger.info(f"Schema setup complete: {total_constraints} constraints, {total_indexes} indexes")
            return results
            
        except Exception as e:
            raise CustomException(f"Schema setup failed: {e}", e)
        
    def verify_schema(self) -> Dict[str, Any]:
        try:
            constraint_query = "SHOW CONSTRAINTS"
            constraints = self.service.execute_query(constraint_query)
            
            index_query = "SHOW INDEXES"
            indexes = self.service.execute_query(index_query)
            
            results = {
                'constraints': {
                    'count': len(constraints),
                    'details': constraints
                },
                'indexes': {
                    'count': len(indexes),
                    'details': indexes
                }
            }
            self.logger.info(f"Schema verification complete: {len(constraints)} constraints, {len(indexes)} indexes found")
            return results
            
        except Exception as e:
            raise CustomException(f"Schema verification failed: {e}", e)
        
    def drop_all_constraints(self, confirm: bool = False):
        if not confirm:
            raise CustomException("Constraint drop operation requires confirmation", None)
        self.logger.warning("Dropping all constraints")
        
        try:
            constraints = self.service.execute_query("SHOW CONSTRAINTS")
            
            for constraint in constraints:
                constraint_name = constraint.get('name')
                if constraint_name:
                    drop_query = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
                    self.service.execute_write_transcation(drop_query)
                    self.logger.info(f"Dropped constraints: {constraint_name}")
        except Exception as e:
            raise CustomException(f"Constraint drop failed: {e}", e)
        

if __name__ == "__main__": 
    try: 
        neo4j_config = Neo4jConfig()
        service = Neo4jService(neo4j_config)
        schema_manager = SchemaManager(service)
        
        results = schema_manager.setup_complete_schema()
        print(f"✅ Schema Setup Complete!")
        print(f"   Constraints: {results['summary']['constraints_created']}/{results['summary']['total_constraints_attempted']}")
        print(f"   Indexes: {results['summary']['indexes_created']}/{results['summary']['total_indexes_attempted']}")
        
        verification = schema_manager.verify_schema()
        print(f"Schema verification: {verification['constraints']['count']} constraints, {verification['indexes']['count']} indexes")
        
        service.close()
    except Exception as e:
        print(f"Schema setup failed: {e}")
