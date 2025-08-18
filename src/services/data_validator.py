import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from src.utils.logging import logger
from src.utils.exception import CustomException

@dataclass
class DatasetSchema:
    """Schema definition for each dataset"""
    name: str
    required_columns: List[str]
    optional_columns: List[str] | None = None
    key_column: Optional[str] = None
    
class DataValidator:
    """
    Validates and processes Orphanet CSV datasets
    Ensures data quality before Neo4j loading
    """
    def __init__(self):
        self.logger = logger
        self.schemas = self._define_schemas()
        self.validation_results = {}
    
    def _define_schemas(self) -> Dict[str, DatasetSchema]:
        """Define expected schemas for each dataset"""
        return {
            'disease_nomenclature': DatasetSchema(
                name='Disease Nomenclature (Product 1)',
                required_columns=['id', 'OrphaCode', 'Name'],
                optional_columns=['ExpertLink', 'Synonyms', 'DisorderType', 'DisorderGroup', 
                                'ExternalReferences', 'Definition'],
                key_column='OrphaCode'
            ),
            
            'disease_phenotype': DatasetSchema(
                name='Disease-Phenotype Associations (Product 6)',
                required_columns=['OrphaCode', 'HPOId'],
                optional_columns=['DisorderName', 'ExpertLink', 'DisorderType', 'DisorderGroup',
                                'HPOTerm', 'Frequency', 'DiagnosticCriteria'],
                key_column='OrphaCode'
            ),
            
            'disease_gene': DatasetSchema(
                name='Disease-Gene Associations (Product 4)',
                required_columns=['OrphaCode', 'GeneID', 'GeneSymbol'],
                optional_columns=['DisorderID', 'DisorderName', 'ExpertLink', 'DisorderType', 
                                'DisorderGroup', 'SourceOfValidation', 'AssociationType', 
                                'AssociationStatus', 'GeneName', 'GeneSynonyms', 'GeneType', 
                                'GeneLocus', 'LocusKey', 'Ref_HGNC', 'Ref_Ensembl', 'Ref_OMIM', 
                                'Ref_SwissProt', 'Ref_Genatlas', 'Ref_ClinVar', 'Ref_Reactome'],
                key_column='OrphaCode'
            ),
            
            'disease_relationships': DatasetSchema(
                name='Disease Relationships (Product 7)',
                required_columns=['OrphaCode', 'TargetOrphaCode'],
                optional_columns=['DisorderName', 'ExpertLink', 'TotalAssociations', 'DisorderId',
                                'RootId', 'IsCycle', 'TargetId', 'TargetName', 'AssociationType'],
                key_column='OrphaCode'
            ),
            
            'hpo_terms': DatasetSchema(
                name='HPO Terms',
                required_columns=['id', 'name'],
                optional_columns=['definition', 'comment', 'synonyms', 'synonym_types', 'xrefs',
                                'alt_ids', 'is_a', 'created_date', 'obsolete'],
                key_column='id'
            ),
            
            'hpo_annotations': DatasetSchema(
                name='HPO Annotations',
                required_columns=['database_id', 'hpo_id'],
                optional_columns=['disease_name', 'qualifier', 'reference', 'evidence', 'onset',
                                'frequency', 'sex', 'modifier', 'aspect', 'biocuration'],
                key_column='database_id'
            )
        }
        
    def validate_file_exists(self, file_path: str) -> bool:
        try:
            path = Path(file_path)
            if not path.exists():
                self.logger.error(f"File does not exist:{file_path}")
                return False
            if not path.is_file():
                self.logger.error(f"Path is not a file:{file_path}")
                return False
            
            pd.read_csv(file_path, nrows=1)
            return True
        except Exception as e:
            raise CustomException(f"File validation failed for {file_path}:{e}", e)
        
    def validate_dataset_schema(self, df:pd.DataFrame, schema: DatasetSchema) -> Dict[str, Any]:
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        missing_required = set(schema.required_columns) - set(df.columns)
        if missing_required:
            results['valid'] = False
            results['errors'].append(f"Missing required columns: {missing_required}")
            
        expected_columns = set(schema.required_columns + (schema.optional_columns or []))
        unexpected_columns = set(df.columns) - expected_columns
        if unexpected_columns:
            results['warnings'].append(f"Unexpected columns found: {unexpected_columns}")
            
        results['stats'] = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns_present': list(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() /1024 /1024
        }
        
        if schema.key_column and schema.key_column in df.columns:
            key_stats = self._validate_key_column(df, schema.key_column)
            results['stats']['key_column'] = key_stats
            
            if key_stats['duplicates'] > 0:
                results['warnings'].append(f"Found {key_stats['duplicates']} duplicate values in key column {schema.key_column}")
            
            if key_stats['null_count'] > 0:
                results['warnings'].append(f"Found {key_stats['null_count']} null values in key column {schema.key_column}") 
        return results
    
    def _validate_key_column(self, df:pd.DataFrame, key_column:str) -> Dict[str, Any]:
        series = df[key_column]
        return{
            'total_values': len(series),
            'unique_values': series.nunique(),
            'duplicates': len(series) - series.nunique(),
            'null_count': series.isnull().sum(),
            'completion_rate': (len(series) - series.isnull().sum()) / len(series) if len(series) > 0 else 0
        }
        
    def validate_csv_file(self, file_path: str, dataset_type: str) -> Dict[str, Any]:
        result = {
            'file_path': file_path,
            'dataset_type': dataset_type,
            'valid': False,
            'file_exists': False,
            'schema_validation': None,
            'data_quality': None
        }
        try:
            if not self.validate_file_exists(file_path):
                result['errors'] = ["File does not exists or not readable"]
                return result
            
            if dataset_type not in self.schemas:
                result['errors'] = [f"Unknown dataset type: {dataset_type}"]
                return result
            schema = self.schemas[dataset_type]
            
            try:
                df = pd.read_csv(file_path)
                self.logger.info(f"Loaded CSV: {len(df)} rows, {len(df.columns)} columns")
            except Exception as e:
                result['errors'] = [f"Failed to read CSV: {e}"]
                return result
            
            schema_validation = self.validate_dataset_schema(df, schema)
            result['data_quality'] = schema_validation
            
            data_quality = self._perform_data_quality_checks(df, schema)
            result['data_quality'] = data_quality
            
            result['valid'] = (
                schema_validation['valid'] and
                len(schema_validation['errors']) == 0 and
                data_quality['severe_issues'] == 0
            )
            
            if result['valid']:
                self.logger.info(f"Validation passed for {dataset_type}")
            else:
                self.logger.warning(f"Validation issues found for {dataset_type}")
        except Exception as e:
            raise CustomException(f"CSV Validation faileed: {e}", e)
        return result
    
    def _perform_data_quality_checks(self, df:pd.DataFrame, schema: DatasetSchema) -> Dict[str, Any]:
        quality_results ={
            'total_rows': len(df),
            'issues': [],
            'warnings': [],
            'severe_issues': 0,
            'column_analysis': {}
        }
        for column in df.columns:
            col_analysis = {
                'null_count': df[column].isnull().sum(),
                'null_percentage': (df[column].isnull().sum() / len(df)) * 100,
                'unique_values': df[column].nunique(),
                'data_type': str(df[column].dtype)
            }
            if column in schema.required_columns and col_analysis['null_percentage'] > 10:
                quality_results['issues'].append(f"High null rate in required column {column}: {col_analysis['null_percentage']:.1f}%")
                quality_results['severe_issues'] += 1
                
            if col_analysis['null_percentage'] == 100:
                quality_results['warnings'].append(f"Column {column} is completely empty")
            
            quality_results['column_analysis'][column] = col_analysis
            
            duplicate_rows = df.duplicated().sum()
            if duplicate_rows > 0:
                quality_results['warnings'].append(f"Found {duplicate_rows} completely duplicate rows")
        return quality_results
        
    def validate_all_datsets(self, dataset_paths: Dict[str, str]) -> Dict[str, Any]:
        self.logger.info(f"Starting validation of {len(dataset_paths)} datasets")
        results = {
            'overall_valid': True,
            'datasets': {},
            'summary': {
                'total_datasets': len(dataset_paths),
                'valid_datasets': 0,
                'invalid_datasets': 0,
                'total_rows': 0
            }
        }
        for dataset_type, file_path in dataset_paths.items():
            try:
                validation_result = self.validate_csv_file(file_path, dataset_type)
                results['datasets'][dataset_type] = validation_result
                
                if validation_result['valid']:
                    results['summary']['valid_datasets'] += 1
                else:
                    results['summary']['invalid_datasets'] += 1
                    results['overall_valid'] = False
                
                if validation_result.get('schema_validation'):
                    results['summary']['total_rows'] += validation_result['schema_validation']['stats']['total_rows']
            except Exception as e:
                self.logger.error(f"Failed to validate {dataset_type}:", {e})
                results['datasets'][dataset_type] = {
                    'valid': False,
                    'errors': [f'Validation process failed: {e}']
                }
                results['summary']['invalid_datasets'] += 1
                results['overall_valid'] = False
                
        summary = results['summary']
        self.logger.info(f"Validation complete: {summary['valid_datasets']}/{summary['total_datasets']} datasets valid, {summary['total_rows']} total rows")
        
        return results
    
    def generate_validation_report(self, validation_results: Dict[str, Any]) -> str:
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("ORPHANET DATA VALIDATION REPORT")
        report_lines.append("=" * 60)
        
        # Summary
        summary = validation_results['summary']
        report_lines.append(f"\nSUMMARY:")
        report_lines.append(f"  Total Datasets: {summary['total_datasets']}")
        report_lines.append(f"  Valid Datasets: {summary['valid_datasets']}")
        report_lines.append(f"  Invalid Datasets: {summary['invalid_datasets']}")
        report_lines.append(f"  Total Rows: {summary['total_rows']:,}")
        report_lines.append(f"  Overall Status: {'PASSED' if validation_results['overall_valid'] else 'FAILED'}")
        
        # Dataset details
        report_lines.append(f"\nDETAILED RESULTS:")
        
        for dataset_type, result in validation_results['datasets'].items():
            report_lines.append(f"\n📊 {dataset_type.upper().replace('_', ' ')}")
            report_lines.append(f"   Status: {'✅ Valid' if result['valid'] else '❌ Invalid'}")
            
            if result.get('schema_validation'):
                stats = result['schema_validation']['stats']
                report_lines.append(f"   Rows: {stats['total_rows']:,}")
                report_lines.append(f"   Columns: {stats['total_columns']}")
                
                if result['schema_validation']['errors']:
                    report_lines.append("   ❌ Schema Errors:")
                    for error in result['schema_validation']['errors']:
                        report_lines.append(f"      • {error}")
                
                if result['schema_validation']['warnings']:
                    report_lines.append("   ⚠️ Schema Warnings:")
                    for warning in result['schema_validation']['warnings']:
                        report_lines.append(f"      • {warning}")
            
            if result.get('data_quality'):
                quality = result['data_quality']
                if quality['issues']:
                    report_lines.append("   ❌ Data Quality Issues:")
                    for issue in quality['issues']:
                        report_lines.append(f"      • {issue}")
                
                if quality['warnings']:
                    report_lines.append("   ⚠️Data Quality Warnings:")
                    for warning in quality['warnings']:
                        report_lines.append(f"      • {warning}")
        
        report_lines.append("=" * 60)
        
        return "\n".join(report_lines)
    
if __name__ == "__main__":
    try:
        validator = DataValidator()
        
        current_dir = Path(__file__).parent  # src/services/
        project_root = current_dir.parent.parent  # project root
        data_dir = project_root / "data" / "processed"
        
        dataset_paths = {
            'disease_nomenclature': str(data_dir / 'en_product1.csv'),
            'disease_phenotype': str(data_dir / 'en_product4.csv'),
            'disease_gene': str(data_dir / 'en_product6.csv'), 
            'disease_relationships': str(data_dir / 'en_product7.csv'),
            'hpo_terms': str(data_dir / 'hp.csv'),
            'hpo_annotations': str(data_dir / 'phenotype.csv')
        }
        results = validator.validate_all_datsets(dataset_paths)
        
        report = validator.generate_validation_report(results)
        print(report)
        
    except CustomException as e:
        print(f"❌ Validation failed: {e}")