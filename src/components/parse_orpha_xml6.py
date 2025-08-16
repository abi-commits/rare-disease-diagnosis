import csv
import sys
from pathlib import Path
import xml.etree.ElementTree as ET
from ..utils.exception import CustomException
from ..utils.logging import logger
from ..utils.xml_utlis import local, find_first, find_all, find_text, find_text_lang


def parse_external_references(gene_elem):
    """Parse external references for a gene into a dictionary"""
    refs = {}
    for ref in gene_elem.findall('ExternalReferenceList/ExternalReference'):
        source = find_text(ref, 'Source')
        reference = find_text(ref, 'Reference')
        if source and reference:
            refs[source] = reference
    return refs


def parse_gene(gene_elem):
    """Parse a Gene element into a dictionary with all relevant information"""
    if gene_elem is None:
        return None
    
    # Basic gene info
    gene = {
        'GeneID': gene_elem.get('id', ''),
        'GeneName': find_text_lang(gene_elem, 'Name', lang='en'),
        'GeneSymbol': find_text(gene_elem, 'Symbol'),
    }
    
    # Synonyms
    synonyms = [syn.text.strip() for syn in gene_elem.findall('SynonymList/Synonym') if syn.text]
    gene['GeneSynonyms'] = '; '.join(synonyms)
    
    # Gene type
    gene_type = gene_elem.find('GeneType/Name')
    gene['GeneType'] = gene_type.text.strip() if gene_type is not None and gene_type.text else ''
    
    # External references
    refs = parse_external_references(gene_elem)
    for source, ref in refs.items():
        gene[f'Ref_{source}'] = ref
    
    # Locus information
    locus = gene_elem.find('LocusList/Locus')
    if locus is not None:
        gene['GeneLocus'] = find_text(locus, 'GeneLocus')
        gene['LocusKey'] = find_text(locus, 'LocusKey')
    
    return gene


def parse_disorder(disorder_elem):
    """Parse a Disorder element with its gene associations into a list of rows"""
    # Basic disorder info
    disorder_info = {
        'DisorderID': disorder_elem.get('id', ''),
        'OrphaCode': find_text(disorder_elem, 'OrphaCode'),
        'DisorderName': find_text_lang(disorder_elem, 'Name', lang='en'),
        'ExpertLink': find_text_lang(disorder_elem, 'ExpertLink', lang='en'),
    }
    
    # Disorder type and group
    dtype = disorder_elem.find('DisorderType/Name')
    dgroup = disorder_elem.find('DisorderGroup/Name')
    disorder_info['DisorderType'] = dtype.text.strip() if dtype is not None and dtype.text else ''
    disorder_info['DisorderGroup'] = dgroup.text.strip() if dgroup is not None and dgroup.text else ''
    
    # Process gene associations
    rows = []
    for assoc in disorder_elem.findall('DisorderGeneAssociationList/DisorderGeneAssociation'):
        row = disorder_info.copy()  # Start with disorder info
        
        # Add association details
        row['SourceOfValidation'] = find_text(assoc, 'SourceOfValidation')
        
        # Association type and status
        assoc_type = assoc.find('DisorderGeneAssociationType/Name')
        assoc_status = assoc.find('DisorderGeneAssociationStatus/Name')
        row['AssociationType'] = assoc_type.text.strip() if assoc_type is not None and assoc_type.text else ''
        row['AssociationStatus'] = assoc_status.text.strip() if assoc_status is not None and assoc_status.text else ''
        
        # Add gene information
        gene = parse_gene(assoc.find('Gene'))
        if gene:
            row.update(gene)  # Add all gene fields to the row
            rows.append(row)
    
    return rows


def convert(xml_path: Path, csv_path: Path):
    """Convert Orphanet XML with gene associations to CSV format"""
    xml_path = Path(xml_path)
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Parse XML
        logger.info(f'Reading {xml_path}')
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Get all disorders
        disorders = root.findall('.//Disorder')
        if not disorders:
            msg = f'No <Disorder> elements found in {xml_path}'
            logger.error(msg)
            raise CustomException(msg, sys)
        
        # Define CSV fields to include all possible columns
        fieldnames = [
            # Disorder fields
            'DisorderID', 'OrphaCode', 'DisorderName', 'ExpertLink',
            'DisorderType', 'DisorderGroup',
            # Association fields
            'SourceOfValidation', 'AssociationType', 'AssociationStatus',
            # Gene fields
            'GeneID', 'GeneName', 'GeneSymbol', 'GeneSynonyms',
            'GeneType', 'GeneLocus', 'LocusKey',
            # External reference fields
            'Ref_HGNC', 'Ref_Ensembl', 'Ref_OMIM', 'Ref_SwissProt',
            'Ref_Genatlas', 'Ref_ClinVar', 'Ref_Reactome'
        ]
        
        # Write to CSV, flattening disorder-gene associations
        total_rows = 0
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            
            for disorder in disorders:
                rows = parse_disorder(disorder)
                for row in rows:
                    writer.writerow(row)
                    total_rows += 1
        
        logger.info(f'Wrote {total_rows} disorder-gene associations to {csv_path}')
        return 0
        
    except Exception as e:
        logger.error(f'Failed to convert XML to CSV: {e}')
        raise CustomException(str(e), sys)


def main(argv=None):
    """Parse command line args and run conversion"""
    argv = argv or sys.argv[1:]
    xml_in = Path(argv[0]) if len(argv) >= 1 else Path('data/raw/en_product6.xml')
    csv_out = Path(argv[1]) if len(argv) >= 2 else Path('data/processed/en_product6.csv')
    try:
        return convert(xml_in, csv_out)
    except Exception as e:
        if isinstance(e, CustomException):
            raise
        logger.error(f'Unexpected error in main: {e}')
        raise CustomException(str(e), sys)


if __name__ == '__main__':
    raise SystemExit(main())
