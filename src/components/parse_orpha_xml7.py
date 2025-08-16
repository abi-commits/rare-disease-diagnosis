import csv
import sys
from pathlib import Path
import xml.etree.ElementTree as ET
from ..utils.exception import CustomException
from ..utils.logging import logger
from ..utils.xml_utlis import local, find_first, find_all, find_text, find_text_lang


def parse_target_disorder(target):
    """Parse a TargetDisorder element to extract its details"""
    if target is None:
        return {}
    
    return {
        'TargetId': target.get('id', ''),
        'TargetOrphaCode': find_text(target, 'OrphaCode'),
        'TargetName': find_text_lang(target, 'Name', lang='en')
    }


def parse_disorder_association(association, disorder_id):
    """Parse a single DisorderDisorderAssociation element"""
    if association is None:
        return None
    
    # Get target disorder info
    target = parse_target_disorder(association.find('TargetDisorder'))
    
    # Get association type
    assoc_type = association.find('DisorderDisorderAssociationType/Name')
    association_type = assoc_type.text.strip() if assoc_type is not None and assoc_type.text else ''
    
    # Get root disorder (usually references back to parent)
    root = association.find('RootDisorder')
    root_id = root.get('id', '') if root is not None else ''
    is_cycle = root.get('cycle', 'false') if root is not None else 'false'
    
    return {
        'DisorderId': disorder_id,
        'RootId': root_id,
        'IsCycle': is_cycle,
        'AssociationType': association_type,
        **target  # Add target fields (TargetId, TargetOrphaCode, TargetName)
    }


def parse_disorder(disorder):
    """Parse a Disorder element and its associations"""
    # Get basic disorder info
    disorder_id = disorder.get('id', '')
    orpha_code = find_text(disorder, 'OrphaCode')
    name = find_text_lang(disorder, 'Name', lang='en')
    expert_link = find_text_lang(disorder, 'ExpertLink', lang='en')
    
    # Get all disorder associations
    associations = []
    assoc_list = disorder.find('DisorderDisorderAssociationList')
    if assoc_list is not None:
        total_associations = int(assoc_list.get('count', '0'))
        for assoc in assoc_list.findall('DisorderDisorderAssociation'):
            assoc_data = parse_disorder_association(assoc, disorder_id)
            if assoc_data:
                row = {
                    'OrphaCode': orpha_code,
                    'DisorderName': name,
                    'ExpertLink': expert_link,
                    'TotalAssociations': total_associations,
                    **assoc_data
                }
                associations.append(row)
    
    return associations


def convert(xml_path: Path, csv_path: Path):
    """Convert Orphanet XML with disorder associations to CSV format"""
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
        
        # Define CSV fields
        fieldnames = [
            'OrphaCode', 'DisorderName', 'ExpertLink', 'TotalAssociations',
            'DisorderId', 'RootId', 'IsCycle',
            'TargetId', 'TargetOrphaCode', 'TargetName',
            'AssociationType'
        ]
        
        # Write to CSV, flattening disorder associations
        total_rows = 0
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for disorder in disorders:
                rows = parse_disorder(disorder)
                for row in rows:
                    writer.writerow(row)
                    total_rows += 1
        
        logger.info(f'Wrote {total_rows} disorder associations to {csv_path}')
        return 0
        
    except Exception as e:
        logger.error(f'Failed to convert XML to CSV: {e}')
        raise CustomException(str(e), sys)


def main(argv=None):
    """Parse command line args and run conversion"""
    argv = argv or sys.argv[1:]
    xml_in = Path(argv[0]) if len(argv) >= 1 else Path('data/raw/en_product7.xml')
    csv_out = Path(argv[1]) if len(argv) >= 2 else Path('data/processed/en_product7.csv')
    try:
        return convert(xml_in, csv_out)
    except Exception as e:
        if isinstance(e, CustomException):
            raise
        logger.error(f'Unexpected error in main: {e}')
        raise CustomException(str(e), sys)


if __name__ == '__main__':
    raise SystemExit(main())
