import csv
import sys
from pathlib import Path
from datetime import datetime
from ..utils.exception import CustomException
from ..utils.logging import logger


def parse_metadata(lines):
    """Parse metadata lines starting with # into a dictionary"""
    metadata = {}
    for line in lines:
        if not line.startswith('#'):
            break
        if ':' in line[1:]:  # Skip the # character
            key, value = line[1:].split(':', 1)
            metadata[key.strip()] = value.strip().strip('"')
    return metadata


def parse_hpoa_line(line):
    """Parse a single HPOA annotation line into a dictionary"""
    try:
        (database_id, disease_name, qualifier, hpo_id, reference,
         evidence, onset, frequency, sex, modifier, aspect,
         biocuration) = line.strip().split('\t')

        return {
            'database_id': database_id,
            'disease_name': disease_name,
            'qualifier': qualifier,
            'hpo_id': hpo_id,
            'reference': reference,
            'evidence': evidence,
            'onset': onset,
            'frequency': frequency,
            'sex': sex,
            'modifier': modifier,
            'aspect': aspect,
            'biocuration': biocuration
        }
    except ValueError as e:
        logger.warning(f'Failed to parse line: {line.strip()}, Error: {e}')
        return None


def convert(hpoa_path: Path, csv_path: Path):
    """Convert phenotype.hpoa to CSV format, preserving metadata"""
    hpoa_path = Path(hpoa_path)
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(f'Reading {hpoa_path}')
        
        # Read file and separate metadata from annotations
        with hpoa_path.open('r', encoding='utf-8') as f:
            lines = f.readlines()

        # Parse metadata and find where annotations begin
        metadata = parse_metadata(lines)
        data_start = 0
        for i, line in enumerate(lines):
            if not line.startswith('#'):
                data_start = i
                break

        # Get header line (column names)
        header = lines[data_start].strip().split('\t')
        data_start += 1  # Skip header line

        # Process annotations
        total_rows = 0
        valid_rows = 0
        
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'database_id', 'disease_name', 'qualifier', 'hpo_id',
                'reference', 'evidence', 'onset', 'frequency', 'sex',
                'modifier', 'aspect', 'biocuration'
            ])
            
            # Write metadata as comments in CSV
            for key, value in metadata.items():
                f.write(f'# {key}: {value}\n')
            
            writer.writeheader()
            
            for line in lines[data_start:]:
                total_rows += 1
                row = parse_hpoa_line(line)
                if row:
                    writer.writerow(row)
                    valid_rows += 1

        logger.info(f'Processed {total_rows} rows, wrote {valid_rows} valid entries to {csv_path}')
        logger.info(f'Metadata: version={metadata.get("version")}, description={metadata.get("description")}')
        return 0

    except Exception as e:
        logger.error(f'Failed to convert HPOA to CSV: {e}')
        raise CustomException(str(e), sys)


def main(argv=None):
    """Parse command line args and run conversion"""
    argv = argv or sys.argv[1:]
    hpoa_in = Path(argv[0]) if len(argv) >= 1 else Path('data/raw/phenotype.hpoa')
    csv_out = Path(argv[1]) if len(argv) >= 2 else Path('data/processed/phenotype.csv')
    try:
        return convert(hpoa_in, csv_out)
    except Exception as e:
        if isinstance(e, CustomException):
            raise
        logger.error(f'Unexpected error in main: {e}')
        raise CustomException(str(e), sys)


if __name__ == '__main__':
    raise SystemExit(main())
