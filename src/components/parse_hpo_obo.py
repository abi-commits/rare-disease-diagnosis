import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Iterator
from ..utils.exception import CustomException
from ..utils.logging import logger


def parse_tag_value(line: str) -> tuple[str, str]:
    """Parse a line into tag and value, handling potential colons in the value."""
    if ': ' not in line:
        return line, ''
    tag, value = line.split(': ', 1)
    return tag.strip(), value.strip()


def parse_term_block(lines: List[str]) -> Optional[Dict[str, List[str]]]:
    """Parse a Term block into a dictionary of tag-value pairs.
    Some tags (like synonym, xref) can appear multiple times, so values are lists."""
    if not lines or not lines[0].startswith('[Term]'):
        return None
    
    term = {}
    current_tag = None
    
    for line in lines[1:]:  # Skip [Term] line
        if not line.strip():
            continue
            
        # Handle line continuations (indented lines)
        if line.startswith(' '):
            if current_tag and current_tag in term:
                term[current_tag][-1] += ' ' + line.strip()
            continue
            
        tag, value = parse_tag_value(line)
        current_tag = tag
        
        if not value:
            continue
            
        # Some fields can appear multiple times, store as lists
        if tag in ['synonym', 'xref', 'alt_id', 'is_a', 'intersection_of', 'disjoint_from']:
            if tag not in term:
                term[tag] = []
            term[tag].append(value)
        else:
            # Single-value fields
            term[tag] = [value]
    
    return term


def parse_obo_file(file_path: Path) -> Iterator[Dict[str, List[str]]]:
    """Parse an OBO file, yielding each term block as a dictionary."""
    current_block = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip()
            
            # Start of a new term block
            if line == '[Term]':
                if current_block:  # Process previous block if it exists
                    term_dict = parse_term_block(current_block)
                    if term_dict:
                        yield term_dict
                current_block = [line]
            
            # Empty line marks end of block
            elif not line and current_block:
                term_dict = parse_term_block(current_block)
                if term_dict:
                    yield term_dict
                current_block = []
            
            # Add line to current block
            elif current_block or line.strip():
                current_block.append(line)
    
    # Process last block
    if current_block:
        term_dict = parse_term_block(current_block)
        if term_dict:
            yield term_dict


def extract_synonym_details(synonym: str) -> tuple[str, str, str]:
    """Extract the synonym text, type, and source from a synonym string.
    Example: "Abnormality of body height" EXACT layperson []
    Returns: (synonym_text, synonym_type, source)"""
    # Match the quoted text, type, and optional source
    match = re.match(r'"([^"]+)"\s+(\w+)(?:\s+(\w+))?\s*\[(.*?)\]', synonym)
    if not match:
        return synonym, '', ''  # Return raw text if pattern doesn't match
        
    text, syn_type, syn_subtype, source = match.groups()
    # Combine type and subtype if both exist
    full_type = f"{syn_type} {syn_subtype}" if syn_subtype else syn_type
    return text.strip(), full_type.strip(), source.strip()


def convert(obo_path: Path, csv_path: Path):
    """Convert HPO OBO file to CSV format."""
    import sys
    obo_path = Path(obo_path)
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Define CSV fields
        fieldnames = [
            'id', 'name', 'definition', 'comment',
            'synonyms', 'synonym_types',  # Semicolon-separated lists
            'xrefs',  # Semicolon-separated external references
            'alt_ids',  # Semicolon-separated alternative IDs
            'is_a',  # Semicolon-separated parent terms
            'created_date',
            'obsolete'  # Boolean flag for obsolete terms
        ]
        
        term_count = 0
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for term in parse_obo_file(obo_path):
                # Skip non-terms or metadata blocks
                if 'id' not in term:
                    continue
                
                # Process synonyms
                synonyms = []
                synonym_types = []
                if 'synonym' in term:
                    for syn in term['synonym']:
                        text, syn_type, _ = extract_synonym_details(syn)
                        synonyms.append(text)
                        if syn_type:
                            synonym_types.append(syn_type)
                
                # Build row data
                row = {
                    'id': term['id'][0] if 'id' in term else '',
                    'name': term['name'][0] if 'name' in term else '',
                    'definition': term['def'][0] if 'def' in term else '',
                    'comment': term['comment'][0] if 'comment' in term else '',
                    'synonyms': '; '.join(synonyms),
                    'synonym_types': '; '.join(synonym_types),
                    'xrefs': '; '.join(term.get('xref', [])),
                    'alt_ids': '; '.join(term.get('alt_id', [])),
                    'is_a': '; '.join(term.get('is_a', [])),
                    'created_date': term['creation_date'][0] if 'creation_date' in term else '',
                    'obsolete': 'true' if 'is_obsolete' in term else 'false'
                }
                
                writer.writerow(row)
                term_count += 1
        
        logger.info(f'Processed {term_count} HPO terms to {csv_path}')
        return 0
        
    except Exception as e:
        logger.error(f'Failed to convert OBO to CSV: {e}')
        raise CustomException(str(e), sys)


def main(argv=None):
    """Parse command line args and run conversion."""
    import sys
    argv = argv or sys.argv[1:]
    obo_in = Path(argv[0]) if len(argv) >= 1 else Path('data/raw/hp.obo')
    csv_out = Path(argv[1]) if len(argv) >= 2 else Path('data/processed/hp.csv')
    try:
        return convert(obo_in, csv_out)
    except Exception as e:
        if isinstance(e, CustomException):
            raise
        logger.error(f'Unexpected error in main: {e}')
        raise CustomException(str(e), sys)


if __name__ == '__main__':
    raise SystemExit(main())
