import csv
import sys
from pathlib import Path
import xml.etree.ElementTree as ET
from ..utils.exception import CustomException
from ..utils.logging import logger


def text_of(elem):
    if elem is None:
        return ""
    # return concatenated text including tail for simple HTML inside
    return ''.join(ET.tostring(e, encoding='unicode', method='html') if list(e) else (e.text or '') for e in [elem]).strip()


def get_child_text(parent, tag, lang=None):
    if parent is None:
        return ""
    for child in parent.findall(tag):
        if lang is None:
            return (child.text or '').strip()
        if child.get('lang') == lang:
            return (child.text or '').strip()
    return ""


def parse_disorder(elem):
    # elem is <Disorder>
    row = {}
    row['id'] = elem.get('id', '')
    row['OrphaCode'] = get_child_text(elem, 'OrphaCode')
    row['ExpertLink'] = get_child_text(elem, 'ExpertLink', lang='en')
    row['Name'] = get_child_text(elem, 'Name', lang='en')

    # synonyms
    syns = [s.text.strip() for s in elem.findall('SynonymList/Synonym') if s.text]
    row['Synonyms'] = '; '.join(syns)

    # DisorderType/DisorderGroup
    dtype = elem.find('DisorderType/Name')
    dgroup = elem.find('DisorderGroup/Name')
    row['DisorderType'] = dtype.text.strip() if dtype is not None and dtype.text else ''
    row['DisorderGroup'] = dgroup.text.strip() if dgroup is not None and dgroup.text else ''

    # ExternalReferences: Source:Reference pairs
    ext_pairs = []
    for ext in elem.findall('ExternalReferenceList/ExternalReference'):
        source = get_child_text(ext, 'Source')
        ref = get_child_text(ext, 'Reference')
        if source or ref:
            ext_pairs.append(f"{source}:{ref}")
    row['ExternalReferences'] = '; '.join(ext_pairs)

    # Definition / SummaryInformation -> TextSection -> Contents
    contents = ''
    # try SummaryInformation/TextSectionList/TextSection/Contents
    si = elem.find('SummaryInformationList/SummaryInformation')
    if si is not None:
        ts = si.find('TextSectionList/TextSection/Contents')
        if ts is not None and ts.text:
            contents = ts.text.strip()
        else:
            # some entries use TextAuto/Info
            ta = si.find('TextAuto/Info')
            if ta is not None and ta.text:
                contents = ta.text.strip()
    row['Definition'] = contents

    return row


def convert(xml_path: Path, csv_path: Path):
    xml_path = Path(xml_path)
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # parse incrementally to keep memory use modest
        tree = ET.parse(xml_path)
        root = tree.getroot()

        disorders = root.findall('.//Disorder')
        if not disorders:
            msg = f'No <Disorder> elements found in {xml_path}'
            logger.error(msg)
            raise CustomException(msg, sys)

        fieldnames = ['id', 'OrphaCode', 'Name', 'ExpertLink', 'Synonyms', 'DisorderType', 'DisorderGroup', 'ExternalReferences', 'Definition']

        with csv_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for d in disorders:
                writer.writerow(parse_disorder(d))

        logger.info('Wrote %s (%d rows)', str(csv_path), len(disorders))
        return 0

    except Exception as e:
        logger.error('Failed to convert XML to CSV: %s', e)
        raise CustomException(str(e), sys)


def main(argv=None):
    argv = argv or sys.argv[1:]
    xml_in = Path(argv[0]) if len(argv) >= 1 else Path('data/raw/en_product1.xml')
    csv_out = Path(argv[1]) if len(argv) >= 2 else Path('data/processed/en_product1.csv')
    try:
        return convert(xml_in, csv_out)
    except Exception as e:
        # convert already logs; re-raise as CustomException if not already
        if isinstance(e, CustomException):
            raise
        logger.error('Unexpected error in main: %s', e)
        raise CustomException(str(e), sys)


if __name__ == '__main__':
    raise SystemExit(main())
