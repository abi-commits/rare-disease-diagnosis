import csv, sys
from pathlib import Path
import xml.etree.ElementTree as ET
from src.utils.logging import logger
from src.utils.exception import CustomException
from src.utils.xml_utlis import local, find_first, find_text, find_all

def normalize_hp(hp: str) -> str:
    if not hp:
        return hp
    return hp.replace("_", ":").upper()  # HP_0001250 -> HP:0001250

def convert(xml_path: Path, csv_path: Path):
    try:
        xml_path = Path(xml_path)
        csv_path = Path(csv_path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = ["start_orpha_id", "end_hp_id", "frequency"]
        out = csv.DictWriter(csv_path.open("w", newline="", encoding="utf-8"), fieldnames=fieldnames)
        out.writeheader()

        # streaming parse
        for event, elem in ET.iterparse(xml_path, events=("end",)):
            if local(elem.tag) == "DisorderHPOTermAssociation":
                # structure varies slightly between dumps; be defensive
                dis = find_first(elem, "Disorder")
                orpha = None
                if dis is not None:
                    orpha = find_text(dis, "OrphaNumber") or find_text(dis, "OrphaCode")
                hpo_block = find_first(elem, "HPO")
                hp_id = None
                if hpo_block is not None:
                    hp_id = (find_text(hpo_block, "HPOId")
                             or find_text(hpo_block, "HPO_ID")
                             or find_text(hpo_block, "Id")
                             or find_text(hpo_block, "ID"))
                freq_block = find_first(elem, "Frequency") or find_first(elem, "HPOFrequency")
                freq = None
                if freq_block is not None:
                    freq = find_text(freq_block, "Name") or (freq_block.text or "").strip()

                if orpha and hp_id:
                    out.writerow({
                        "start_orpha_id": f"ORPHA:{orpha}",
                        "end_hp_id": normalize_hp(hp_id),
                        "frequency": (freq or "")
                    })
                elem.clear()  # free memory
      
        logger.info("Wrote disease-HPO edges -> %s", csv_path)
        print(f"✔ disease–HPO edges -> {csv_path}")
        return 0
    except Exception as e:
        logger.error("Failed to parse en_product4.xml: %s", e)
        raise CustomException(str(e), sys)

def main(argv=None):
    argv = argv or sys.argv[1:]
    xml_in = Path(argv[0]) if len(argv) >= 1 else Path("data/raw/orphadata/en_product4.xml")
    csv_out = Path(argv[1]) if len(argv) >= 2 else Path("data/processed/edges_disease_hpo.csv")
    return convert(xml_in, csv_out)

if __name__ == "__main__":
    raise SystemExit(main())
