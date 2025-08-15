import sys
from pathlib import Path
from ..utils.exception import CustomException
from ..utils.logging import logger

path = Path("data/raw")

required_files = [
    path/"en_product1.xml",
    path/"en_product4.xml",
    path/"en_product6.xml",
    path/"en_product7.xml",
    path/"hp.obo",
    path/"phenotype.hpoa"
]

def run():
    try:
        missing = [str(p) for p in required_files if not p.exists()]
        if missing:
            raise CustomException(f"Missing required raw files:\n" + "\n".join(missing), sys)
        sizes = {str(p): p.stat().st_size/(1024 * 1024) for p in required_files}
        logger.info("Raw input validation OK. File sizes (bytes): %s", sizes)
        
    except Exception as e:
        raise CustomException(str(e), sys)

if __name__ == "__main__":
    run()