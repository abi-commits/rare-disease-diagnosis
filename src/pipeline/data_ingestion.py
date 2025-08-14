import os
import requests
from src.logging.logger import logger
from src.exception.exception import CustomException
import sys

class DataIngestion:
    def __init__(self, data_folder="data"):
        # Root-level data folder
        self.data_folder = os.path.join(os.getcwd(), data_folder)
        os.makedirs(self.data_folder, exist_ok=True)

    def download_file(self, url):
        try:
            file_name = url.split("/")[-1]
            file_path = os.path.join(self.data_folder, file_name)

            if os.path.exists(file_path):
                logger.info(f"File already exists, skipping: {file_name}")
                return file_path

            logger.info(f"Downloading: {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Successfully downloaded: {file_name}")
            return file_path

        except Exception as e:
            logger.error(f"Failed to download {url}: {str(e)}")
            raise CustomException(str(e), sys)

    def download_all_orphadata(self):
        """Downloads all Orphadata + HPO free access datasets"""
        urls = [
            # Orphadata XML products
            "https://www.orphadata.com/data/xml/en_product1.xml",
            "https://www.orphadata.com/data/xml/en_product4.xml",
            "https://www.orphadata.com/data/xml/en_product6.xml",
            "https://www.orphadata.com/data/xml/en_product7.xml",
            "https://www.orphadata.com/data/xml/en_product9.xml",

            # HPO files
            "http://purl.obolibrary.org/obo/hp.obo",
            "https://github.com/obophenotype/human-phenotype-ontology/releases/latest/download/phenotype.hpoa"
        ]

        downloaded_files = []
        for url in urls:
            try:
                file_path = self.download_file(url)
                downloaded_files.append(file_path)
            except CustomException as ce:
                logger.error(f"Skipping file due to error: {ce}")

        return downloaded_files