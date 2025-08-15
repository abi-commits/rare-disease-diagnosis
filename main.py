from src.components.data_ingestion import DataIngestion

ingestor = DataIngestion()
download_files = ingestor.download_all_orphadata()
print(f"Downloaded_files;{download_files}")