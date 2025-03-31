import eurostat
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Dictionary of required datasets with descriptions
DATASETS = {
    # Migration flows (immigration/emigration)
    "migr_imm1ctz": "Immigration by citizenship, age and sex",  # Annual data on immigrants
    "migr_emi1ctz": "Emigration by citizenship, age and sex",  # Annual data on emigrants
    
    # For Ukraine war analysis
    "migr_imm1ctz": "Same as above",  # Reused with filters
    "migr_asydcfstq": "Asylum and first time asylum applicants",  # Alternative for recent data
}

COUNTRIES = ["SK", "AT", "HU", "CZ"]  # Slovakia, Austria, Hungary, Czech Republic

def download_dataset(dataset_id):
    try:
        df = eurostat.get_data_df(dataset_id, verbose=False)
        df.to_csv(os.path.join(DATA_DIR, f"{dataset_id}.csv"), index=False)
        logging.info(f"Downloaded {dataset_id}")
        return True
    except Exception as e:
        logging.error(f"Failed to download {dataset_id}: {str(e)}")
        return False

if __name__ == "__main__":
    for dataset_id in DATASETS:
        output_path = os.path.join(DATA_DIR, f"{dataset_id}.csv")
        if not os.path.exists(output_path):
            download_dataset(dataset_id)
        else:
            logging.info(f"{dataset_id} already exists, skipping download")