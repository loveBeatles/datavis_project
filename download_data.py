import eurostat
import pandas as pd
import requests
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.abspath("./data")
os.makedirs(DATA_DIR, exist_ok=True)
EUROSTAT_DATA = {
    "migr_imm1ctz": "immigration", # Emigration by citizenship, age and sex
    "migr_emi1ctz": "emigration", # Emigration by citizenship, age and sex
    "migr_asydcfstq": "asylum", # Asylum and first time asylum applicants
    "tps00176": "immigration_test",
    "tps00177": "emigration_test" 
}

OTHER_DATA = {
    # UNHCR API endpoint for Ukrainian sylum seekers
    (
    "https://api.unhcr.org/population/v1/asylum-applications/?download=true&"
    "year_from=2020&"
    "coo=UKR&"  # ISO3 code for Ukraine
    "coa_region=5&"  # Europe region code
    "format=csv"
    ): "ua_asylum"
}

def download_unhcr_csv(dataset_url: str, filename: str) -> bool:
    """Download zip from URL"""
    try:
        response = requests.get(dataset_url, timeout=30)
        response.raise_for_status()
        
        # Save zip
        output_path = os.path.join(DATA_DIR, f"{filename}.zip")
        with open(output_path, "wb") as f:
            f.write(response.content)
        
        logger.info(f"Downloaded zip: {filename}")
        return True
    except Exception as e:
        logger.error(f"Failed to download {filename}: {str(e)}")
        return False

def download_eurostat_data(dataset_id: str, filename: str) -> bool:
    """Download Eurostat data."""
    try:
        df = eurostat.get_data_df(dataset_id, verbose=False)
        output_path = os.path.join(DATA_DIR, f"{filename}.csv")
        df.to_csv(output_path, index=False)
        logger.info(f"Downloaded Eurostat: {filename}")
        return True
    except Exception as e:
        logger.error(f"Failed to download {filename}: {str(e)}")
        return False

def main() -> None:
    """Main download workflow."""
    logger.info("Starting downloads")
    
    # Download Eurostat data
    for dataset_id, filename in EUROSTAT_DATA.items():
        output_path = os.path.join(DATA_DIR, f"{filename}.csv")
        if not os.path.exists(output_path):
            success = download_eurostat_data(dataset_id, filename)
            if not success:
                logger.warning(f"Skipping failed dataset: {filename}")
        else:
            logger.info(f"Skipping existing: {filename}")
    
    # Download UNHCR data
    for dataset_url, filename in OTHER_DATA.items():
        output_path = os.path.join(DATA_DIR, f"{filename}.zip")
        if not os.path.exists(output_path):
            success = download_unhcr_csv(dataset_url, filename)
            if not success:
                logger.warning(f"Skipping failed dataset: {filename}")
        else:
            logger.info(f"Skipping existing: {filename}")
    
    logger.info("Download process completed")

if __name__ == "__main__":
    main()