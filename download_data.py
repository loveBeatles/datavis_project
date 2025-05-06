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
    "migr_imm1ctz": "immigration_whole", # Immigration by citizenship, age and sex
    "migr_emi1ctz": "emigration_whole", # Emigration by citizenship, age and sex
    "migr_asydcfstq": "asylum_eurostat", # Asylum and first time asylum applicants
    "tps00176": "immigration_filtered",
    "tps00177": "emigration_filtered" 
}

OTHER_DATA = {
    # Combined dataset with both asylum seekers and refugees
    (
        "https://api.unhcr.org/population/v1/population/?download=true&"
        "year_from=2013&"
        "coo=UKR&"                  # Origin country: Ukraine
        "coa_all=true&"              # Get all countries of asylum
        "format=csv&"
        "disaggregations=coa&"       # Force country-level disaggregation
        "population_type=ASY,REF&"   # Both asylum seekers and refugees
        "columns=year,coa_name,coa,population_type,population"
    ): "ua_displacement"
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