"""
Data download script for COVID-19 Data Engineering Project.
Downloads the latest COVID-19 dataset from Our World in Data.
"""

import sys
import os
import logging
import requests
from datetime import datetime
import time

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_covid_data(url: str, output_path: str) -> bool:
    """
    Download COVID-19 data from the specified URL.
    
    Args:
        url: URL to download the data from
        output_path: Local path to save the downloaded file
        
    Returns:
        bool: True if download successful, False otherwise
    """
    try:
        logger.info(f"Starting download from: {url}")
        logger.info(f"Output path: {output_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Download with progress tracking
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Get file size for progress tracking
        total_size = int(response.headers.get('content-length', 0))
        
        logger.info(f"File size: {total_size / (1024*1024):.2f} MB")
        
        # Download file with progress
        downloaded_size = 0
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    downloaded_size += len(chunk)
                    
                    # Log progress every 10MB
                    if downloaded_size % (10 * 1024 * 1024) < 8192:
                        progress = (downloaded_size / total_size) * 100 if total_size > 0 else 0
                        logger.info(f"Download progress: {progress:.1f}% ({downloaded_size / (1024*1024):.1f} MB)")
        
        logger.info("Download completed successfully!")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during download: {str(e)}")
        return False

def verify_downloaded_file(file_path: str) -> bool:
    """
    Verify that the downloaded file is valid.
    
    Args:
        file_path: Path to the downloaded file
        
    Returns:
        bool: True if file is valid, False otherwise
    """
    try:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False
        
        # Check file size
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            logger.error("Downloaded file is empty")
            return False
        
        logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
        
        # Check if it's a valid CSV file
        import pandas as pd
        try:
            # Try to read first few rows
            df_sample = pd.read_csv(file_path, nrows=5)
            logger.info(f"CSV validation successful. Columns: {list(df_sample.columns)}")
            logger.info(f"Sample data shape: {df_sample.shape}")
            
            # Check for required columns
            required_columns = ['date', 'location', 'new_cases', 'total_cases']
            missing_columns = [col for col in required_columns if col not in df_sample.columns]
            
            if missing_columns:
                logger.warning(f"Missing columns: {missing_columns}")
                logger.info("Available columns: " + ", ".join(df_sample.columns))
            
            return True
            
        except Exception as e:
            logger.error(f"CSV validation failed: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"File verification failed: {str(e)}")
        return False

def get_file_info(file_path: str) -> dict:
    """
    Get information about the downloaded file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        dict: File information
    """
    try:
        import pandas as pd
        
        # Get basic file info
        stat = os.stat(file_path)
        file_info = {
            'file_path': file_path,
            'file_size_mb': stat.st_size / (1024 * 1024),
            'last_modified': datetime.fromtimestamp(stat.st_mtime),
            'created': datetime.fromtimestamp(stat.st_ctime)
        }
        
        # Get data info
        df = pd.read_csv(file_path)
        file_info.update({
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'date_range': {
                'min_date': df['date'].min() if 'date' in df.columns else None,
                'max_date': df['date'].max() if 'date' in df.columns else None
            },
            'countries_count': df['location'].nunique() if 'location' in df.columns else None
        })
        
        return file_info
        
    except Exception as e:
        logger.error(f"Failed to get file info: {str(e)}")
        return {}

def main():
    """Main function to download COVID-19 data."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download COVID-19 Data')
    parser.add_argument('--url', type=str, help='Custom URL for data download')
    parser.add_argument('--output', type=str, help='Custom output path')
    parser.add_argument('--verify-only', action='store_true', help='Only verify existing file')
    args = parser.parse_args()
    
    # Use command line arguments or defaults from config
    download_url = args.url or config.COVID_DATA_URL
    output_file = args.output or os.path.join(config.RAW_DATA_DIR, 'owid-covid-data.csv')
    
    logger.info("COVID-19 Data Download Script")
    logger.info("=" * 50)
    
    if args.verify_only:
        logger.info("Verification mode - checking existing file...")
        if verify_downloaded_file(output_file):
            file_info = get_file_info(output_file)
            if file_info:
                logger.info("File verification successful!")
                logger.info(f"Total rows: {file_info.get('total_rows', 'N/A')}")
                logger.info(f"Total columns: {file_info.get('total_columns', 'N/A')}")
                logger.info(f"Countries: {file_info.get('countries_count', 'N/A')}")
                if file_info.get('date_range', {}).get('min_date'):
                    logger.info(f"Date range: {file_info['date_range']['min_date']} to {file_info['date_range']['max_date']}")
        else:
            logger.error("File verification failed!")
            sys.exit(1)
        return
    
    # Check if file already exists
    if os.path.exists(output_file):
        logger.info(f"File already exists: {output_file}")
        response = input("Do you want to download again? (y/N): ").strip().lower()
        if response != 'y':
            logger.info("Skipping download. Using existing file.")
            if verify_downloaded_file(output_file):
                logger.info("Existing file is valid.")
                return
            else:
                logger.warning("Existing file appears to be invalid. Proceeding with download...")
    
    # Download the data
    start_time = time.time()
    success = download_covid_data(download_url, output_file)
    end_time = time.time()
    
    if success:
        logger.info(f"Download completed in {end_time - start_time:.2f} seconds")
        
        # Verify the downloaded file
        if verify_downloaded_file(output_file):
            file_info = get_file_info(output_file)
            if file_info:
                logger.info("Download Summary:")
                logger.info(f"  - File: {file_info['file_path']}")
                logger.info(f"  - Size: {file_info['file_size_mb']:.2f} MB")
                logger.info(f"  - Rows: {file_info['total_rows']:,}")
                logger.info(f"  - Columns: {file_info['total_columns']}")
                logger.info(f"  - Countries: {file_info['countries_count']}")
                if file_info['date_range']['min_date']:
                    logger.info(f"  - Date range: {file_info['date_range']['min_date']} to {file_info['date_range']['max_date']}")
            
            logger.info("Data download completed successfully!")
        else:
            logger.error("Downloaded file verification failed!")
            sys.exit(1)
    else:
        logger.error("Data download failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 