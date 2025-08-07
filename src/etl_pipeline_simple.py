"""
Simplified ETL Pipeline for COVID-19 Data Processing using Pandas.
This version doesn't require Java/PySpark and is easier to run.
"""

import logging
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import config
from src.database import db_manager

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_DIR, 'etl_pipeline_simple.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SimpleCOVIDETLPipeline:
    """Simplified ETL pipeline using pandas instead of PySpark."""
    
    def __init__(self):
        self.raw_data_path = os.path.join(config.RAW_DATA_DIR, 'owid-covid-data.csv')
        self.processed_data_path = os.path.join(config.PROCESSED_DATA_DIR, 'covid_processed.csv')
        
    def extract_data(self) -> bool:
        """Extract data from CSV file."""
        try:
            if not os.path.exists(self.raw_data_path):
                logger.error(f"Raw data file not found: {self.raw_data_path}")
                logger.info("Please run download_data.py first to download the dataset")
                return False
            
            # Read CSV with pandas
            self.df = pd.read_csv(self.raw_data_path)
            
            logger.info(f"Successfully loaded data: {len(self.df)} rows, {len(self.df.columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to extract data: {str(e)}")
            return False
    
    def transform_data(self) -> bool:
        """Transform data using pandas operations."""
        try:
            logger.info("Starting data transformation...")
            
            # Filter for target countries
            target_countries = config.TARGET_COUNTRIES
            filtered_df = self.df[self.df['location'].isin(target_countries)].copy()
            
            logger.info(f"Filtered for countries {target_countries}: {len(filtered_df)} rows")
            
            # Select relevant columns and rename for clarity
            selected_df = filtered_df[['date', 'location', 'new_cases', 'total_cases']].copy()
            selected_df.columns = ['date', 'country', 'new_cases', 'total_cases']
            
            # Convert date column
            selected_df['date'] = pd.to_datetime(selected_df['date'])
            
            # Remove rows with missing values
            cleaned_df = selected_df.dropna(subset=['date', 'country', 'new_cases', 'total_cases'])
            
            logger.info(f"After removing missing values: {len(cleaned_df)} rows")
            
            # Filter for dates after minimum date
            min_date = pd.to_datetime(config.MIN_DATE)
            date_filtered_df = cleaned_df[cleaned_df['date'] >= min_date].copy()
            
            # Calculate 7-day rolling average of new cases
            date_filtered_df = date_filtered_df.sort_values(['country', 'date'])
            date_filtered_df['new_cases_7day_avg'] = date_filtered_df.groupby('country')['new_cases'].rolling(
                window=config.ROLLING_WINDOW_DAYS, min_periods=1
            ).mean().reset_index(0, drop=True)
            
            # Sort by country and date
            self.processed_df = date_filtered_df.sort_values(['country', 'date'])
            
            logger.info(f"Transformation completed: {len(self.processed_df)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Failed to transform data: {str(e)}")
            return False
    
    def load_to_csv(self) -> bool:
        """Save processed data to CSV file."""
        try:
            # Ensure output directory exists
            os.makedirs(config.PROCESSED_DATA_DIR, exist_ok=True)
            
            # Save as CSV
            self.processed_df.to_csv(self.processed_data_path, index=False)
            
            logger.info(f"Data saved to CSV: {self.processed_data_path}")
            logger.info(f"CSV file contains {len(self.processed_df)} rows")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save CSV: {str(e)}")
            return False
    
    def load_to_database(self) -> bool:
        """Load processed data into PostgreSQL database."""
        try:
            # Convert to list of dictionaries for database insertion
            data_records = self.processed_df.to_dict('records')
            
            # Insert data into database
            success = db_manager.insert_data(data_records)
            
            if success:
                logger.info(f"Successfully loaded {len(data_records)} records to database")
                return True
            else:
                logger.error("Failed to load data to database")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load data to database: {str(e)}")
            return False
    
    def run_pipeline(self) -> bool:
        """Run the complete ETL pipeline."""
        try:
            logger.info("Starting COVID-19 ETL Pipeline (Pandas Version)...")
            
            # Ensure directories exist
            config.ensure_directories()
            
            # Extract
            logger.info("Step 1: Extracting data...")
            if not self.extract_data():
                return False
            
            # Transform
            logger.info("Step 2: Transforming data...")
            if not self.transform_data():
                return False
            
            # Load to CSV
            logger.info("Step 3: Loading data to CSV...")
            if not self.load_to_csv():
                return False
            
            # Load to Database
            logger.info("Step 4: Loading data to database...")
            if not self.load_to_database():
                return False
            
            logger.info("ETL Pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False
        
        finally:
            # Clean up
            db_manager.disconnect()

def main():
    """Main function to run the simplified ETL pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='COVID-19 ETL Pipeline (Pandas Version)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Initialize database connection
    if not db_manager.connect():
        logger.error("Failed to connect to database. Please check your configuration.")
        sys.exit(1)
    
    # Create tables if they don't exist
    if not db_manager.create_tables():
        logger.error("Failed to create database tables.")
        sys.exit(1)
    
    # Run pipeline
    pipeline = SimpleCOVIDETLPipeline()
    success = pipeline.run_pipeline()
    
    if success:
        # Display summary
        table_info = db_manager.get_table_info()
        if table_info:
            logger.info(f"Database summary: {table_info['row_count']} records in {table_info['table_name']}")
        
        logger.info("Pipeline completed successfully!")
        sys.exit(0)
    else:
        logger.error("Pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 