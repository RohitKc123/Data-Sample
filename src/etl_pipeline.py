"""
Main ETL Pipeline for COVID-19 Data Processing.
Extracts data from CSV, transforms using PySpark, and loads into PostgreSQL.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional, List
import pandas as pd

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import config
from src.database import db_manager

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_DIR, 'etl_pipeline.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class COVIDETLPipeline:
    """Main ETL pipeline for COVID-19 data processing."""
    
    def __init__(self):
        self.spark = None
        self.raw_data_path = os.path.join(config.RAW_DATA_DIR, 'owid-covid-data.csv')
        self.processed_data_path = os.path.join(config.PROCESSED_DATA_DIR, 'covid_processed.csv')
        
    def initialize_spark(self):
        """Initialize PySpark session with configuration."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, when, avg, window, sum as spark_sum
            
            # Create Spark session
            self.spark = SparkSession.builder \
                .appName("COVID-19-ETL-Pipeline") \
                .config("spark.master", config.SPARK_MASTER) \
                .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
                .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            # Import functions for use in class
            self.col = col
            self.when = when
            self.avg = avg
            self.window = window
            self.spark_sum = spark_sum
            
            logger.info("PySpark session initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize PySpark: {str(e)}")
            return False
    
    def stop_spark(self):
        """Stop PySpark session."""
        if self.spark:
            self.spark.stop()
            logger.info("PySpark session stopped")
    
    def extract_data(self) -> bool:
        """Extract data from CSV file."""
        try:
            if not os.path.exists(self.raw_data_path):
                logger.error(f"Raw data file not found: {self.raw_data_path}")
                logger.info("Please run download_data.py first to download the dataset")
                return False
            
            # Read CSV with PySpark
            self.df = self.spark.read.csv(
                self.raw_data_path,
                header=True,
                inferSchema=True
            )
            
            logger.info(f"Successfully loaded data: {self.df.count()} rows, {len(self.df.columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to extract data: {str(e)}")
            return False
    
    def transform_data(self) -> bool:
        """Transform data using PySpark operations."""
        try:
            logger.info("Starting data transformation...")
            
            # Filter for target countries
            target_countries = config.TARGET_COUNTRIES
            filtered_df = self.df.filter(self.col("location").isin(target_countries))
            
            logger.info(f"Filtered for countries {target_countries}: {filtered_df.count()} rows")
            
            # Select relevant columns and rename for clarity
            selected_df = filtered_df.select(
                self.col("date").cast("date").alias("date"),
                self.col("location").alias("country"),
                self.col("new_cases").cast("int").alias("new_cases"),
                self.col("total_cases").cast("int").alias("total_cases")
            )
            
            # Remove rows with missing values
            cleaned_df = selected_df.na.drop(subset=["date", "country", "new_cases", "total_cases"])
            
            logger.info(f"After removing missing values: {cleaned_df.count()} rows")
            
            # Filter for dates after minimum date
            from pyspark.sql.functions import to_date
            min_date = config.MIN_DATE
            date_filtered_df = cleaned_df.filter(
                self.col("date") >= to_date(self.col("date"), "yyyy-MM-dd")
            )
            
            # Calculate 7-day rolling average of new cases
            window_spec = self.window("date", f"{config.ROLLING_WINDOW_DAYS} days")
            
            final_df = date_filtered_df.withColumn(
                "new_cases_7day_avg",
                self.avg("new_cases").over(window_spec)
            )
            
            # Sort by country and date
            self.processed_df = final_df.orderBy("country", "date")
            
            logger.info(f"Transformation completed: {self.processed_df.count()} rows")
            return True
            
        except Exception as e:
            logger.error(f"Failed to transform data: {str(e)}")
            return False
    
    def load_to_csv(self) -> bool:
        """Save processed data to CSV file."""
        try:
            # Ensure output directory exists
            os.makedirs(config.PROCESSED_DATA_DIR, exist_ok=True)
            
            # Convert to pandas DataFrame and save as CSV
            pandas_df = self.processed_df.toPandas()
            pandas_df.to_csv(self.processed_data_path, index=False)
            
            logger.info(f"Data saved to CSV: {self.processed_data_path}")
            logger.info(f"CSV file contains {len(pandas_df)} rows")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save CSV: {str(e)}")
            return False
    
    def load_to_database(self) -> bool:
        """Load processed data into PostgreSQL database."""
        try:
            # Convert Spark DataFrame to pandas for database insertion
            pandas_df = self.processed_df.toPandas()
            
            # Convert to list of dictionaries for database insertion
            data_records = pandas_df.to_dict('records')
            
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
            logger.info("Starting COVID-19 ETL Pipeline...")
            
            # Ensure directories exist
            config.ensure_directories()
            
            # Initialize Spark
            if not self.initialize_spark():
                return False
            
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
            self.stop_spark()
            db_manager.disconnect()

def main():
    """Main function to run the ETL pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='COVID-19 ETL Pipeline')
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
    pipeline = COVIDETLPipeline()
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