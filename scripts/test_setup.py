"""
Test script to verify the COVID-19 Data Engineering Project setup.
Tests database connection, data download, and basic ETL functionality.
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import config
from src.database import db_manager

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_database_connection():
    """Test database connection and basic operations."""
    logger.info("Testing database connection...")
    
    try:
        # Test connection
        if not db_manager.connect():
            logger.error("Database connection failed!")
            return False
        
        # Test table creation
        if not db_manager.create_tables():
            logger.error("Table creation failed!")
            return False
        
        # Test data insertion
        test_data = [
            {
                'date': '2020-01-01',
                'country': 'Test Country',
                'new_cases': 100,
                'total_cases': 100,
                'new_cases_7day_avg': 85.5
            }
        ]
        
        if not db_manager.insert_data(test_data):
            logger.error("Data insertion failed!")
            return False
        
        # Test data retrieval
        table_info = db_manager.get_table_info()
        if not table_info:
            logger.error("Data retrieval failed!")
            return False
        
        logger.info(f"Database test successful! Found {table_info['row_count']} records")
        
        # Clean up test data
        db_manager.clear_table()
        
        return True
        
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        return False

def test_data_download():
    """Test data download functionality."""
    logger.info("Testing data download...")
    
    try:
        from scripts.download_data import verify_downloaded_file, get_file_info
        
        raw_data_path = os.path.join(config.RAW_DATA_DIR, 'owid-covid-data.csv')
        
        if not os.path.exists(raw_data_path):
            logger.warning("Raw data file not found. Please run download_data.py first.")
            return False
        
        # Verify file
        if not verify_downloaded_file(raw_data_path):
            logger.error("Data file verification failed!")
            return False
        
        # Get file info
        file_info = get_file_info(raw_data_path)
        if file_info:
            logger.info(f"Data file test successful!")
            logger.info(f"  - Rows: {file_info.get('total_rows', 'N/A')}")
            logger.info(f"  - Columns: {file_info.get('total_columns', 'N/A')}")
            logger.info(f"  - Countries: {file_info.get('countries_count', 'N/A')}")
            return True
        else:
            logger.error("Failed to get file info!")
            return False
            
    except Exception as e:
        logger.error(f"Data download test failed: {str(e)}")
        return False

def test_pyspark_setup():
    """Test PySpark setup and basic functionality."""
    logger.info("Testing PySpark setup...")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("Test-Spark-Setup") \
            .config("spark.master", config.SPARK_MASTER) \
            .getOrCreate()
        
        # Test basic Spark operations
        test_data = [
            ("2020-01-01", "Test Country", 100, 100),
            ("2020-01-02", "Test Country", 150, 250)
        ]
        
        df = spark.createDataFrame(test_data, ["date", "country", "new_cases", "total_cases"])
        
        # Test filtering
        filtered_df = df.filter(col("new_cases") > 120)
        
        # Test aggregation
        result = df.groupBy("country").agg({"new_cases": "sum"}).collect()
        
        spark.stop()
        
        logger.info("PySpark test successful!")
        logger.info(f"  - Created DataFrame with {df.count()} rows")
        logger.info(f"  - Filtered to {filtered_df.count()} rows")
        logger.info(f"  - Aggregation result: {result}")
        
        return True
        
    except Exception as e:
        logger.error(f"PySpark test failed: {str(e)}")
        return False

def test_configuration():
    """Test configuration loading."""
    logger.info("Testing configuration...")
    
    try:
        # Test basic config values
        assert config.DB_HOST is not None, "DB_HOST not configured"
        assert config.DB_PORT is not None, "DB_PORT not configured"
        assert config.DB_NAME is not None, "DB_NAME not configured"
        assert config.TARGET_COUNTRIES is not None, "TARGET_COUNTRIES not configured"
        assert config.COVID_DATA_URL is not None, "COVID_DATA_URL not configured"
        
        logger.info("Configuration test successful!")
        logger.info(f"  - Database: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
        logger.info(f"  - Target countries: {config.TARGET_COUNTRIES}")
        logger.info(f"  - Data URL: {config.COVID_DATA_URL}")
        
        return True
        
    except Exception as e:
        logger.error(f"Configuration test failed: {str(e)}")
        return False

def test_directories():
    """Test directory structure."""
    logger.info("Testing directory structure...")
    
    try:
        # Ensure directories exist
        config.ensure_directories()
        
        # Check if directories were created
        directories = [config.RAW_DATA_DIR, config.PROCESSED_DATA_DIR, config.LOG_DIR]
        
        for directory in directories:
            if not os.path.exists(directory):
                logger.error(f"Directory not found: {directory}")
                return False
        
        logger.info("Directory structure test successful!")
        for directory in directories:
            logger.info(f"  - {directory}: {os.path.abspath(directory)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Directory test failed: {str(e)}")
        return False

def main():
    """Run all tests."""
    logger.info("COVID-19 Data Engineering Project - Setup Test")
    logger.info("=" * 60)
    
    tests = [
        ("Configuration", test_configuration),
        ("Directory Structure", test_directories),
        ("Database Connection", test_database_connection),
        ("Data Download", test_data_download),
        ("PySpark Setup", test_pyspark_setup)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\nRunning {test_name} test...")
        try:
            success = test_func()
            results.append((test_name, success))
            if success:
                logger.info(f"✓ {test_name} test PASSED")
            else:
                logger.error(f"✗ {test_name} test FAILED")
        except Exception as e:
            logger.error(f"✗ {test_name} test FAILED with exception: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "PASSED" if success else "FAILED"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Your setup is ready.")
        logger.info("\nNext steps:")
        logger.info("1. Run: python scripts/download_data.py")
        logger.info("2. Run: python src/etl_pipeline.py")
        logger.info("3. Or use Docker: docker-compose up --build")
        return True
    else:
        logger.error("❌ Some tests failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 