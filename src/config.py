"""
Configuration module for COVID-19 Data Engineering Project.
Loads environment variables and defines project settings.
"""

import os
from dotenv import load_dotenv
from typing import List

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class containing all project settings."""
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'covid_data')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'your_password')
    
    # Data Source Configuration
    COVID_DATA_URL = os.getenv('COVID_DATA_URL', 
                              'https://covid.ourworldindata.org/data/owid-covid-data.csv')
    
    # Target Countries
    TARGET_COUNTRIES = os.getenv('TARGET_COUNTRIES', 'Nepal,India,United States').split(',')
    
    # Spark Configuration
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
    SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '2g')
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '2g')
    
    # File Paths
    RAW_DATA_DIR = os.getenv('RAW_DATA_DIR', 'data/raw')
    PROCESSED_DATA_DIR = os.getenv('PROCESSED_DATA_DIR', 'data/processed')
    LOG_DIR = os.getenv('LOG_DIR', 'logs')
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Data Processing Configuration
    ROLLING_WINDOW_DAYS = 7
    MIN_DATE = '2020-01-01'  # Start date for data processing
    
    @classmethod
    def get_database_url(cls) -> str:
        """Generate database connection URL."""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def get_spark_config(cls) -> dict:
        """Get Spark configuration dictionary."""
        return {
            'spark.master': cls.SPARK_MASTER,
            'spark.driver.memory': cls.SPARK_DRIVER_MEMORY,
            'spark.executor.memory': cls.SPARK_EXECUTOR_MEMORY,
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.skewJoin.enabled': 'true'
        }
    
    @classmethod
    def ensure_directories(cls) -> None:
        """Ensure all required directories exist."""
        directories = [cls.RAW_DATA_DIR, cls.PROCESSED_DATA_DIR, cls.LOG_DIR]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)

# Global configuration instance
config = Config() 