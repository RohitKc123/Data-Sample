"""
Database setup script for COVID-19 Data Engineering Project.
Creates PostgreSQL database and tables.
"""

import sys
import os
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_database():
    """Create PostgreSQL database if it doesn't exist."""
    try:
        # Connect to PostgreSQL server (not to a specific database)
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database='postgres'  # Connect to default postgres database
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (config.DB_NAME,))
        exists = cursor.fetchone()
        
        if not exists:
            # Create database
            cursor.execute(f'CREATE DATABASE "{config.DB_NAME}"')
            logger.info(f"Database '{config.DB_NAME}' created successfully")
        else:
            logger.info(f"Database '{config.DB_NAME}' already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database: {str(e)}")
        return False

def create_tables():
    """Create tables in the database."""
    try:
        # Connect to the specific database
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME
        )
        cursor = conn.cursor()
        
        # Create covid_data table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS covid_data (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            country VARCHAR(100) NOT NULL,
            new_cases INTEGER,
            total_cases INTEGER,
            new_cases_7day_avg DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, country)
        );
        """
        
        cursor.execute(create_table_sql)
        
        # Create index for better query performance
        create_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_covid_data_date_country 
        ON covid_data(date, country);
        """
        
        cursor.execute(create_index_sql)
        
        # Create additional useful indexes
        create_date_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_covid_data_date 
        ON covid_data(date);
        """
        
        cursor.execute(create_date_index_sql)
        
        create_country_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_covid_data_country 
        ON covid_data(country);
        """
        
        cursor.execute(create_country_index_sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Tables and indexes created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create tables: {str(e)}")
        return False

def verify_setup():
    """Verify that the database setup is correct."""
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME
        )
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'covid_data'
        """)
        
        table_exists = cursor.fetchone()
        
        if table_exists:
            # Get table structure
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'covid_data'
                ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            
            logger.info("Database setup verification successful!")
            logger.info("Table structure:")
            for column in columns:
                logger.info(f"  - {column[0]}: {column[1]} ({'NULL' if column[2] == 'YES' else 'NOT NULL'})")
            
            # Check indexes
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'covid_data'
            """)
            
            indexes = cursor.fetchall()
            logger.info("Indexes:")
            for index in indexes:
                logger.info(f"  - {index[0]}")
            
        else:
            logger.error("Table 'covid_data' not found!")
            return False
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to verify setup: {str(e)}")
        return False

def main():
    """Main function to set up the database."""
    logger.info("Starting database setup...")
    
    # Step 1: Create database
    logger.info("Step 1: Creating database...")
    if not create_database():
        logger.error("Database creation failed!")
        sys.exit(1)
    
    # Step 2: Create tables
    logger.info("Step 2: Creating tables...")
    if not create_tables():
        logger.error("Table creation failed!")
        sys.exit(1)
    
    # Step 3: Verify setup
    logger.info("Step 3: Verifying setup...")
    if not verify_setup():
        logger.error("Setup verification failed!")
        sys.exit(1)
    
    logger.info("Database setup completed successfully!")
    logger.info(f"Database: {config.DB_NAME}")
    logger.info(f"Host: {config.DB_HOST}:{config.DB_PORT}")
    logger.info(f"User: {config.DB_USER}")

if __name__ == "__main__":
    main() 