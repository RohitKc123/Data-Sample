"""
Database utilities for PostgreSQL connection and table management.
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, Any
from .config import config

# Configure logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages PostgreSQL database connections and operations."""
    
    def __init__(self):
        self.connection = None
        self.engine = None
    
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            # Create SQLAlchemy engine for pandas operations
            self.engine = create_engine(config.get_database_url())
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Database connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
    
    def create_tables(self) -> bool:
        """Create necessary database tables."""
        try:
            with self.engine.connect() as conn:
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
                
                conn.execute(text(create_table_sql))
                conn.commit()
                
                # Create index for better query performance
                index_sql = """
                CREATE INDEX IF NOT EXISTS idx_covid_data_date_country 
                ON covid_data(date, country);
                """
                conn.execute(text(index_sql))
                conn.commit()
                
                logger.info("Database tables created successfully")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to create tables: {str(e)}")
            return False
    
    def insert_data(self, data: list) -> bool:
        """Insert data into covid_data table."""
        try:
            # Convert list of dictionaries to DataFrame and insert
            import pandas as pd
            df = pd.DataFrame(data)
            
            df.to_sql('covid_data', self.engine, if_exists='append', 
                     index=False, method='multi')
            
            logger.info(f"Successfully inserted {len(data)} records into database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert data: {str(e)}")
            return False
    
    def clear_table(self, table_name: str = 'covid_data') -> bool:
        """Clear all data from specified table."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {table_name}"))
                conn.commit()
                logger.info(f"Cleared all data from {table_name} table")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to clear table: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str = 'covid_data') -> Optional[Dict[str, Any]]:
        """Get table information including row count."""
        try:
            with self.engine.connect() as conn:
                # Get row count
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = count_result.scalar()
                
                # Get sample data
                sample_result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 5"))
                sample_data = [dict(row._mapping) for row in sample_result]
                
                return {
                    'table_name': table_name,
                    'row_count': row_count,
                    'sample_data': sample_data
                }
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get table info: {str(e)}")
            return None
    
    def execute_query(self, query: str) -> Optional[list]:
        """Execute a custom SQL query and return results."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row) for row in result]
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to execute query: {str(e)}")
            return None

# Global database manager instance
db_manager = DatabaseManager() 