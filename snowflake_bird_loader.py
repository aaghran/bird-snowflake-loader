#!/usr/bin/env python3
"""
BIRD Dataset to Snowflake Loader

This script loads the BIRD SQL dataset into Snowflake.
The BIRD dataset contains 12,751 text-to-SQL pairs across 95 databases covering 37 professional domains.

Prerequisites:
1. Install required packages: pip install snowflake-connector-python datasets
2. Download BIRD dataset from HuggingFace: birdsql/bird-critic-1.0-flash-exp
3. Set up Snowflake credentials as environment variables or in config file
"""

import os
import sys
import json
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datasets import load_dataset
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SnowflakeBirdLoader:
    """Loads BIRD dataset into Snowflake"""
    
    def __init__(self, config: Dict):
        """
        Initialize the loader with Snowflake connection config
        
        Args:
            config: Dictionary containing Snowflake connection parameters
                - account: Snowflake account identifier
                - user: Username
                - password: Password
                - warehouse: Warehouse name
                - database: Database name
                - schema: Schema name
                - role: Role (optional)
        """
        self.config = config
        self.conn = None
        self.cursor = None
        
    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(
                account=self.config['account'],
                user=self.config['user'],
                password=self.config['password'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema'],
                role=self.config.get('role', 'ACCOUNTADMIN')
            )
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to Snowflake")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def create_bird_schema(self):
        """Create the necessary tables for BIRD dataset"""
        
        # Create main BIRD questions table
        questions_ddl = """
        CREATE OR REPLACE TABLE bird_questions (
            id VARCHAR(50) PRIMARY KEY,
            db_id VARCHAR(100),
            question TEXT,
            evidence TEXT,
            sql_query TEXT,
            difficulty VARCHAR(20),
            domain VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create database metadata table
        databases_ddl = """
        CREATE OR REPLACE TABLE bird_databases (
            db_id VARCHAR(100) PRIMARY KEY,
            db_name VARCHAR(200),
            domain VARCHAR(50),
            num_tables INTEGER,
            description TEXT,
            size_mb FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create table metadata table
        tables_ddl = """
        CREATE OR REPLACE TABLE bird_tables (
            id VARCHAR(50) PRIMARY KEY,
            db_id VARCHAR(100),
            table_name VARCHAR(200),
            column_names ARRAY,
            column_types ARRAY,
            primary_keys ARRAY,
            foreign_keys ARRAY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create execution results table
        execution_ddl = """
        CREATE OR REPLACE TABLE bird_execution_results (
            question_id VARCHAR(50),
            execution_time_ms FLOAT,
            result_rows INTEGER,
            execution_success BOOLEAN,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        try:
            self.cursor.execute(questions_ddl)
            self.cursor.execute(databases_ddl)
            self.cursor.execute(tables_ddl)
            self.cursor.execute(execution_ddl)
            logger.info("Created BIRD schema tables successfully")
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    def download_bird_dataset(self) -> Dict:
        """Download BIRD dataset from HuggingFace"""
        try:
            logger.info("Downloading BIRD dataset from HuggingFace...")
            dataset = load_dataset("birdsql/bird-critic-1.0-flash-exp")
            logger.info(f"Downloaded dataset with {len(dataset['flash'])} examples")
            return dataset
        except Exception as e:
            logger.error(f"Failed to download BIRD dataset: {e}")
            raise
    
    def process_bird_data(self, dataset) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Process BIRD dataset into pandas DataFrames"""
        
        questions_data = []
        databases_data = {}
        tables_data = []
        
        for idx, item in enumerate(dataset['flash']):
            # Process question data
            question_record = {
                'id': f"bird_{idx:06d}",
                'db_id': item.get('db_id', ''),
                'question': item.get('question', ''),
                'evidence': item.get('evidence', ''),
                'sql_query': item.get('SQL', ''),
                'difficulty': item.get('difficulty', 'unknown'),
                'domain': self._extract_domain(item.get('db_id', ''))
            }
            questions_data.append(question_record)
            
            # Process database metadata
            db_id = item.get('db_id', '')
            if db_id and db_id not in databases_data:
                databases_data[db_id] = {
                    'db_id': db_id,
                    'db_name': db_id,
                    'domain': self._extract_domain(db_id),
                    'num_tables': 0,  # Will be updated if we have schema info
                    'description': f"Database from BIRD benchmark: {db_id}",
                    'size_mb': 0.0  # Placeholder
                }
        
        questions_df = pd.DataFrame(questions_data)
        databases_df = pd.DataFrame(list(databases_data.values()))
        tables_df = pd.DataFrame(tables_data)  # Empty for now
        
        return questions_df, databases_df, tables_df
    
    def _extract_domain(self, db_id: str) -> str:
        """Extract domain from database ID"""
        # Simple heuristic to extract domain
        common_domains = {
            'financial': ['bank', 'finance', 'trading', 'stock'],
            'healthcare': ['hospital', 'medical', 'patient', 'health'],
            'education': ['school', 'student', 'university', 'college'],
            'retail': ['store', 'shop', 'retail', 'customer'],
            'sports': ['game', 'sport', 'team', 'player'],
            'technology': ['software', 'tech', 'computer', 'system']
        }
        
        db_id_lower = db_id.lower()
        for domain, keywords in common_domains.items():
            if any(keyword in db_id_lower for keyword in keywords):
                return domain
        return 'general'
    
    def load_to_snowflake(self, questions_df: pd.DataFrame, databases_df: pd.DataFrame, tables_df: pd.DataFrame):
        """Load DataFrames to Snowflake tables"""
        
        try:
            # Load questions
            logger.info(f"Loading {len(questions_df)} questions to Snowflake...")
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                questions_df, 
                'BIRD_QUESTIONS',
                auto_create_table=False,
                overwrite=True
            )
            logger.info(f"Loaded {nrows} question records")
            
            # Load databases
            logger.info(f"Loading {len(databases_df)} database records to Snowflake...")
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                databases_df, 
                'BIRD_DATABASES',
                auto_create_table=False,
                overwrite=True
            )
            logger.info(f"Loaded {nrows} database records")
            
            # Load tables (if any)
            if not tables_df.empty:
                logger.info(f"Loading {len(tables_df)} table records to Snowflake...")
                success, nchunks, nrows, _ = write_pandas(
                    self.conn, 
                    tables_df, 
                    'BIRD_TABLES',
                    auto_create_table=False,
                    overwrite=True
                )
                logger.info(f"Loaded {nrows} table records")
            
        except Exception as e:
            logger.error(f"Failed to load data to Snowflake: {e}")
            raise
    
    def create_sample_views(self):
        """Create useful views for analyzing BIRD data"""
        
        views = [
            """
            CREATE OR REPLACE VIEW bird_summary AS
            SELECT 
                domain,
                COUNT(*) as question_count,
                COUNT(DISTINCT db_id) as database_count,
                AVG(LENGTH(question)) as avg_question_length,
                AVG(LENGTH(sql_query)) as avg_sql_length
            FROM bird_questions 
            GROUP BY domain
            ORDER BY question_count DESC
            """,
            
            """
            CREATE OR REPLACE VIEW complex_questions AS
            SELECT 
                id, db_id, question, difficulty,
                LENGTH(sql_query) as sql_length,
                (LENGTH(sql_query) - LENGTH(REPLACE(sql_query, 'JOIN', ''))) / 4 as join_count
            FROM bird_questions
            WHERE difficulty IN ('hard', 'extra_hard') OR LENGTH(sql_query) > 200
            ORDER BY sql_length DESC
            """,
            
            """
            CREATE OR REPLACE VIEW domain_complexity AS
            SELECT 
                domain,
                AVG(LENGTH(sql_query)) as avg_sql_complexity,
                COUNT(CASE WHEN sql_query LIKE '%JOIN%' THEN 1 END) as join_questions,
                COUNT(CASE WHEN sql_query LIKE '%SUBQUERY%' OR sql_query LIKE '%(%' THEN 1 END) as subquery_questions,
                COUNT(*) as total_questions
            FROM bird_questions
            GROUP BY domain
            ORDER BY avg_sql_complexity DESC
            """
        ]
        
        for view_sql in views:
            try:
                self.cursor.execute(view_sql)
                logger.info("Created view successfully")
            except Exception as e:
                logger.error(f"Failed to create view: {e}")
    
    def validate_load(self):
        """Validate the data load"""
        validation_queries = [
            "SELECT COUNT(*) as total_questions FROM bird_questions",
            "SELECT COUNT(DISTINCT db_id) as unique_databases FROM bird_questions",
            "SELECT domain, COUNT(*) as count FROM bird_questions GROUP BY domain ORDER BY count DESC LIMIT 5",
            "SELECT difficulty, COUNT(*) as count FROM bird_questions WHERE difficulty IS NOT NULL GROUP BY difficulty"
        ]
        
        logger.info("Validating data load...")
        for query in validation_queries:
            try:
                self.cursor.execute(query)
                results = self.cursor.fetchall()
                logger.info(f"Query: {query}")
                logger.info(f"Results: {results}")
            except Exception as e:
                logger.error(f"Validation query failed: {e}")
    
    def close(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Closed Snowflake connection")

def load_config() -> Dict:
    """Load Snowflake configuration from environment or config file"""
    
    # Try environment variables first
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'BIRD_DB'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    }
    
    # Check if config file exists
    config_file = Path('snowflake_config.json')
    if config_file.exists():
        with open(config_file, 'r') as f:
            file_config = json.load(f)
            # Override with file config, but keep env vars if they exist
            for key, value in file_config.items():
                if not config.get(key):
                    config[key] = value
    
    # Validate required fields
    required_fields = ['account', 'user', 'password']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        logger.error(f"Missing required configuration fields: {missing_fields}")
        logger.info("Set environment variables or create snowflake_config.json with:")
        logger.info(json.dumps({
            "account": "your-account.snowflakecomputing.com",
            "user": "your-username",
            "password": "your-password",
            "warehouse": "COMPUTE_WH",
            "database": "BIRD_DB",
            "schema": "PUBLIC"
        }, indent=2))
        sys.exit(1)
    
    return config

def main():
    """Main function to orchestrate the BIRD dataset loading"""
    
    logger.info("Starting BIRD dataset loading to Snowflake...")
    
    # Load configuration
    config = load_config()
    
    # Initialize loader
    loader = SnowflakeBirdLoader(config)
    
    try:
        # Connect to Snowflake
        if not loader.connect():
            return 1
        
        # Create schema
        loader.create_bird_schema()
        
        # Download and process BIRD dataset
        dataset = loader.download_bird_dataset()
        questions_df, databases_df, tables_df = loader.process_bird_data(dataset)
        
        # Load to Snowflake
        loader.load_to_snowflake(questions_df, databases_df, tables_df)
        
        # Create helpful views
        loader.create_sample_views()
        
        # Validate the load
        loader.validate_load()
        
        logger.info("Successfully loaded BIRD dataset to Snowflake!")
        
    except Exception as e:
        logger.error(f"Failed to load BIRD dataset: {e}")
        return 1
    
    finally:
        loader.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())