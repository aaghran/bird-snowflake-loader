#!/usr/bin/env python3
"""
BIRD Dataset Test Loader for Snowflake

This script loads just 1-2 tables from the BIRD dataset as a test to validate
the Snowflake connection and loading process before doing the full load.
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
from typing import Dict
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datasets import load_dataset
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SnowflakeBirdTestLoader:
    """Simple test loader for BIRD dataset"""
    
    def __init__(self, config: Dict):
        """Initialize with Snowflake config"""
        self.config = config
        self.conn = None
        self.cursor = None
        
    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            role = self.config.get('role', 'DEV')
            logger.info(f"Using role: {role}")
            self.conn = snowflake.connector.connect(
                account=self.config['account'],
                user=self.config['user'],
                password=self.config['password'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema'],
                role=role
            )
            self.cursor = self.conn.cursor()
            logger.info("✅ Successfully connected to Snowflake")
            logger.info(f"Connected to: {self.config['database']}.{self.config['schema']}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            return False
    
    def test_warehouse(self):
        """Test warehouse is running"""
        try:
            self.cursor.execute("SELECT CURRENT_WAREHOUSE()")
            warehouse = self.cursor.fetchone()[0]
            logger.info(f"✅ Using warehouse: {warehouse}")
            
            self.cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
            db, schema = self.cursor.fetchone()
            logger.info(f"✅ Using database.schema: {db}.{schema}")
            
        except Exception as e:
            logger.error(f"❌ Warehouse test failed: {e}")
            raise
    
    def create_test_tables(self):
        """Create minimal test tables"""
        
        # Simple test questions table
        questions_ddl = """
        CREATE OR REPLACE TABLE bird_test_questions (
            id VARCHAR(50) PRIMARY KEY,
            db_id VARCHAR(100),
            question TEXT,
            sql_query TEXT,
            difficulty VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Simple test summary table
        summary_ddl = """
        CREATE OR REPLACE TABLE bird_test_summary (
            domain VARCHAR(50),
            question_count INTEGER,
            avg_question_length FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        try:
            logger.info("Creating test tables...")
            self.cursor.execute(questions_ddl)
            logger.info("✅ Created bird_test_questions table")
            
            self.cursor.execute(summary_ddl)
            logger.info("✅ Created bird_test_summary table")
            
        except Exception as e:
            logger.error(f"❌ Failed to create test tables: {e}")
            raise
    
    def load_sample_data(self, limit: int = 50):
        """Load a small sample of BIRD data"""
        try:
            logger.info("Downloading sample BIRD dataset...")
            dataset = load_dataset("birdsql/bird-critic-1.0-flash-exp", split="flash[:100]")  # Only first 100 records
            logger.info(f"✅ Downloaded {len(dataset)} sample records")
            
            # Process sample data
            questions_data = []
            for idx, item in enumerate(dataset):
                if idx >= limit:  # Limit to specified number
                    break
                    
                question_record = {
                    'id': f"test_{idx:03d}",
                    'db_id': item.get('db_id', ''),
                    'question': item.get('question', '')[:1000],  # Truncate long questions
                    'sql_query': item.get('SQL', '')[:2000],      # Truncate long SQL
                    'difficulty': item.get('difficulty', 'unknown')
                }
                questions_data.append(question_record)
            
            # Load questions
            questions_df = pd.DataFrame(questions_data)
            # Convert column names to uppercase for Snowflake
            questions_df.columns = [col.upper() for col in questions_df.columns]
            logger.info(f"Loading {len(questions_df)} sample questions...")
            
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                questions_df, 
                'BIRD_TEST_QUESTIONS',
                auto_create_table=False,
                overwrite=True
            )
            logger.info(f"✅ Loaded {nrows} question records")
            
            # Create summary data
            summary_data = []
            domain_counts = questions_df.groupby('DB_ID').size().to_dict()  # Use uppercase after conversion
            
            for db_id, count in list(domain_counts.items())[:5]:  # Top 5 databases
                domain = self._extract_domain(db_id)
                avg_length = questions_df[questions_df['DB_ID'] == db_id]['QUESTION'].str.len().mean()  # Use uppercase
                
                summary_data.append({
                    'domain': domain,
                    'question_count': count,
                    'avg_question_length': avg_length
                })
            
            # Load summary
            summary_df = pd.DataFrame(summary_data)
            # Convert column names to uppercase for Snowflake
            summary_df.columns = [col.upper() for col in summary_df.columns]
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                summary_df, 
                'BIRD_TEST_SUMMARY',
                auto_create_table=False,
                overwrite=True
            )
            logger.info(f"✅ Loaded {nrows} summary records")
            
        except Exception as e:
            logger.error(f"❌ Failed to load sample data: {e}")
            raise
    
    def _extract_domain(self, db_id: str) -> str:
        """Simple domain extraction"""
        db_id_lower = db_id.lower()
        if any(word in db_id_lower for word in ['bank', 'finance', 'money']):
            return 'financial'
        elif any(word in db_id_lower for word in ['hospital', 'medical', 'health']):
            return 'healthcare'
        elif any(word in db_id_lower for word in ['school', 'student', 'university']):
            return 'education'
        elif any(word in db_id_lower for word in ['store', 'shop', 'retail']):
            return 'retail'
        elif any(word in db_id_lower for word in ['sport', 'game', 'team']):
            return 'sports'
        else:
            return 'general'
    
    def run_test_queries(self):
        """Run test queries to validate the load"""
        
        test_queries = [
            ("Total questions", "SELECT COUNT(*) FROM bird_test_questions"),
            ("Sample questions", "SELECT id, db_id, LEFT(question, 100) as question_preview FROM bird_test_questions LIMIT 3"),
            ("Questions by database", "SELECT db_id, COUNT(*) as count FROM bird_test_questions GROUP BY db_id ORDER BY count DESC LIMIT 3"),
            ("Domain summary", "SELECT * FROM bird_test_summary"),
            ("SQL complexity", "SELECT difficulty, COUNT(*) as count, AVG(LENGTH(sql_query)) as avg_sql_length FROM bird_test_questions WHERE difficulty IS NOT NULL GROUP BY difficulty")
        ]
        
        logger.info("Running validation queries...")
        
        for description, query in test_queries:
            try:
                logger.info(f"\n📊 {description}:")
                self.cursor.execute(query)
                results = self.cursor.fetchall()
                
                # Get column names
                columns = [desc[0] for desc in self.cursor.description]
                
                # Print results in a readable format
                if results:
                    # Print header
                    print("  " + " | ".join(f"{col:15}" for col in columns))
                    print("  " + "-" * (len(columns) * 18))
                    
                    # Print rows (limit to 5 for readability)
                    for row in results[:5]:
                        formatted_row = []
                        for value in row:
                            if isinstance(value, str) and len(value) > 15:
                                formatted_row.append(value[:12] + "...")
                            else:
                                formatted_row.append(str(value))
                        print("  " + " | ".join(f"{val:15}" for val in formatted_row))
                else:
                    print("  No results")
                    
            except Exception as e:
                logger.error(f"❌ Query failed: {e}")
    
    def cleanup_test_tables(self):
        """Optional: Clean up test tables"""
        try:
            self.cursor.execute("DROP TABLE IF EXISTS bird_test_questions")
            self.cursor.execute("DROP TABLE IF EXISTS bird_test_summary")
            logger.info("🧹 Cleaned up test tables")
        except Exception as e:
            logger.warning(f"⚠️ Cleanup warning: {e}")
    
    def close(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("🔌 Closed Snowflake connection")

def load_config() -> Dict:
    """Load Snowflake configuration"""
    
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'BIRD_DB'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    }
    
    # Try config file
    config_file = Path('snowflake_config.json')
    if config_file.exists():
        with open(config_file, 'r') as f:
            file_config = json.load(f)
            for key, value in file_config.items():
                config[key] = value  # Override env vars with config file values
    
    # Validate required fields
    required_fields = ['account', 'user', 'password']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        logger.error(f"❌ Missing required config: {missing_fields}")
        logger.info("💡 Set environment variables or edit snowflake_config.json")
        return None
    
    return config

def main():
    """Main test function"""
    
    print("🧪 BIRD Dataset Test Loader for Snowflake")
    print("=" * 50)
    
    # Load config
    config = load_config()
    if not config:
        return 1
    
    loader = SnowflakeBirdTestLoader(config)
    
    try:
        # Test connection
        if not loader.connect():
            return 1
        
        # Test warehouse
        loader.test_warehouse()
        
        # Create test tables
        loader.create_test_tables()
        
        # Load sample data
        sample_size = 25  # Very small for testing
        loader.load_sample_data(limit=sample_size)
        
        # Run validation queries
        loader.run_test_queries()
        
        print("\n✅ Test completed successfully!")
        print("💡 Ready to run the full loader: python snowflake_bird_full_loader.py")
        
        # Ask if user wants to clean up
        cleanup = input("\n🧹 Clean up test tables? (y/N): ").lower().strip()
        if cleanup == 'y':
            loader.cleanup_test_tables()
        else:
            print("💾 Test tables preserved for inspection")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return 1
    
    finally:
        loader.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())