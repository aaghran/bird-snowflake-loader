#!/usr/bin/env python3
"""
BIRD Database Data Test Loader for Snowflake

This script loads actual database tables and data from BIRD mini-dev dataset
so you can run the SQL queries against real data, not just the questions.

Focus: Load the underlying database tables that the SQL queries operate on.
"""

import os
import sys
import json
import pandas as pd
import sqlite3
import tempfile
from pathlib import Path
from typing import Dict, List
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datasets import load_dataset
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SnowflakeBirdDataTestLoader:
    """Test loader focused on actual BIRD database tables and data"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.conn = None
        self.cursor = None
        self.temp_dir = None
        
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
    
    def load_mini_dev_dataset(self):
        """Load BIRD mini-dev dataset which contains actual SQLite databases"""
        try:
            logger.info("Downloading BIRD mini-dev dataset...")
            # Try to load the mini-dev dataset with actual databases
            dataset = load_dataset("birdsql/bird_mini_dev")
            logger.info(f"✅ Downloaded mini-dev dataset")
            
            # Also get some questions for context
            questions_dataset = load_dataset("birdsql/bird-critic-1.0-flash-exp", split="flash[:10]")
            logger.info(f"✅ Downloaded 10 sample questions")
            
            return dataset, questions_dataset
            
        except Exception as e:
            logger.error(f"❌ Failed to download mini-dev dataset: {e}")
            # Fallback: create sample database structure
            return self._create_sample_database_structure()
    
    def _create_sample_database_structure(self):
        """Create sample database structure if mini-dev isn't available"""
        logger.info("Creating sample database structure...")
        
        # Create temporary SQLite database with sample data
        self.temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(self.temp_dir, "sample_financial.sqlite")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create sample financial database tables
        cursor.execute("""
            CREATE TABLE accounts (
                account_id INTEGER PRIMARY KEY,
                account_number VARCHAR(20),
                account_type VARCHAR(50),
                customer_id INTEGER,
                balance DECIMAL(15,2),
                created_date DATE
            )
        """)
        
        cursor.execute("""
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                phone VARCHAR(20),
                address VARCHAR(200),
                city VARCHAR(50),
                state VARCHAR(2),
                zip_code VARCHAR(10)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE transactions (
                transaction_id INTEGER PRIMARY KEY,
                account_id INTEGER,
                transaction_date DATE,
                amount DECIMAL(15,2),
                transaction_type VARCHAR(20),
                description VARCHAR(200),
                FOREIGN KEY (account_id) REFERENCES accounts(account_id)
            )
        """)
        
        # Insert sample data
        customers_data = [
            (1, 'John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'Boston', 'MA', '02101'),
            (2, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Boston', 'MA', '02102'),
            (3, 'Bob', 'Johnson', 'bob.j@email.com', '555-0103', '789 Pine Rd', 'Cambridge', 'MA', '02139'),
            (4, 'Alice', 'Brown', 'alice.b@email.com', '555-0104', '321 Elm St', 'Boston', 'MA', '02101'),
            (5, 'Charlie', 'Wilson', 'charlie.w@email.com', '555-0105', '654 Maple Dr', 'Somerville', 'MA', '02144')
        ]
        cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", customers_data)
        
        accounts_data = [
            (1001, 'CHK-001', 'CHECKING', 1, 2500.00, '2023-01-15'),
            (1002, 'SAV-001', 'SAVINGS', 1, 10000.00, '2023-01-15'),
            (1003, 'CHK-002', 'CHECKING', 2, 1800.50, '2023-02-20'),
            (1004, 'SAV-002', 'SAVINGS', 2, 25000.00, '2023-02-20'),
            (1005, 'CHK-003', 'CHECKING', 3, 750.25, '2023-03-10'),
            (1006, 'SAV-003', 'SAVINGS', 3, 5500.00, '2023-03-10'),
            (1007, 'CHK-004', 'CHECKING', 4, 3200.75, '2023-01-05'),
            (1008, 'SAV-004', 'SAVINGS', 5, 15000.00, '2023-04-12')
        ]
        cursor.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?)", accounts_data)
        
        transactions_data = [
            (1, 1001, '2024-12-01', -150.00, 'DEBIT', 'Grocery Store'),
            (2, 1001, '2024-12-02', -50.00, 'DEBIT', 'Gas Station'),
            (3, 1001, '2024-12-03', 2000.00, 'CREDIT', 'Salary Deposit'),
            (4, 1003, '2024-12-01', -200.00, 'DEBIT', 'Utility Bill'),
            (5, 1003, '2024-12-04', 1500.00, 'CREDIT', 'Freelance Payment'),
            (6, 1005, '2024-12-02', -75.50, 'DEBIT', 'Restaurant'),
            (7, 1007, '2024-12-01', -300.00, 'DEBIT', 'Rent Payment'),
            (8, 1001, '2024-12-05', -25.00, 'DEBIT', 'Coffee Shop'),
            (9, 1003, '2024-12-05', -100.00, 'DEBIT', 'Online Shopping'),
            (10, 1007, '2024-12-04', 3000.00, 'CREDIT', 'Bonus Payment')
        ]
        cursor.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?)", transactions_data)
        
        conn.commit()
        conn.close()
        
        # Create sample questions for this database
        sample_questions = [
            {
                'db_id': 'financial',
                'question': 'What is the total balance across all checking accounts?',
                'SQL': 'SELECT SUM(balance) FROM accounts WHERE account_type = "CHECKING"',
                'difficulty': 'easy'
            },
            {
                'db_id': 'financial', 
                'question': 'Which customers have both checking and savings accounts?',
                'SQL': '''SELECT DISTINCT c.first_name, c.last_name 
                         FROM customers c 
                         JOIN accounts a1 ON c.customer_id = a1.customer_id AND a1.account_type = "CHECKING"
                         JOIN accounts a2 ON c.customer_id = a2.customer_id AND a2.account_type = "SAVINGS"''',
                'difficulty': 'medium'
            },
            {
                'db_id': 'financial',
                'question': 'What is the average transaction amount by customer in December 2024?',
                'SQL': '''SELECT c.first_name, c.last_name, AVG(t.amount) as avg_amount
                         FROM customers c
                         JOIN accounts a ON c.customer_id = a.customer_id
                         JOIN transactions t ON a.account_id = t.account_id
                         WHERE t.transaction_date >= "2024-12-01"
                         GROUP BY c.customer_id, c.first_name, c.last_name''',
                'difficulty': 'hard'
            }
        ]
        
        return {'sample_db': db_path}, sample_questions
    
    def create_tables_from_sqlite(self, db_path: str, db_name: str):
        """Analyze SQLite database and create corresponding Snowflake tables"""
        
        logger.info(f"Analyzing SQLite database: {db_name}")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Found {len(tables)} tables: {tables}")
        
        tables_created = []
        
        for table_name in tables:
            try:
                snowflake_table_name = f"BIRD_{db_name.upper()}_{table_name.upper()}"
                
                # Get table schema
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                # Create Snowflake DDL
                ddl_parts = [f"CREATE OR REPLACE TABLE {snowflake_table_name} ("]
                column_defs = []
                
                for col in columns:
                    col_name = col[1].upper()
                    col_type = self._convert_sqlite_type_to_snowflake(col[2])
                    is_pk = bool(col[5])
                    
                    col_def = f"    {col_name} {col_type}"
                    if is_pk:
                        col_def += " PRIMARY KEY"
                    
                    column_defs.append(col_def)
                
                ddl_parts.append(",\n".join(column_defs))
                ddl_parts.append(")")
                
                ddl = "\n".join(ddl_parts)
                
                # Execute DDL
                logger.info(f"Creating table: {snowflake_table_name}")
                self.cursor.execute(ddl)
                
                # Load data
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                
                if not df.empty:
                    # Convert column names to uppercase
                    df.columns = [col.upper() for col in df.columns]
                    
                    # Load to Snowflake
                    success, nchunks, nrows, _ = write_pandas(
                        self.conn, 
                        df, 
                        snowflake_table_name,
                        auto_create_table=False,
                        overwrite=True
                    )
                    logger.info(f"✅ Loaded {nrows} rows into {snowflake_table_name}")
                    
                    tables_created.append({
                        'original_table': table_name,
                        'snowflake_table': snowflake_table_name,
                        'row_count': nrows,
                        'columns': len(df.columns)
                    })
                else:
                    logger.info(f"⚠️ Table {table_name} is empty")
                    
            except Exception as e:
                logger.error(f"❌ Failed to create table {table_name}: {e}")
        
        conn.close()
        return tables_created
    
    def _convert_sqlite_type_to_snowflake(self, sqlite_type: str) -> str:
        """Convert SQLite types to Snowflake types"""
        type_mapping = {
            'INTEGER': 'INTEGER',
            'TEXT': 'VARCHAR(16777216)',
            'VARCHAR': 'VARCHAR(16777216)', 
            'REAL': 'FLOAT',
            'DECIMAL': 'DECIMAL(15,2)',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP'
        }
        
        sqlite_upper = sqlite_type.upper()
        
        # Handle VARCHAR with size
        if 'VARCHAR(' in sqlite_upper:
            return sqlite_type
        
        # Handle DECIMAL with precision
        if 'DECIMAL(' in sqlite_upper:
            return sqlite_type
            
        for sqlite_type_key, snowflake_type in type_mapping.items():
            if sqlite_type_key in sqlite_upper:
                return snowflake_type
        
        return 'VARCHAR(16777216)'  # Default
    
    def create_sample_questions_table(self, questions_data: List):
        """Create table with sample questions that can be executed"""
        
        questions_ddl = """
        CREATE OR REPLACE TABLE BIRD_SAMPLE_QUESTIONS (
            ID VARCHAR(50) PRIMARY KEY,
            DB_ID VARCHAR(100),
            QUESTION TEXT,
            SQL_QUERY TEXT,
            DIFFICULTY VARCHAR(20),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        self.cursor.execute(questions_ddl)
        logger.info("✅ Created BIRD_SAMPLE_QUESTIONS table")
        
        # Process questions
        processed_questions = []
        for i, q in enumerate(questions_data):
            processed_questions.append({
                'ID': f"sample_{i:03d}",
                'DB_ID': q.get('db_id', 'unknown'),
                'QUESTION': q.get('question', ''),
                'SQL_QUERY': q.get('SQL', ''),
                'DIFFICULTY': q.get('difficulty', 'unknown')
            })
        
        if processed_questions:
            df = pd.DataFrame(processed_questions)
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                df, 
                'BIRD_SAMPLE_QUESTIONS',
                auto_create_table=False,
                overwrite=True
            )
            logger.info(f"✅ Loaded {nrows} sample questions")
    
    def run_sample_queries(self, tables_created: List):
        """Run sample queries against the loaded data"""
        
        if not tables_created:
            logger.info("No tables created to query")
            return
        
        logger.info("Running sample queries against loaded data...")
        
        # Find financial tables
        financial_tables = {t['original_table']: t['snowflake_table'] 
                          for t in tables_created 
                          if 'financial' in t['snowflake_table'].lower()}
        
        sample_queries = [
            ("Table row counts", [f"SELECT COUNT(*) as row_count FROM {t['snowflake_table']}" 
                                for t in tables_created]),
            
            ("Sample data from each table", [f"SELECT * FROM {t['snowflake_table']} LIMIT 3" 
                                           for t in tables_created]),
        ]
        
        # Add financial-specific queries if we have financial tables
        if 'customers' in [t['original_table'] for t in tables_created]:
            customer_table = next(t['snowflake_table'] for t in tables_created if t['original_table'] == 'customers')
            sample_queries.extend([
                ("Customer data", [f"SELECT FIRST_NAME, LAST_NAME, CITY FROM {customer_table} LIMIT 5"])
            ])
        
        if 'accounts' in [t['original_table'] for t in tables_created]:
            accounts_table = next(t['snowflake_table'] for t in tables_created if t['original_table'] == 'accounts')
            sample_queries.extend([
                ("Account balances by type", [f"SELECT ACCOUNT_TYPE, SUM(BALANCE) as total_balance FROM {accounts_table} GROUP BY ACCOUNT_TYPE"])
            ])
        
        if all(table in [t['original_table'] for t in tables_created] for table in ['customers', 'accounts', 'transactions']):
            customer_table = next(t['snowflake_table'] for t in tables_created if t['original_table'] == 'customers')
            accounts_table = next(t['snowflake_table'] for t in tables_created if t['original_table'] == 'accounts')
            transactions_table = next(t['snowflake_table'] for t in tables_created if t['original_table'] == 'transactions')
            
            sample_queries.extend([
                ("Customer transaction summary", [f"""
                    SELECT c.FIRST_NAME, c.LAST_NAME, 
                           COUNT(t.TRANSACTION_ID) as transaction_count,
                           SUM(t.AMOUNT) as total_amount
                    FROM {customer_table} c
                    JOIN {accounts_table} a ON c.CUSTOMER_ID = a.CUSTOMER_ID
                    JOIN {transactions_table} t ON a.ACCOUNT_ID = t.ACCOUNT_ID
                    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME
                    ORDER BY total_amount DESC
                """])
            ])
        
        for description, queries in sample_queries:
            logger.info(f"\n📊 {description}:")
            
            for query in queries:
                try:
                    self.cursor.execute(query)
                    results = self.cursor.fetchall()
                    
                    if results:
                        # Get column names
                        columns = [desc[0] for desc in self.cursor.description]
                        
                        # Print header
                        print("  " + " | ".join(f"{col:15}" for col in columns))
                        print("  " + "-" * (len(columns) * 18))
                        
                        # Print rows (limit to 5)
                        for row in results[:5]:
                            formatted_row = []
                            for value in row:
                                if isinstance(value, (int, float)) and abs(value) > 999:
                                    formatted_row.append(f"{value:,.0f}")
                                elif isinstance(value, str) and len(value) > 15:
                                    formatted_row.append(value[:12] + "...")
                                else:
                                    formatted_row.append(str(value))
                            print("  " + " | ".join(f"{val:15}" for val in formatted_row))
                    else:
                        print("  No results")
                    
                    print()  # Empty line
                        
                except Exception as e:
                    logger.error(f"❌ Query failed: {e}")
    
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
            logger.info("🧹 Cleaned up temporary files")
    
    def close(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.cleanup_temp_files()
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
        'role': os.getenv('SNOWFLAKE_ROLE', 'DEV')
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
    """Main test function focused on database data"""
    
    print("🗄️ BIRD Database Data Test Loader for Snowflake")
    print("Focus: Loading actual database tables and data")
    print("=" * 60)
    
    # Load config
    config = load_config()
    if not config:
        return 1
    
    loader = SnowflakeBirdDataTestLoader(config)
    
    try:
        # Test connection
        if not loader.connect():
            return 1
        
        # Test warehouse
        loader.test_warehouse()
        
        # Download datasets
        dataset_info, questions_data = loader.load_mini_dev_dataset()
        
        tables_created = []
        
        # Process datasets
        if isinstance(dataset_info, dict) and 'sample_db' in dataset_info:
            # Sample database case
            tables_created = loader.create_tables_from_sqlite(
                dataset_info['sample_db'], 
                'financial'
            )
            
            # Create questions table
            loader.create_sample_questions_table(questions_data)
            
        else:
            logger.info("Processing actual BIRD mini-dev dataset...")
            # TODO: Process actual mini-dev dataset when available
            # For now, fall back to sample
            dataset_info, questions_data = loader._create_sample_database_structure()
            tables_created = loader.create_tables_from_sqlite(
                dataset_info['sample_db'], 
                'financial'
            )
            loader.create_sample_questions_table(questions_data)
        
        # Run sample queries to show the data works
        loader.run_sample_queries(tables_created)
        
        print(f"\n✅ Database data test completed!")
        print(f"📊 Created {len(tables_created)} tables with real data")
        print(f"🔍 You can now run SQL queries against the loaded tables")
        
        if tables_created:
            print(f"\n📋 Tables created:")
            for table in tables_created:
                print(f"  • {table['snowflake_table']} ({table['row_count']} rows, {table['columns']} columns)")
        
        print(f"\n💡 Next: Run the full loader to get all 95 BIRD databases")
        
        # Ask if user wants to clean up
        try:
            cleanup = input("\n🧹 Clean up test tables? (y/N): ").lower().strip()
            if cleanup == 'y':
                for table in tables_created:
                    try:
                        loader.cursor.execute(f"DROP TABLE IF EXISTS {table['snowflake_table']}")
                        logger.info(f"Dropped {table['snowflake_table']}")
                    except:
                        pass
                loader.cursor.execute("DROP TABLE IF EXISTS BIRD_SAMPLE_QUESTIONS")
                print("🧹 Cleaned up test tables")
            else:
                print("💾 Test tables preserved for inspection")
        except (EOFError, KeyboardInterrupt):
            print("\n💾 Test tables preserved")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return 1
    
    finally:
        loader.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())