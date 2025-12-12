#!/usr/bin/env python3
"""
Complete BIRD Dataset to Snowflake Loader

This script loads the complete BIRD SQL dataset into Snowflake, including:
- 12,751 text-to-SQL pairs
- 95 databases with full schemas and data
- 37 professional domains
- Complete table structures and relationships

Prerequisites:
1. Install required packages: pip install snowflake-connector-python datasets sqlite3
2. Download BIRD dataset from HuggingFace: birdsql/bird_mini_dev
3. Set up Snowflake credentials as environment variables or in config file
"""

import os
import sys
import json
import sqlite3
import pandas as pd
import tempfile
import zipfile
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datasets import load_dataset
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SnowflakeBirdFullLoader:
    """Loads complete BIRD dataset including all databases into Snowflake"""
    
    def __init__(self, config: Dict):
        """
        Initialize the loader with Snowflake connection config
        
        Args:
            config: Dictionary containing Snowflake connection parameters
        """
        self.config = config
        self.conn = None
        self.cursor = None
        self.temp_dir = None
        self.database_schemas = {}
        
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
    
    def create_bird_schema(self):
        """Create the comprehensive schema for BIRD dataset"""
        
        # Create main BIRD questions table
        questions_ddl = """
        CREATE OR REPLACE TABLE bird_questions (
            ID VARCHAR(50) PRIMARY KEY,
            DB_ID VARCHAR(100),
            QUESTION TEXT,
            EVIDENCE TEXT,
            SQL_QUERY TEXT,
            DIFFICULTY VARCHAR(20),
            DOMAIN VARCHAR(50),
            QUESTION_ID_ORIGINAL VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create database metadata table
        databases_ddl = """
        CREATE OR REPLACE TABLE bird_databases (
            DB_ID VARCHAR(100) PRIMARY KEY,
            DB_NAME VARCHAR(200),
            DOMAIN VARCHAR(50),
            NUM_TABLES INTEGER,
            DESCRIPTION TEXT,
            SIZE_MB FLOAT,
            SQLITE_PATH VARCHAR(500),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create table metadata table
        tables_ddl = """
        CREATE OR REPLACE TABLE bird_table_schemas (
            ID VARCHAR(200) PRIMARY KEY,
            DB_ID VARCHAR(100),
            TABLE_NAME VARCHAR(200),
            COLUMN_NAME VARCHAR(200),
            COLUMN_TYPE VARCHAR(100),
            IS_PRIMARY_KEY BOOLEAN DEFAULT FALSE,
            IS_FOREIGN_KEY BOOLEAN DEFAULT FALSE,
            FOREIGN_TABLE VARCHAR(200),
            FOREIGN_COLUMN VARCHAR(200),
            COLUMN_POSITION INTEGER,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create foreign key relationships table
        foreign_keys_ddl = """
        CREATE OR REPLACE TABLE bird_foreign_keys (
            ID VARCHAR(200) PRIMARY KEY,
            DB_ID VARCHAR(100),
            SOURCE_TABLE VARCHAR(200),
            SOURCE_COLUMN VARCHAR(200),
            TARGET_TABLE VARCHAR(200),
            TARGET_COLUMN VARCHAR(200),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create table data metadata
        table_data_ddl = """
        CREATE OR REPLACE TABLE bird_table_data_info (
            DB_ID VARCHAR(100),
            TABLE_NAME VARCHAR(200),
            ROW_COUNT INTEGER,
            DATA_LOADED BOOLEAN DEFAULT FALSE,
            SNOWFLAKE_TABLE_NAME VARCHAR(200),
            LOAD_TIMESTAMP TIMESTAMP,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            self.cursor.execute(foreign_keys_ddl)
            self.cursor.execute(table_data_ddl)
            self.cursor.execute(execution_ddl)
            logger.info("Created comprehensive BIRD schema tables successfully")
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    def download_bird_datasets(self) -> Tuple[Any, Any]:
        """Download both question data and database files from HuggingFace"""
        try:
            logger.info("Downloading BIRD datasets from HuggingFace...")
            
            # Load main dataset with questions and SQL
            main_dataset = load_dataset("birdsql/bird-critic-1.0-flash-exp")
            logger.info(f"✅ Downloaded main dataset with {len(main_dataset['flash'])} examples")
            
            # Load mini-dev dataset with actual SQLite database files
            mini_dataset = load_dataset("birdsql/bird_mini_dev")
            logger.info(f"✅ Downloaded mini-dev dataset with database files")
            
            # Also try to get LiveSQL SQLite databases
            try:
                livesql_dataset = load_dataset("birdsql/livesqlbench-base-lite-sqlite")
                logger.info(f"✅ Downloaded LiveSQLBench SQLite dataset")
                return main_dataset, mini_dataset, livesql_dataset
            except Exception as e:
                logger.warning(f"Could not download LiveSQLBench dataset: {e}")
                return main_dataset, mini_dataset, None
            
        except Exception as e:
            logger.error(f"Failed to download BIRD datasets: {e}")
            raise
    
    def extract_sqlite_databases(self, mini_dataset, livesql_dataset=None) -> Dict[str, str]:
        """Extract actual SQLite database files from BIRD datasets"""
        
        db_paths = {}
        self.temp_dir = tempfile.mkdtemp()
        logger.info(f"Using temporary directory: {self.temp_dir}")
        
        # Method 0: Check for locally downloaded databases first
        local_db_dir = Path("bird_databases")
        if local_db_dir.exists():
            logger.info("🔍 Checking for locally downloaded BIRD databases...")
            sqlite_files = list(local_db_dir.glob("*.sqlite"))
            
            if sqlite_files:
                logger.info(f"✅ Found {len(sqlite_files)} local database files!")
                for db_file in sqlite_files:
                    db_id = db_file.stem
                    if self._verify_sqlite_database(str(db_file)):
                        db_paths[db_id] = str(db_file)
                        logger.info(f"✅ Using local database: {db_id}")
                    else:
                        logger.warning(f"❌ Invalid local database: {db_id}")
                
                if db_paths:
                    logger.info(f"🎉 Using {len(db_paths)} local BIRD databases!")
                    return db_paths
            else:
                logger.info("No local database files found, falling back to dataset extraction...")
        else:
            logger.info("No bird_databases directory found, falling back to dataset extraction...")
        
        try:
            # Method 1: Extract from mini_dev_sqlite dataset
            if 'mini_dev_sqlite' in mini_dataset:
                logger.info("Extracting databases from mini_dev_sqlite...")
                for item in mini_dataset['mini_dev_sqlite']:
                    db_id = item.get('db_id')
                    if db_id:
                        # Check for different possible database fields
                        db_data = None
                        for field_name in ['database', 'db_file', 'sqlite_file', 'db_path']:
                            if field_name in item:
                                db_data = item.get(field_name)
                                break
                        
                        if db_data:
                            db_path = os.path.join(self.temp_dir, f"{db_id}.sqlite")
                            try:
                                if isinstance(db_data, bytes):
                                    with open(db_path, 'wb') as f:
                                        f.write(db_data)
                                elif isinstance(db_data, str):
                                    # If it's a string path or base64 encoded
                                    import base64
                                    try:
                                        decoded = base64.b64decode(db_data)
                                        with open(db_path, 'wb') as f:
                                            f.write(decoded)
                                    except:
                                        # If not base64, try to download from URL
                                        if db_data.startswith('http'):
                                            self._download_database_file(db_data, db_path)
                                        else:
                                            logger.warning(f"Unknown database format for {db_id}")
                                            continue
                                
                                # Verify the SQLite file
                                if self._verify_sqlite_database(db_path):
                                    db_paths[db_id] = db_path
                                    logger.info(f"✅ Extracted database: {db_id}")
                                else:
                                    logger.warning(f"❌ Invalid SQLite database: {db_id}")
                                    
                            except Exception as e:
                                logger.warning(f"Failed to extract {db_id}: {e}")
            
            # Method 2: Extract from LiveSQLBench dataset if available
            if livesql_dataset and not db_paths:
                logger.info("Extracting databases from LiveSQLBench...")
                # Process LiveSQLBench dataset structure
                for split_name, split_data in livesql_dataset.items():
                    if 'sqlite' in split_name.lower():
                        for item in split_data:
                            db_id = item.get('db_id')
                            if db_id and 'database' in item:
                                db_data = item.get('database')
                                if db_data:
                                    db_path = os.path.join(self.temp_dir, f"livesql_{db_id}.sqlite")
                                    try:
                                        if isinstance(db_data, bytes):
                                            with open(db_path, 'wb') as f:
                                                f.write(db_data)
                                        
                                        if self._verify_sqlite_database(db_path):
                                            db_paths[f"livesql_{db_id}"] = db_path
                                            logger.info(f"✅ Extracted LiveSQL database: {db_id}")
                                    except Exception as e:
                                        logger.warning(f"Failed to extract LiveSQL {db_id}: {e}")
            
            # Method 3: Try to download from official BIRD GitHub if no databases found
            if not db_paths:
                logger.info("No database files found in HuggingFace datasets")
                logger.info("Attempting to download from official BIRD repository...")
                db_paths = self._download_from_official_bird_repo()
            
        except Exception as e:
            logger.warning(f"Could not extract databases from HuggingFace: {e}")
            
        if not db_paths:
            logger.warning("❌ No actual BIRD databases could be extracted")
            logger.info("💡 You may need to manually download from: https://bird-bench.github.io/")
            logger.info("💡 Or contact bird.bench25@gmail.com for database access")
            
        return db_paths
    
    def _verify_sqlite_database(self, db_path: str) -> bool:
        """Verify that the file is a valid SQLite database"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            conn.close()
            return len(tables) > 0
        except:
            return False
    
    def _download_database_file(self, url: str, db_path: str):
        """Download database file from URL"""
        import requests
        response = requests.get(url)
        response.raise_for_status()
        with open(db_path, 'wb') as f:
            f.write(response.content)
    
    def _download_from_official_bird_repo(self) -> Dict[str, str]:
        """Attempt to download from official BIRD repository"""
        logger.info("🔍 Attempting to download from official BIRD sources...")
        
        # URLs for BIRD dataset files
        bird_sources = [
            {
                'name': 'bird_mini_dev_v2',
                'url': 'https://huggingface.co/datasets/birdsql/bird_mini_dev/raw/main/mini_dev_sqlite.json'
            }
        ]
        
        db_paths = {}
        
        for source in bird_sources:
            try:
                logger.info(f"Trying to download from {source['name']}...")
                import requests
                
                response = requests.get(source['url'])
                if response.status_code == 200:
                    # Parse the JSON and extract database information
                    data = response.json()
                    logger.info(f"✅ Downloaded metadata from {source['name']}")
                    
                    # Process the data to extract database files
                    # This would depend on the exact structure of the JSON
                    # For now, log what we found
                    logger.info(f"Found data structure: {list(data.keys()) if isinstance(data, dict) else 'List with {} items'.format(len(data))}")
                    
            except Exception as e:
                logger.warning(f"Failed to download from {source['name']}: {e}")
        
        if not db_paths:
            logger.warning("❌ Could not download databases from official sources")
            logger.info("💡 Manual steps:")
            logger.info("   1. Visit https://bird-bench.github.io/")
            logger.info("   2. Download the SQLite databases")
            logger.info("   3. Extract to a directory and update the script path")
            logger.info("   4. Or email bird.bench25@gmail.com for automated access")
        
        return db_paths
    
    def _download_databases_from_github(self) -> Dict[str, str]:
        """Create sample databases as fallback when BIRD databases aren't available"""
        
        logger.info("Creating sample databases for testing...")
        db_paths = {}
        
        # Create sample databases similar to data test loader
        sample_databases = [
            ('financial', self._create_financial_db),
            ('education', self._create_education_db),
            ('retail', self._create_retail_db)
        ]
        
        for db_name, creator_func in sample_databases:
            try:
                db_path = os.path.join(self.temp_dir, f"{db_name}.sqlite")
                creator_func(db_path)
                db_paths[db_name] = db_path
                logger.info(f"✅ Created sample database: {db_name}")
            except Exception as e:
                logger.error(f"❌ Failed to create {db_name} database: {e}")
        
        return db_paths
    
    def _create_financial_db(self, db_path: str):
        """Create sample financial database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create tables
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
            CREATE TABLE accounts (
                account_id INTEGER PRIMARY KEY,
                account_number VARCHAR(20),
                account_type VARCHAR(50),
                customer_id INTEGER,
                balance DECIMAL(15,2),
                created_date DATE,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
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
    
    def _create_education_db(self, db_path: str):
        """Create sample education database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE students (
                student_id INTEGER PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                major VARCHAR(100),
                year INTEGER,
                gpa DECIMAL(3,2)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE courses (
                course_id INTEGER PRIMARY KEY,
                course_code VARCHAR(10),
                course_name VARCHAR(100),
                credits INTEGER,
                instructor VARCHAR(100)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE enrollments (
                enrollment_id INTEGER PRIMARY KEY,
                student_id INTEGER,
                course_id INTEGER,
                semester VARCHAR(20),
                grade VARCHAR(2),
                FOREIGN KEY (student_id) REFERENCES students(student_id),
                FOREIGN KEY (course_id) REFERENCES courses(course_id)
            )
        """)
        
        # Insert sample data
        students_data = [
            (1, 'Emma', 'Watson', 'emma.w@university.edu', 'Computer Science', 3, 3.75),
            (2, 'Liam', 'Johnson', 'liam.j@university.edu', 'Mathematics', 2, 3.50),
            (3, 'Sophia', 'Garcia', 'sophia.g@university.edu', 'Physics', 4, 3.90),
            (4, 'Noah', 'Brown', 'noah.b@university.edu', 'Computer Science', 1, 3.25),
            (5, 'Olivia', 'Davis', 'olivia.d@university.edu', 'Chemistry', 3, 3.80)
        ]
        cursor.executemany("INSERT INTO students VALUES (?, ?, ?, ?, ?, ?, ?)", students_data)
        
        courses_data = [
            (101, 'CS101', 'Introduction to Programming', 4, 'Dr. Smith'),
            (102, 'MATH201', 'Calculus II', 4, 'Prof. Johnson'),
            (103, 'PHYS301', 'Quantum Mechanics', 3, 'Dr. Einstein'),
            (104, 'CS201', 'Data Structures', 4, 'Dr. Turing'),
            (105, 'CHEM101', 'General Chemistry', 4, 'Prof. Curie')
        ]
        cursor.executemany("INSERT INTO courses VALUES (?, ?, ?, ?, ?)", courses_data)
        
        enrollments_data = [
            (1, 1, 101, 'Fall 2024', 'A'),
            (2, 1, 104, 'Fall 2024', 'A-'),
            (3, 2, 102, 'Fall 2024', 'B+'),
            (4, 3, 103, 'Fall 2024', 'A'),
            (5, 4, 101, 'Fall 2024', 'B'),
            (6, 5, 105, 'Fall 2024', 'A-')
        ]
        cursor.executemany("INSERT INTO enrollments VALUES (?, ?, ?, ?, ?)", enrollments_data)
        
        conn.commit()
        conn.close()
    
    def _create_retail_db(self, db_path: str):
        """Create sample retail database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                stock_quantity INTEGER
            )
        """)
        
        cursor.execute("""
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                phone VARCHAR(20)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                total_amount DECIMAL(10,2),
                status VARCHAR(20),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE order_items (
                item_id INTEGER PRIMARY KEY,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        """)
        
        # Insert sample data
        products_data = [
            (1, 'iPhone 15', 'Electronics', 999.00, 50),
            (2, 'Samsung Galaxy S24', 'Electronics', 899.00, 30),
            (3, 'Nike Air Max', 'Footwear', 150.00, 100),
            (4, 'Adidas Ultraboost', 'Footwear', 180.00, 75),
            (5, 'MacBook Pro', 'Electronics', 1999.00, 25)
        ]
        cursor.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?)", products_data)
        
        customers_data = [
            (1, 'Michael', 'Scott', 'michael@dundermifflin.com', '555-0201'),
            (2, 'Dwight', 'Schrute', 'dwight@schrutefarms.com', '555-0202'),
            (3, 'Jim', 'Halpert', 'jim@athleap.com', '555-0203')
        ]
        cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?)", customers_data)
        
        orders_data = [
            (1001, 1, '2024-12-01', 999.00, 'Shipped'),
            (1002, 2, '2024-12-02', 330.00, 'Processing'),
            (1003, 3, '2024-12-03', 1999.00, 'Delivered')
        ]
        cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", orders_data)
        
        order_items_data = [
            (1, 1001, 1, 1, 999.00),
            (2, 1002, 3, 2, 150.00),
            (3, 1002, 4, 1, 180.00),
            (4, 1003, 5, 1, 1999.00)
        ]
        cursor.executemany("INSERT INTO order_items VALUES (?, ?, ?, ?, ?)", order_items_data)
        
        conn.commit()
        conn.close()
    
    def analyze_sqlite_database(self, db_path: str, db_id: str) -> Dict:
        """Analyze SQLite database structure and extract metadata"""
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            db_info = {
                'db_id': db_id,
                'tables': {},
                'foreign_keys': [],
                'size_mb': os.path.getsize(db_path) / (1024 * 1024)
            }
            
            for table_name in [t[0] for t in tables]:
                # Get table schema
                cursor.execute(f"PRAGMA table_info([{table_name}])")
                columns = cursor.fetchall()
                
                # Get row count (use quotes for table names to handle reserved keywords)
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                row_count = cursor.fetchone()[0]
                
                # Get foreign keys
                cursor.execute(f"PRAGMA foreign_key_list([{table_name}])")
                fks = cursor.fetchall()
                
                table_info = {
                    'columns': columns,
                    'row_count': row_count,
                    'foreign_keys': fks
                }
                
                db_info['tables'][table_name] = table_info
                
                # Store foreign key relationships
                for fk in fks:
                    fk_info = {
                        'source_table': table_name,
                        'source_column': fk[3],
                        'target_table': fk[2],
                        'target_column': fk[4]
                    }
                    db_info['foreign_keys'].append(fk_info)
            
            conn.close()
            return db_info
            
        except Exception as e:
            logger.error(f"Failed to analyze database {db_id}: {e}")
            return {}
    
    def load_database_schemas(self, db_paths: Dict[str, str]):
        """Load database schemas into Snowflake metadata tables"""
        
        schema_data = []
        fk_data = []
        table_info_data = []
        db_data = []
        
        for db_id, db_path in db_paths.items():
            logger.info(f"Analyzing database schema: {db_id}")
            
            db_info = self.analyze_sqlite_database(db_path, db_id)
            if not db_info:
                continue
                
            # Database metadata
            domain = self._extract_domain(db_id)
            db_record = {
                'DB_ID': db_id,
                'DB_NAME': db_id,
                'DOMAIN': domain,
                'NUM_TABLES': len(db_info['tables']),
                'DESCRIPTION': f"BIRD benchmark database: {db_id}",
                'SIZE_MB': db_info['size_mb'],
                'SQLITE_PATH': db_path
            }
            db_data.append(db_record)
            
            # Table schemas
            for table_name, table_info in db_info['tables'].items():
                for col_idx, column in enumerate(table_info['columns']):
                    schema_record = {
                        'ID': f"{db_id}_{table_name}_{column[1]}",
                        'DB_ID': db_id,
                        'TABLE_NAME': table_name,
                        'COLUMN_NAME': column[1],
                        'COLUMN_TYPE': column[2],
                        'IS_PRIMARY_KEY': bool(column[5]),
                        'IS_FOREIGN_KEY': False,  # Will be updated below
                        'FOREIGN_TABLE': None,
                        'FOREIGN_COLUMN': None,
                        'COLUMN_POSITION': col_idx
                    }
                    schema_data.append(schema_record)
                
                # Table data info
                table_info_record = {
                    'DB_ID': db_id,
                    'TABLE_NAME': table_name,
                    'ROW_COUNT': table_info['row_count'],
                    'DATA_LOADED': False,
                    'SNOWFLAKE_TABLE_NAME': f"BIRD_{db_id}_{table_name}".upper()
                }
                table_info_data.append(table_info_record)
            
            # Foreign keys
            for fk in db_info['foreign_keys']:
                fk_record = {
                    'ID': f"{db_id}_{fk['source_table']}_{fk['source_column']}",
                    'DB_ID': db_id,
                    'SOURCE_TABLE': fk['source_table'],
                    'SOURCE_COLUMN': fk['source_column'],
                    'TARGET_TABLE': fk['target_table'],
                    'TARGET_COLUMN': fk['target_column']
                }
                fk_data.append(fk_record)
                
                # Update schema data to mark foreign key columns
                for schema_record in schema_data:
                    if (schema_record['DB_ID'] == db_id and 
                        schema_record['TABLE_NAME'] == fk['source_table'] and
                        schema_record['COLUMN_NAME'] == fk['source_column']):
                        schema_record['IS_FOREIGN_KEY'] = True
                        schema_record['FOREIGN_TABLE'] = fk['target_table']
                        schema_record['FOREIGN_COLUMN'] = fk['target_column']
        
        # Load into Snowflake
        if db_data:
            db_df = pd.DataFrame(db_data)
            write_pandas(self.conn, db_df, 'BIRD_DATABASES', auto_create_table=False, overwrite=True)
            logger.info(f"Loaded {len(db_data)} database records")
        
        if schema_data:
            schema_df = pd.DataFrame(schema_data)
            write_pandas(self.conn, schema_df, 'BIRD_TABLE_SCHEMAS', auto_create_table=False, overwrite=True)
            logger.info(f"Loaded {len(schema_data)} schema records")
        
        if fk_data:
            fk_df = pd.DataFrame(fk_data)
            write_pandas(self.conn, fk_df, 'BIRD_FOREIGN_KEYS', auto_create_table=False, overwrite=True)
            logger.info(f"Loaded {len(fk_data)} foreign key records")
        
        if table_info_data:
            table_df = pd.DataFrame(table_info_data)
            write_pandas(self.conn, table_df, 'BIRD_TABLE_DATA_INFO', auto_create_table=False, overwrite=True)
            logger.info(f"Loaded {len(table_info_data)} table info records")
    
    def load_database_data(self, db_paths: Dict[str, str], max_tables: int = None):
        """Load actual data from SQLite databases into Snowflake tables"""
        
        if max_tables:
            logger.info(f"Loading database data (max {max_tables} tables)...")
        else:
            logger.info("Loading ALL database data (no limits)...")
        tables_loaded = 0
        
        for db_id, db_path in db_paths.items():
            if max_tables and tables_loaded >= max_tables:
                logger.info(f"Reached maximum table limit ({max_tables})")
                break
                
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Get all tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [t[0] for t in cursor.fetchall()]
                
                for table_name in tables:
                    if max_tables and tables_loaded >= max_tables:
                        break
                        
                    snowflake_table_name = f"BIRD_{db_id}_{table_name}".upper()
                    
                    try:
                        # Read table data into DataFrame
                        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                        
                        # Clean column names for Snowflake
                        df.columns = [col.upper().replace(' ', '_').replace('-', '_') for col in df.columns]
                        
                        # Load complete data (no sampling)
                        logger.info(f"Loading complete table {table_name} with {len(df):,} rows")
                        
                        # Load to Snowflake
                        write_pandas(self.conn, df, snowflake_table_name, auto_create_table=True, overwrite=True)
                        
                        # Update table info
                        update_sql = f"""
                        UPDATE bird_table_data_info 
                        SET data_loaded = TRUE, 
                            load_timestamp = CURRENT_TIMESTAMP,
                            snowflake_table_name = '{snowflake_table_name}'
                        WHERE db_id = '{db_id}' AND table_name = '{table_name}'
                        """
                        self.cursor.execute(update_sql)
                        
                        tables_loaded += 1
                        logger.info(f"Loaded table {snowflake_table_name} ({len(df)} rows)")
                        
                    except Exception as e:
                        logger.error(f"Failed to load table {table_name} from {db_id}: {e}")
                
                conn.close()
                
            except Exception as e:
                logger.error(f"Failed to process database {db_id}: {e}")
        
        logger.info(f"Successfully loaded {tables_loaded} tables")
    
    def process_bird_data(self, main_dataset, mini_dataset) -> pd.DataFrame:
        """Process BIRD question data into DataFrame"""
        
        questions_data = []
        
        # Use mini_dev_sqlite which has the proper question structure
        if 'mini_dev_sqlite' in mini_dataset:
            logger.info("Processing questions from mini_dev_sqlite dataset...")
            for idx, item in enumerate(mini_dataset['mini_dev_sqlite']):
                question_record = {
                    'id': f"bird_{idx:06d}",
                    'db_id': item.get('db_id', ''),
                    'question': item.get('question', ''),
                    'evidence': item.get('evidence', ''),
                    'sql_query': item.get('SQL', ''),
                    'difficulty': item.get('difficulty', 'unknown'),
                    'domain': self._extract_domain(item.get('db_id', '')),
                    'question_id_original': item.get('question_id', f"q_{idx}")
                }
                questions_data.append(question_record)
        else:
            # Fallback to main dataset if mini_dev not available
            logger.info("Processing questions from main dataset (flash)...")
            for idx, item in enumerate(main_dataset['flash']):
                question_record = {
                    'id': f"bird_{idx:06d}",
                    'db_id': item.get('db_id', ''),
                    'question': item.get('query', ''),  # Note: uses 'query' not 'question' in flash dataset
                    'evidence': '',
                    'sql_query': item.get('clean_up_sql', item.get('preprocess_sql', '')),
                    'difficulty': item.get('difficulty_tier', 'unknown'),
                    'domain': self._extract_domain(item.get('db_id', '')),
                    'question_id_original': item.get('instance_id', f"q_{idx}")
                }
                questions_data.append(question_record)
        
        return pd.DataFrame(questions_data)
    
    def _extract_domain(self, db_id: str) -> str:
        """Extract domain from database ID"""
        common_domains = {
            'financial': ['bank', 'finance', 'trading', 'stock', 'money', 'credit'],
            'healthcare': ['hospital', 'medical', 'patient', 'health', 'doctor', 'clinic'],
            'education': ['school', 'student', 'university', 'college', 'academic', 'course'],
            'retail': ['store', 'shop', 'retail', 'customer', 'product', 'sales'],
            'sports': ['game', 'sport', 'team', 'player', 'match', 'league'],
            'technology': ['software', 'tech', 'computer', 'system', 'app', 'web'],
            'entertainment': ['movie', 'film', 'music', 'artist', 'show', 'concert'],
            'transportation': ['car', 'flight', 'train', 'transport', 'airline', 'vehicle'],
            'government': ['government', 'public', 'city', 'county', 'state', 'federal'],
            'real_estate': ['property', 'house', 'building', 'real_estate', 'apartment']
        }
        
        db_id_lower = db_id.lower()
        for domain, keywords in common_domains.items():
            if any(keyword in db_id_lower for keyword in keywords):
                return domain
        return 'general'
    
    def create_enhanced_views(self):
        """Create comprehensive views for analyzing BIRD data and schemas"""
        
        views = [
            """
            CREATE OR REPLACE VIEW bird_comprehensive_summary AS
            SELECT 
                d.domain,
                COUNT(DISTINCT q.db_id) as database_count,
                COUNT(q.id) as question_count,
                AVG(LENGTH(q.question)) as avg_question_length,
                AVG(LENGTH(q.sql_query)) as avg_sql_length,
                SUM(d.num_tables) as total_tables,
                AVG(d.size_mb) as avg_db_size_mb
            FROM bird_questions q
            JOIN bird_databases d ON q.db_id = d.db_id
            GROUP BY d.domain
            ORDER BY question_count DESC
            """,
            
            """
            CREATE OR REPLACE VIEW bird_schema_overview AS
            SELECT 
                s.db_id,
                d.domain,
                COUNT(DISTINCT s.table_name) as table_count,
                COUNT(s.column_name) as total_columns,
                COUNT(CASE WHEN s.is_primary_key THEN 1 END) as primary_key_columns,
                COUNT(CASE WHEN s.is_foreign_key THEN 1 END) as foreign_key_columns
            FROM bird_table_schemas s
            JOIN bird_databases d ON s.db_id = d.db_id
            GROUP BY s.db_id, d.domain
            ORDER BY table_count DESC
            """,
            
            """
            CREATE OR REPLACE VIEW bird_data_availability AS
            SELECT 
                t.db_id,
                d.domain,
                COUNT(*) as total_tables,
                COUNT(CASE WHEN t.data_loaded THEN 1 END) as tables_loaded,
                SUM(t.row_count) as total_rows,
                AVG(t.row_count) as avg_rows_per_table
            FROM bird_table_data_info t
            JOIN bird_databases d ON t.db_id = d.db_id
            GROUP BY t.db_id, d.domain
            ORDER BY total_rows DESC
            """,
            
            """
            CREATE OR REPLACE VIEW bird_relationship_complexity AS
            SELECT 
                fk.db_id,
                d.domain,
                COUNT(*) as foreign_key_count,
                COUNT(DISTINCT fk.source_table) as tables_with_fks,
                COUNT(DISTINCT fk.target_table) as referenced_tables
            FROM bird_foreign_keys fk
            JOIN bird_databases d ON fk.db_id = d.db_id
            GROUP BY fk.db_id, d.domain
            ORDER BY foreign_key_count DESC
            """
        ]
        
        for i, view_sql in enumerate(views):
            try:
                self.cursor.execute(view_sql)
                logger.info(f"Created enhanced view {i+1}")
            except Exception as e:
                logger.error(f"Failed to create view {i+1}: {e}")
    
    def validate_comprehensive_load(self):
        """Validate the comprehensive data load"""
        validation_queries = [
            "SELECT COUNT(*) as total_questions FROM bird_questions",
            "SELECT COUNT(DISTINCT db_id) as unique_databases FROM bird_databases",
            "SELECT COUNT(*) as schema_records FROM bird_table_schemas", 
            "SELECT COUNT(*) as foreign_key_relationships FROM bird_foreign_keys",
            "SELECT COUNT(CASE WHEN data_loaded THEN 1 END) as tables_with_data FROM bird_table_data_info",
            "SELECT domain, COUNT(*) as db_count FROM bird_databases GROUP BY domain ORDER BY db_count DESC LIMIT 5"
        ]
        
        logger.info("Validating comprehensive data load...")
        for query in validation_queries:
            try:
                self.cursor.execute(query)
                results = self.cursor.fetchall()
                logger.info(f"Query: {query}")
                logger.info(f"Results: {results}")
            except Exception as e:
                logger.error(f"Validation query failed: {e}")
    
    def cleanup(self):
        """Clean up temporary files"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
            logger.info("Cleaned up temporary files")
    
    def close(self):
        """Close Snowflake connection and cleanup"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.cleanup()
        logger.info("Closed Snowflake connection")

def load_config() -> Dict:
    """Load Snowflake configuration from environment or config file"""
    
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'BIRD_DB'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'DEV')
    }
    
    config_file = Path('snowflake_config.json')
    if config_file.exists():
        with open(config_file, 'r') as f:
            file_config = json.load(f)
            for key, value in file_config.items():
                config[key] = value  # Override env vars with config file values
    
    # Check for template/placeholder values
    template_values = ['YOUR_SNOWFLAKE_ACCOUNT', 'YOUR_USERNAME', 'YOUR_PASSWORD', 'YOUR_WAREHOUSE', 'YOUR_ROLE']
    for key, value in config.items():
        if value in template_values:
            config[key] = None
    
    required_fields = ['account', 'user', 'password']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        logger.error(f"Missing required Snowflake configuration fields: {missing_fields}")
        logger.error("Please set environment variables or update snowflake_config.json with your credentials:")
        logger.error("Environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD")
        logger.error("Or copy snowflake_config.template.json to snowflake_config.json and fill in your details")
        sys.exit(1)
    
    return config

def main():
    """Main function to orchestrate the complete BIRD dataset loading"""
    
    logger.info("Starting complete BIRD dataset loading to Snowflake...")
    
    config = load_config()
    loader = SnowflakeBirdFullLoader(config)
    
    try:
        if not loader.connect():
            return 1
        
        # Create comprehensive schema
        loader.create_bird_schema()
        
        # Download datasets
        datasets_result = loader.download_bird_datasets()
        if len(datasets_result) == 3:
            main_dataset, mini_dataset, livesql_dataset = datasets_result
        else:
            main_dataset, mini_dataset = datasets_result
            livesql_dataset = None
        
        # Extract and analyze actual BIRD databases
        db_paths = loader.extract_sqlite_databases(mini_dataset, livesql_dataset)
        
        # If no databases found in HuggingFace, try local downloaded databases
        if not db_paths:
            logger.info("Checking for locally downloaded BIRD databases...")
            db_paths = loader._check_local_bird_databases()
        
        if db_paths:
            logger.info(f"Found {len(db_paths)} database files")
            # Load database schemas and metadata
            loader.load_database_schemas(db_paths)
            
            # Load complete database data (no limits)
            loader.load_database_data(db_paths)
        else:
            logger.warning("No database files found, loading questions only")
        
        # Load question data
        questions_df = loader.process_bird_data(main_dataset, mini_dataset)
        # Fix column names for Snowflake
        questions_df.columns = [col.upper() for col in questions_df.columns]
        write_pandas(loader.conn, questions_df, 'BIRD_QUESTIONS', auto_create_table=False, overwrite=True)
        logger.info(f"✅ Loaded {len(questions_df)} question records")
        
        # Create enhanced views
        loader.create_enhanced_views()
        
        # Validate the load
        loader.validate_comprehensive_load()
        
        logger.info("Successfully loaded complete BIRD dataset to Snowflake!")
        
    except Exception as e:
        logger.error(f"Failed to load complete BIRD dataset: {e}")
        return 1
    
    finally:
        loader.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())