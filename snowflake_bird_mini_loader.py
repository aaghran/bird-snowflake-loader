#!/usr/bin/env python3
"""
BIRD Mini Loader for Snowflake

This script loads a small subset of the BIRD dataset for testing:
- Sample questions from BIRD dataset
- 3 sample databases (financial, education, retail) with actual table data
- Demonstrates the full pipeline with manageable data size

Use this to validate everything works before running the full loader.
"""

import os
import sys
import json
import sqlite3
import pandas as pd
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

class SnowflakeBirdMiniLoader:
    """Mini loader for BIRD dataset - creates actual database tables with sample data"""
    
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
    
    def create_comprehensive_schema(self):
        """Create comprehensive schema for BIRD data"""
        
        # Questions table
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
        
        # Database metadata
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
        
        # Table metadata
        tables_ddl = """
        CREATE OR REPLACE TABLE bird_table_schemas (
            id VARCHAR(50) PRIMARY KEY,
            db_id VARCHAR(100),
            table_name VARCHAR(200),
            column_name VARCHAR(200),
            column_type VARCHAR(100),
            is_primary_key BOOLEAN DEFAULT FALSE,
            is_foreign_key BOOLEAN DEFAULT FALSE,
            foreign_table VARCHAR(200),
            foreign_column VARCHAR(200),
            column_position INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        try:
            logger.info("Creating comprehensive BIRD schema...")
            self.cursor.execute(questions_ddl)
            logger.info("✅ Created bird_questions table")
            
            self.cursor.execute(databases_ddl)
            logger.info("✅ Created bird_databases table")
            
            self.cursor.execute(tables_ddl)
            logger.info("✅ Created bird_table_schemas table")
            
        except Exception as e:
            logger.error(f"❌ Failed to create schema: {e}")
            raise
    
    def create_sample_databases(self):
        """Create sample databases with realistic data"""
        
        self.temp_dir = tempfile.mkdtemp()
        logger.info(f"Creating sample databases in: {self.temp_dir}")
        
        databases_created = []
        
        # Financial database
        financial_db = os.path.join(self.temp_dir, "financial.sqlite")
        self._create_financial_database(financial_db)
        databases_created.append(('financial', financial_db, 'financial'))
        
        # Education database
        education_db = os.path.join(self.temp_dir, "education.sqlite")
        self._create_education_database(education_db)
        databases_created.append(('education', education_db, 'education'))
        
        # Retail database
        retail_db = os.path.join(self.temp_dir, "retail.sqlite")
        self._create_retail_database(retail_db)
        databases_created.append(('retail', retail_db, 'retail'))
        
        logger.info(f"✅ Created {len(databases_created)} sample databases")
        return databases_created
    
    def _create_financial_database(self, db_path: str):
        """Create comprehensive financial database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Customers table
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
                zip_code VARCHAR(10),
                credit_score INTEGER,
                created_date DATE
            )
        """)
        
        # Accounts table
        cursor.execute("""
            CREATE TABLE accounts (
                account_id INTEGER PRIMARY KEY,
                account_number VARCHAR(20) UNIQUE,
                account_type VARCHAR(50),
                customer_id INTEGER,
                balance DECIMAL(15,2),
                interest_rate DECIMAL(5,4),
                opened_date DATE,
                status VARCHAR(20),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """)
        
        # Transactions table
        cursor.execute("""
            CREATE TABLE transactions (
                transaction_id INTEGER PRIMARY KEY,
                account_id INTEGER,
                transaction_date DATE,
                amount DECIMAL(15,2),
                transaction_type VARCHAR(20),
                description VARCHAR(200),
                merchant VARCHAR(100),
                category VARCHAR(50),
                FOREIGN KEY (account_id) REFERENCES accounts(account_id)
            )
        """)
        
        # Loans table
        cursor.execute("""
            CREATE TABLE loans (
                loan_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                loan_type VARCHAR(50),
                principal_amount DECIMAL(15,2),
                interest_rate DECIMAL(5,4),
                term_months INTEGER,
                monthly_payment DECIMAL(10,2),
                outstanding_balance DECIMAL(15,2),
                loan_date DATE,
                status VARCHAR(20),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """)
        
        # Insert comprehensive sample data
        customers_data = [
            (1, 'John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'Boston', 'MA', '02101', 750, '2022-01-15'),
            (2, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Boston', 'MA', '02102', 720, '2022-02-20'),
            (3, 'Bob', 'Johnson', 'bob.j@email.com', '555-0103', '789 Pine Rd', 'Cambridge', 'MA', '02139', 680, '2022-03-10'),
            (4, 'Alice', 'Brown', 'alice.b@email.com', '555-0104', '321 Elm St', 'Boston', 'MA', '02101', 800, '2022-01-05'),
            (5, 'Charlie', 'Wilson', 'charlie.w@email.com', '555-0105', '654 Maple Dr', 'Somerville', 'MA', '02144', 650, '2022-04-12'),
            (6, 'Diana', 'Davis', 'diana.d@email.com', '555-0106', '987 Cedar Ln', 'Cambridge', 'MA', '02138', 780, '2022-05-18'),
            (7, 'Edward', 'Miller', 'edward.m@email.com', '555-0107', '147 Birch St', 'Boston', 'MA', '02103', 710, '2022-06-22'),
            (8, 'Fiona', 'Garcia', 'fiona.g@email.com', '555-0108', '258 Spruce Ave', 'Somerville', 'MA', '02143', 690, '2022-07-30')
        ]
        cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", customers_data)
        
        accounts_data = [
            (1001, 'CHK-001', 'CHECKING', 1, 2500.00, 0.0025, '2022-01-15', 'ACTIVE'),
            (1002, 'SAV-001', 'SAVINGS', 1, 10000.00, 0.0200, '2022-01-15', 'ACTIVE'),
            (1003, 'CHK-002', 'CHECKING', 2, 1800.50, 0.0025, '2022-02-20', 'ACTIVE'),
            (1004, 'SAV-002', 'SAVINGS', 2, 25000.00, 0.0250, '2022-02-20', 'ACTIVE'),
            (1005, 'CHK-003', 'CHECKING', 3, 750.25, 0.0025, '2022-03-10', 'ACTIVE'),
            (1006, 'SAV-003', 'SAVINGS', 3, 5500.00, 0.0150, '2022-03-10', 'ACTIVE'),
            (1007, 'CHK-004', 'CHECKING', 4, 3200.75, 0.0025, '2022-01-05', 'ACTIVE'),
            (1008, 'SAV-004', 'SAVINGS', 5, 15000.00, 0.0225, '2022-04-12', 'ACTIVE'),
            (1009, 'CHK-005', 'CHECKING', 6, 4500.00, 0.0025, '2022-05-18', 'ACTIVE'),
            (1010, 'SAV-005', 'SAVINGS', 7, 8750.00, 0.0175, '2022-06-22', 'ACTIVE')
        ]
        cursor.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", accounts_data)
        
        transactions_data = [
            (1, 1001, '2024-12-01', -150.00, 'DEBIT', 'Grocery Store Purchase', 'Whole Foods', 'Groceries'),
            (2, 1001, '2024-12-02', -50.00, 'DEBIT', 'Gas Station', 'Shell', 'Gas'),
            (3, 1001, '2024-12-03', 2000.00, 'CREDIT', 'Salary Deposit', 'Acme Corp', 'Salary'),
            (4, 1003, '2024-12-01', -200.00, 'DEBIT', 'Electric Bill', 'Boston Edison', 'Utilities'),
            (5, 1003, '2024-12-04', 1500.00, 'CREDIT', 'Freelance Payment', 'Design Co', 'Income'),
            (6, 1005, '2024-12-02', -75.50, 'DEBIT', 'Restaurant', 'Local Bistro', 'Dining'),
            (7, 1007, '2024-12-01', -300.00, 'DEBIT', 'Rent Payment', 'Property Mgmt', 'Housing'),
            (8, 1001, '2024-12-05', -25.00, 'DEBIT', 'Coffee Shop', 'Starbucks', 'Dining'),
            (9, 1003, '2024-12-05', -100.00, 'DEBIT', 'Online Shopping', 'Amazon', 'Shopping'),
            (10, 1007, '2024-12-04', 3000.00, 'CREDIT', 'Bonus Payment', 'Tech Corp', 'Bonus'),
            (11, 1009, '2024-12-03', -45.00, 'DEBIT', 'Pharmacy', 'CVS', 'Healthcare'),
            (12, 1009, '2024-12-06', 2200.00, 'CREDIT', 'Consulting Fee', 'Consultant Inc', 'Income')
        ]
        cursor.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?)", transactions_data)
        
        loans_data = [
            (2001, 1, 'MORTGAGE', 250000.00, 0.0425, 360, 1231.43, 245000.00, '2022-01-20', 'ACTIVE'),
            (2002, 2, 'AUTO', 25000.00, 0.0575, 60, 479.15, 22500.00, '2022-03-15', 'ACTIVE'),
            (2003, 4, 'PERSONAL', 15000.00, 0.0850, 48, 372.86, 12800.00, '2022-06-01', 'ACTIVE'),
            (2004, 6, 'AUTO', 32000.00, 0.0625, 72, 515.30, 28900.00, '2022-08-10', 'ACTIVE')
        ]
        cursor.executemany("INSERT INTO loans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", loans_data)
        
        conn.commit()
        conn.close()
    
    def _create_education_database(self, db_path: str):
        """Create comprehensive education database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Students table
        cursor.execute("""
            CREATE TABLE students (
                student_id INTEGER PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                major VARCHAR(100),
                year INTEGER,
                gpa DECIMAL(3,2),
                enrollment_date DATE,
                graduation_date DATE,
                status VARCHAR(20)
            )
        """)
        
        # Courses table
        cursor.execute("""
            CREATE TABLE courses (
                course_id INTEGER PRIMARY KEY,
                course_code VARCHAR(10),
                course_name VARCHAR(100),
                credits INTEGER,
                instructor VARCHAR(100),
                department VARCHAR(50),
                max_enrollment INTEGER
            )
        """)
        
        # Enrollments table
        cursor.execute("""
            CREATE TABLE enrollments (
                enrollment_id INTEGER PRIMARY KEY,
                student_id INTEGER,
                course_id INTEGER,
                semester VARCHAR(20),
                grade VARCHAR(2),
                grade_points DECIMAL(3,1),
                FOREIGN KEY (student_id) REFERENCES students(student_id),
                FOREIGN KEY (course_id) REFERENCES courses(course_id)
            )
        """)
        
        # Professors table
        cursor.execute("""
            CREATE TABLE professors (
                professor_id INTEGER PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                department VARCHAR(50),
                title VARCHAR(50),
                hire_date DATE
            )
        """)
        
        # Insert sample data
        students_data = [
            (1, 'Emma', 'Watson', 'emma.w@university.edu', 'Computer Science', 3, 3.75, '2022-09-01', None, 'ACTIVE'),
            (2, 'Liam', 'Johnson', 'liam.j@university.edu', 'Mathematics', 2, 3.50, '2023-09-01', None, 'ACTIVE'),
            (3, 'Sophia', 'Garcia', 'sophia.g@university.edu', 'Physics', 4, 3.90, '2021-09-01', '2025-05-15', 'GRADUATING'),
            (4, 'Noah', 'Brown', 'noah.b@university.edu', 'Computer Science', 1, 3.25, '2024-09-01', None, 'ACTIVE'),
            (5, 'Olivia', 'Davis', 'olivia.d@university.edu', 'Chemistry', 3, 3.80, '2022-09-01', None, 'ACTIVE'),
            (6, 'William', 'Miller', 'william.m@university.edu', 'Engineering', 2, 3.65, '2023-09-01', None, 'ACTIVE')
        ]
        cursor.executemany("INSERT INTO students VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", students_data)
        
        professors_data = [
            (1, 'Robert', 'Smith', 'r.smith@university.edu', 'Computer Science', 'Professor', '2015-08-15'),
            (2, 'Mary', 'Johnson', 'm.johnson@university.edu', 'Mathematics', 'Associate Professor', '2018-08-20'),
            (3, 'Albert', 'Einstein', 'a.einstein@university.edu', 'Physics', 'Distinguished Professor', '2010-09-01'),
            (4, 'Alan', 'Turing', 'a.turing@university.edu', 'Computer Science', 'Professor', '2012-08-15'),
            (5, 'Marie', 'Curie', 'm.curie@university.edu', 'Chemistry', 'Professor', '2016-08-20')
        ]
        cursor.executemany("INSERT INTO professors VALUES (?, ?, ?, ?, ?, ?, ?)", professors_data)
        
        courses_data = [
            (101, 'CS101', 'Introduction to Programming', 4, 'Dr. Smith', 'Computer Science', 30),
            (102, 'MATH201', 'Calculus II', 4, 'Prof. Johnson', 'Mathematics', 35),
            (103, 'PHYS301', 'Quantum Mechanics', 3, 'Dr. Einstein', 'Physics', 20),
            (104, 'CS201', 'Data Structures', 4, 'Dr. Turing', 'Computer Science', 25),
            (105, 'CHEM101', 'General Chemistry', 4, 'Prof. Curie', 'Chemistry', 40),
            (106, 'CS301', 'Algorithms', 3, 'Dr. Smith', 'Computer Science', 20),
            (107, 'MATH301', 'Linear Algebra', 3, 'Prof. Johnson', 'Mathematics', 30)
        ]
        cursor.executemany("INSERT INTO courses VALUES (?, ?, ?, ?, ?, ?, ?)", courses_data)
        
        enrollments_data = [
            (1, 1, 101, 'Fall 2024', 'A', 4.0),
            (2, 1, 104, 'Fall 2024', 'A-', 3.7),
            (3, 2, 102, 'Fall 2024', 'B+', 3.3),
            (4, 3, 103, 'Fall 2024', 'A', 4.0),
            (5, 4, 101, 'Fall 2024', 'B', 3.0),
            (6, 5, 105, 'Fall 2024', 'A-', 3.7),
            (7, 1, 106, 'Spring 2024', 'A', 4.0),
            (8, 2, 107, 'Spring 2024', 'B+', 3.3),
            (9, 6, 101, 'Fall 2024', 'A-', 3.7)
        ]
        cursor.executemany("INSERT INTO enrollments VALUES (?, ?, ?, ?, ?, ?)", enrollments_data)
        
        conn.commit()
        conn.close()
    
    def _create_retail_database(self, db_path: str):
        """Create comprehensive retail database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Products table
        cursor.execute("""
            CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                cost DECIMAL(10,2),
                stock_quantity INTEGER,
                supplier_id INTEGER,
                created_date DATE
            )
        """)
        
        # Customers table
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
                zip_code VARCHAR(10),
                loyalty_points INTEGER,
                created_date DATE
            )
        """)
        
        # Orders table
        cursor.execute("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                total_amount DECIMAL(10,2),
                tax_amount DECIMAL(8,2),
                shipping_cost DECIMAL(8,2),
                status VARCHAR(20),
                shipping_address VARCHAR(300),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """)
        
        # Order items table
        cursor.execute("""
            CREATE TABLE order_items (
                item_id INTEGER PRIMARY KEY,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                total_price DECIMAL(10,2),
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        """)
        
        # Suppliers table
        cursor.execute("""
            CREATE TABLE suppliers (
                supplier_id INTEGER PRIMARY KEY,
                supplier_name VARCHAR(100),
                contact_email VARCHAR(100),
                phone VARCHAR(20),
                address VARCHAR(300)
            )
        """)
        
        # Insert sample data
        suppliers_data = [
            (1, 'Apple Inc', 'orders@apple.com', '800-275-2273', '1 Apple Park Way, Cupertino, CA'),
            (2, 'Samsung Electronics', 'orders@samsung.com', '800-726-7864', '129 Samsung St, Seoul, South Korea'),
            (3, 'Nike Inc', 'orders@nike.com', '800-344-6453', '1 Bowerman Dr, Beaverton, OR'),
            (4, 'Adidas AG', 'orders@adidas.com', '800-982-9337', 'Adi-Dassler-Strasse 1, Herzogenaurach, Germany')
        ]
        cursor.executemany("INSERT INTO suppliers VALUES (?, ?, ?, ?, ?)", suppliers_data)
        
        products_data = [
            (1, 'iPhone 15 Pro', 'Electronics', 999.00, 750.00, 50, 1, '2024-01-15'),
            (2, 'Samsung Galaxy S24', 'Electronics', 899.00, 650.00, 30, 2, '2024-01-20'),
            (3, 'Nike Air Max 270', 'Footwear', 150.00, 85.00, 100, 3, '2024-02-01'),
            (4, 'Adidas Ultraboost 22', 'Footwear', 180.00, 95.00, 75, 4, '2024-02-05'),
            (5, 'MacBook Pro 16"', 'Electronics', 2399.00, 1800.00, 25, 1, '2024-01-25'),
            (6, 'AirPods Pro', 'Electronics', 249.00, 150.00, 200, 1, '2024-03-01'),
            (7, 'Nike React Infinity', 'Footwear', 160.00, 88.00, 80, 3, '2024-03-10'),
            (8, 'Samsung Galaxy Buds', 'Electronics', 149.00, 90.00, 120, 2, '2024-03-15')
        ]
        cursor.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?)", products_data)
        
        customers_data = [
            (1, 'Michael', 'Scott', 'michael@dundermifflin.com', '555-0201', '1725 Slough Ave', 'Scranton', 'PA', '18505', 1250, '2024-01-10'),
            (2, 'Dwight', 'Schrute', 'dwight@schrutefarms.com', '555-0202', '1 Schrute Farms Rd', 'Honesdale', 'PA', '18431', 890, '2024-01-15'),
            (3, 'Jim', 'Halpert', 'jim@athleap.com', '555-0203', '42 Wallaby Way', 'Philadelphia', 'PA', '19103', 2100, '2024-02-01'),
            (4, 'Pam', 'Beesly', 'pam@dundermifflin.com', '555-0204', '123 Art St', 'Scranton', 'PA', '18505', 750, '2024-02-10'),
            (5, 'Andy', 'Bernard', 'andy@cornell.edu', '555-0205', '456 Cornell Ave', 'Ithaca', 'NY', '14850', 1500, '2024-02-15')
        ]
        cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", customers_data)
        
        orders_data = [
            (1001, 1, '2024-12-01', 999.00, 79.92, 0.00, 'DELIVERED', '1725 Slough Ave, Scranton, PA 18505'),
            (1002, 2, '2024-12-02', 330.00, 26.40, 9.99, 'SHIPPED', '1 Schrute Farms Rd, Honesdale, PA 18431'),
            (1003, 3, '2024-12-03', 2399.00, 191.92, 0.00, 'PROCESSING', '42 Wallaby Way, Philadelphia, PA 19103'),
            (1004, 4, '2024-12-04', 249.00, 19.92, 5.99, 'DELIVERED', '123 Art St, Scranton, PA 18505'),
            (1005, 5, '2024-12-05', 340.00, 27.20, 7.99, 'SHIPPED', '456 Cornell Ave, Ithaca, NY 14850')
        ]
        cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", orders_data)
        
        order_items_data = [
            (1, 1001, 1, 1, 999.00, 999.00),
            (2, 1002, 3, 2, 150.00, 300.00),
            (3, 1002, 7, 1, 160.00, 160.00),
            (4, 1003, 5, 1, 2399.00, 2399.00),
            (5, 1004, 6, 1, 249.00, 249.00),
            (6, 1005, 4, 1, 180.00, 180.00),
            (7, 1005, 8, 1, 149.00, 149.00)
        ]
        cursor.executemany("INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?)", order_items_data)
        
        conn.commit()
        conn.close()
    
    def load_databases_to_snowflake(self, databases: List):
        """Load all database tables to Snowflake"""
        
        logger.info("Loading database tables to Snowflake...")
        
        total_tables_created = 0
        database_metadata = []
        table_metadata = []
        
        for db_name, db_path, domain in databases:
            logger.info(f"Processing database: {db_name}")
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Get all tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                
                db_tables_created = 0
                
                for table_name in tables:
                    snowflake_table_name = f"BIRD_{db_name.upper()}_{table_name.upper()}"
                    
                    try:
                        # Get table schema
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        columns = cursor.fetchall()
                        
                        # Create Snowflake DDL
                        ddl_parts = [f"CREATE OR REPLACE TABLE {snowflake_table_name} ("]
                        column_defs = []
                        
                        for col_idx, col in enumerate(columns):
                            col_name = col[1].upper()
                            col_type = self._convert_sqlite_type(col[2])
                            is_pk = bool(col[5])
                            
                            col_def = f"    {col_name} {col_type}"
                            if is_pk:
                                col_def += " PRIMARY KEY"
                            
                            column_defs.append(col_def)
                            
                            # Store column metadata
                            table_metadata.append({
                                'id': f"{db_name}_{table_name}_{col[1]}",
                                'db_id': db_name,
                                'table_name': table_name,
                                'column_name': col[1],
                                'column_type': col[2],
                                'is_primary_key': is_pk,
                                'is_foreign_key': False,  # Could enhance this
                                'foreign_table': None,
                                'foreign_column': None,
                                'column_position': col_idx
                            })
                        
                        ddl_parts.append(",\n".join(column_defs))
                        ddl_parts.append(")")
                        
                        ddl = "\n".join(ddl_parts)
                        
                        # Execute DDL
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
                            db_tables_created += 1
                            total_tables_created += 1
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to load table {table_name}: {e}")
                
                # Store database metadata
                database_metadata.append({
                    'db_id': db_name,
                    'db_name': db_name,
                    'domain': domain,
                    'num_tables': db_tables_created,
                    'description': f"Sample {domain} database for BIRD dataset",
                    'size_mb': os.path.getsize(db_path) / (1024 * 1024)
                })
                
                conn.close()
                logger.info(f"✅ Completed {db_name}: {db_tables_created} tables loaded")
                
            except Exception as e:
                logger.error(f"❌ Failed to process database {db_name}: {e}")
        
        # Load metadata to Snowflake
        if database_metadata:
            db_df = pd.DataFrame(database_metadata)
            db_df.columns = [col.upper() for col in db_df.columns]
            write_pandas(self.conn, db_df, 'BIRD_DATABASES', auto_create_table=False, overwrite=True)
            logger.info(f"✅ Loaded {len(database_metadata)} database metadata records")
        
        if table_metadata:
            table_df = pd.DataFrame(table_metadata)
            table_df.columns = [col.upper() for col in table_df.columns]
            write_pandas(self.conn, table_df, 'BIRD_TABLE_SCHEMAS', auto_create_table=False, overwrite=True)
            logger.info(f"✅ Loaded {len(table_metadata)} table schema records")
        
        return total_tables_created
    
    def _convert_sqlite_type(self, sqlite_type: str) -> str:
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
        
        # Handle specific patterns
        if 'VARCHAR(' in sqlite_upper:
            return sqlite_type
        if 'DECIMAL(' in sqlite_upper:
            return sqlite_type
            
        for sqlite_key, snowflake_type in type_mapping.items():
            if sqlite_key in sqlite_upper:
                return snowflake_type
        
        return 'VARCHAR(16777216)'
    
    def load_sample_questions(self):
        """Load sample questions from BIRD dataset"""
        try:
            logger.info("Loading sample BIRD questions...")
            dataset = load_dataset("birdsql/bird-critic-1.0-flash-exp", split="flash[:50]")
            
            questions_data = []
            for idx, item in enumerate(dataset):
                questions_data.append({
                    'id': f"bird_{idx:04d}",
                    'db_id': item.get('db_id', ''),
                    'question': item.get('question', ''),
                    'evidence': item.get('evidence', ''),
                    'sql_query': item.get('SQL', ''),
                    'difficulty': item.get('difficulty', 'unknown'),
                    'domain': self._extract_domain(item.get('db_id', ''))
                })
            
            if questions_data:
                questions_df = pd.DataFrame(questions_data)
                questions_df.columns = [col.upper() for col in questions_df.columns]
                
                success, nchunks, nrows, _ = write_pandas(
                    self.conn, 
                    questions_df, 
                    'BIRD_QUESTIONS',
                    auto_create_table=False,
                    overwrite=True
                )
                logger.info(f"✅ Loaded {nrows} BIRD questions")
            
        except Exception as e:
            logger.error(f"❌ Failed to load questions: {e}")
    
    def _extract_domain(self, db_id: str) -> str:
        """Extract domain from database ID"""
        db_id_lower = db_id.lower()
        domain_keywords = {
            'financial': ['bank', 'finance', 'money', 'credit', 'loan', 'account'],
            'education': ['school', 'student', 'university', 'college', 'course'],
            'retail': ['store', 'shop', 'retail', 'customer', 'product', 'order'],
            'healthcare': ['hospital', 'medical', 'patient', 'health', 'doctor'],
            'sports': ['game', 'sport', 'team', 'player', 'match', 'league']
        }
        
        for domain, keywords in domain_keywords.items():
            if any(keyword in db_id_lower for keyword in keywords):
                return domain
        return 'general'
    
    def run_validation_queries(self):
        """Run comprehensive validation queries"""
        
        validation_queries = [
            ("Database Summary", """
                SELECT 
                    DOMAIN,
                    COUNT(*) as database_count,
                    SUM(NUM_TABLES) as total_tables,
                    AVG(SIZE_MB) as avg_size_mb
                FROM bird_databases 
                GROUP BY DOMAIN 
                ORDER BY database_count DESC
            """),
            
            ("Table Count by Database", """
                SELECT DB_ID, NUM_TABLES, DESCRIPTION
                FROM bird_databases
                ORDER BY NUM_TABLES DESC
            """),
            
            ("Sample Questions by Domain", """
                SELECT 
                    DOMAIN,
                    COUNT(*) as question_count,
                    AVG(LENGTH(QUESTION)) as avg_question_length
                FROM bird_questions
                GROUP BY DOMAIN
                ORDER BY question_count DESC
                LIMIT 5
            """),
            
            ("Financial Database Sample", """
                SELECT FIRST_NAME, LAST_NAME, BALANCE, ACCOUNT_TYPE
                FROM BIRD_FINANCIAL_CUSTOMERS c
                JOIN BIRD_FINANCIAL_ACCOUNTS a ON c.CUSTOMER_ID = a.CUSTOMER_ID
                ORDER BY BALANCE DESC
                LIMIT 5
            """),
            
            ("Education Database Sample", """
                SELECT s.FIRST_NAME, s.LAST_NAME, s.MAJOR, s.GPA, COUNT(e.COURSE_ID) as courses_enrolled
                FROM BIRD_EDUCATION_STUDENTS s
                LEFT JOIN BIRD_EDUCATION_ENROLLMENTS e ON s.STUDENT_ID = e.STUDENT_ID
                GROUP BY s.STUDENT_ID, s.FIRST_NAME, s.LAST_NAME, s.MAJOR, s.GPA
                ORDER BY s.GPA DESC
                LIMIT 5
            """),
            
            ("Retail Database Sample", """
                SELECT c.FIRST_NAME, c.LAST_NAME, o.ORDER_DATE, o.TOTAL_AMOUNT, o.STATUS
                FROM BIRD_RETAIL_CUSTOMERS c
                JOIN BIRD_RETAIL_ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
                ORDER BY o.ORDER_DATE DESC
                LIMIT 5
            """)
        ]
        
        logger.info("Running comprehensive validation queries...")
        
        for description, query in validation_queries:
            try:
                logger.info(f"\n📊 {description}:")
                self.cursor.execute(query)
                results = self.cursor.fetchall()
                
                if results:
                    # Get column names
                    columns = [desc[0] for desc in self.cursor.description]
                    
                    # Print header
                    print("  " + " | ".join(f"{col:20}" for col in columns))
                    print("  " + "-" * (len(columns) * 23))
                    
                    # Print rows
                    for row in results:
                        formatted_row = []
                        for value in row:
                            if isinstance(value, (int, float)) and abs(value) > 999:
                                formatted_row.append(f"{value:,.2f}")
                            elif isinstance(value, str) and len(value) > 20:
                                formatted_row.append(value[:17] + "...")
                            else:
                                formatted_row.append(str(value))
                        print("  " + " | ".join(f"{val:20}" for val in formatted_row))
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
    """Main function for mini BIRD dataset loading"""
    
    print("🗄️ BIRD Mini Dataset Loader for Snowflake")
    print("Loading: Questions + 3 Sample Databases with Real Data")
    print("=" * 65)
    
    # Load config
    config = load_config()
    if not config:
        return 1
    
    loader = SnowflakeBirdMiniLoader(config)
    
    try:
        # Test connection
        if not loader.connect():
            return 1
        
        # Test warehouse
        loader.test_warehouse()
        
        # Create schema
        loader.create_comprehensive_schema()
        
        # Create sample databases
        databases = loader.create_sample_databases()
        
        # Load databases to Snowflake
        tables_created = loader.load_databases_to_snowflake(databases)
        
        # Load sample questions
        loader.load_sample_questions()
        
        # Run validation queries
        loader.run_validation_queries()
        
        print(f"\n✅ Mini BIRD dataset loaded successfully!")
        print(f"📊 Created {tables_created} database tables with real data")
        print(f"🔍 Loaded sample questions from BIRD dataset")
        print(f"🗄️ 3 domains: Financial, Education, Retail")
        
        print(f"\n💡 You now have actual database tables to run SQL queries against!")
        print(f"💡 Ready for full loader: python snowflake_bird_full_loader.py")
        
    except Exception as e:
        logger.error(f"❌ Loading failed: {e}")
        return 1
    
    finally:
        loader.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())