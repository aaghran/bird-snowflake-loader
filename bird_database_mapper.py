#!/usr/bin/env python3
"""
BIRD Database Schema Mapper

This utility helps map and analyze the 95 databases across 37 professional domains
in the BIRD dataset. It provides detailed schema analysis and domain classification.
"""

import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Set
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BirdDatabaseMapper:
    """Maps and analyzes BIRD database schemas by domain"""
    
    def __init__(self):
        self.domain_mappings = {
            # Financial Services
            'financial': {
                'keywords': ['bank', 'finance', 'trading', 'stock', 'money', 'credit', 'loan', 'payment', 'investment'],
                'databases': [],
                'description': 'Banking, trading, investments, financial services'
            },
            
            # Healthcare & Medicine
            'healthcare': {
                'keywords': ['hospital', 'medical', 'patient', 'health', 'doctor', 'clinic', 'medicine', 'treatment'],
                'databases': [],
                'description': 'Hospitals, medical records, patient management'
            },
            
            # Education
            'education': {
                'keywords': ['school', 'student', 'university', 'college', 'academic', 'course', 'class', 'grade'],
                'databases': [],
                'description': 'Schools, universities, student management systems'
            },
            
            # Retail & E-commerce
            'retail': {
                'keywords': ['store', 'shop', 'retail', 'customer', 'product', 'sales', 'order', 'purchase'],
                'databases': [],
                'description': 'Retail stores, e-commerce, sales management'
            },
            
            # Sports & Recreation
            'sports': {
                'keywords': ['game', 'sport', 'team', 'player', 'match', 'league', 'tournament', 'athlete'],
                'databases': [],
                'description': 'Sports leagues, games, player statistics'
            },
            
            # Technology & Software
            'technology': {
                'keywords': ['software', 'tech', 'computer', 'system', 'app', 'web', 'platform', 'network'],
                'databases': [],
                'description': 'Software systems, technology platforms'
            },
            
            # Entertainment & Media
            'entertainment': {
                'keywords': ['movie', 'film', 'music', 'artist', 'show', 'concert', 'media', 'theater'],
                'databases': [],
                'description': 'Movies, music, entertainment industry'
            },
            
            # Transportation & Logistics
            'transportation': {
                'keywords': ['car', 'flight', 'train', 'transport', 'airline', 'vehicle', 'shipping', 'logistics'],
                'databases': [],
                'description': 'Airlines, transportation, logistics'
            },
            
            # Government & Public Services
            'government': {
                'keywords': ['government', 'public', 'city', 'county', 'state', 'federal', 'municipal', 'civic'],
                'databases': [],
                'description': 'Government agencies, public services'
            },
            
            # Real Estate & Property
            'real_estate': {
                'keywords': ['property', 'house', 'building', 'real_estate', 'apartment', 'rent', 'lease'],
                'databases': [],
                'description': 'Real estate, property management'
            },
            
            # Human Resources
            'human_resources': {
                'keywords': ['employee', 'hr', 'payroll', 'staff', 'personnel', 'workforce', 'hiring'],
                'databases': [],
                'description': 'Human resources, employee management'
            },
            
            # Manufacturing & Supply Chain
            'manufacturing': {
                'keywords': ['factory', 'production', 'manufacturing', 'supply', 'warehouse', 'inventory'],
                'databases': [],
                'description': 'Manufacturing, supply chain management'
            },
            
            # Telecommunications
            'telecom': {
                'keywords': ['telecom', 'phone', 'mobile', 'network', 'communication', 'cellular'],
                'databases': [],
                'description': 'Telecommunications, mobile networks'
            },
            
            # Insurance
            'insurance': {
                'keywords': ['insurance', 'policy', 'claim', 'coverage', 'premium', 'risk'],
                'databases': [],
                'description': 'Insurance companies, policy management'
            },
            
            # Food & Restaurants
            'food_service': {
                'keywords': ['restaurant', 'food', 'menu', 'dining', 'cafe', 'recipe', 'nutrition'],
                'databases': [],
                'description': 'Restaurants, food service industry'
            },
            
            # Energy & Utilities
            'energy': {
                'keywords': ['energy', 'power', 'utility', 'electric', 'gas', 'solar', 'renewable'],
                'databases': [],
                'description': 'Energy companies, utilities'
            },
            
            # Agriculture
            'agriculture': {
                'keywords': ['farm', 'agriculture', 'crop', 'livestock', 'farming', 'rural'],
                'databases': [],
                'description': 'Agriculture, farming operations'
            },
            
            # Legal & Law Enforcement
            'legal': {
                'keywords': ['legal', 'law', 'court', 'justice', 'attorney', 'police', 'crime'],
                'databases': [],
                'description': 'Legal systems, law enforcement'
            },
            
            # Non-profit & Social Services
            'nonprofit': {
                'keywords': ['nonprofit', 'charity', 'social', 'volunteer', 'donation', 'community'],
                'databases': [],
                'description': 'Non-profit organizations, social services'
            },
            
            # Tourism & Travel
            'tourism': {
                'keywords': ['travel', 'tourism', 'hotel', 'vacation', 'booking', 'destination'],
                'databases': [],
                'description': 'Tourism, travel booking, hospitality'
            },
            
            # Research & Science
            'research': {
                'keywords': ['research', 'science', 'laboratory', 'experiment', 'study', 'academic'],
                'databases': [],
                'description': 'Scientific research, laboratories'
            }
        }
        
        # Common database patterns that help identify domains
        self.table_patterns = {
            'financial': ['account', 'transaction', 'payment', 'balance', 'credit', 'debit'],
            'healthcare': ['patient', 'doctor', 'diagnosis', 'treatment', 'medical_record'],
            'education': ['student', 'course', 'grade', 'enrollment', 'teacher', 'class'],
            'retail': ['customer', 'order', 'product', 'inventory', 'sale', 'purchase'],
            'sports': ['player', 'team', 'game', 'score', 'match', 'season'],
            'transportation': ['flight', 'passenger', 'route', 'schedule', 'vehicle'],
            'entertainment': ['movie', 'actor', 'artist', 'album', 'song', 'show'],
            'government': ['citizen', 'permit', 'license', 'department', 'official'],
            'real_estate': ['property', 'listing', 'agent', 'buyer', 'seller', 'mortgage']
        }
    
    def classify_database(self, db_id: str, table_names: List[str] = None, 
                         column_names: List[str] = None) -> str:
        """Classify database into domain based on ID, table names, and columns"""
        
        db_id_lower = db_id.lower()
        
        # Score each domain
        domain_scores = {}
        
        for domain, info in self.domain_mappings.items():
            score = 0
            
            # Check database ID against keywords
            for keyword in info['keywords']:
                if keyword in db_id_lower:
                    score += 10
            
            # Check table names if provided
            if table_names:
                table_patterns = self.table_patterns.get(domain, [])
                for table in table_names:
                    table_lower = table.lower()
                    for pattern in table_patterns:
                        if pattern in table_lower:
                            score += 5
                    
                    # Also check against domain keywords
                    for keyword in info['keywords']:
                        if keyword in table_lower:
                            score += 3
            
            # Check column names if provided
            if column_names:
                for column in column_names:
                    column_lower = column.lower()
                    for keyword in info['keywords']:
                        if keyword in column_lower:
                            score += 1
            
            domain_scores[domain] = score
        
        # Return domain with highest score, or 'general' if no clear match
        if max(domain_scores.values()) > 0:
            return max(domain_scores, key=domain_scores.get)
        return 'general'
    
    def analyze_database_file(self, db_path: str) -> Dict:
        """Analyze a single SQLite database file"""
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get database info
            db_name = Path(db_path).stem
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Get all column names
            all_columns = []
            table_details = {}
            
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                
                table_info = {
                    'columns': [col[1] for col in columns],
                    'column_types': [col[2] for col in columns],
                    'primary_keys': [col[1] for col in columns if col[5]]
                }
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                table_info['row_count'] = cursor.fetchone()[0]
                
                # Get foreign keys
                cursor.execute(f"PRAGMA foreign_key_list({table})")
                fks = cursor.fetchall()
                table_info['foreign_keys'] = [(fk[3], fk[2], fk[4]) for fk in fks]
                
                table_details[table] = table_info
                all_columns.extend(table_info['columns'])
            
            conn.close()
            
            # Classify domain
            domain = self.classify_database(db_name, tables, all_columns)
            
            # Calculate complexity metrics
            total_tables = len(tables)
            total_columns = len(all_columns)
            total_foreign_keys = sum(len(info['foreign_keys']) for info in table_details.values())
            total_rows = sum(info['row_count'] for info in table_details.values())
            
            return {
                'db_id': db_name,
                'domain': domain,
                'file_path': db_path,
                'tables': tables,
                'table_count': total_tables,
                'total_columns': total_columns,
                'total_foreign_keys': total_foreign_keys,
                'total_rows': total_rows,
                'table_details': table_details,
                'complexity_score': self._calculate_complexity_score(total_tables, total_columns, total_foreign_keys)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing {db_path}: {e}")
            return None
    
    def _calculate_complexity_score(self, tables: int, columns: int, foreign_keys: int) -> float:
        """Calculate a complexity score for the database"""
        return (tables * 1.0) + (columns * 0.1) + (foreign_keys * 2.0)
    
    def map_all_databases(self, db_directory: str) -> Dict:
        """Map all databases in a directory"""
        
        db_path = Path(db_directory)
        if not db_path.exists():
            logger.error(f"Directory {db_directory} does not exist")
            return {}
        
        # Find all SQLite files
        sqlite_files = list(db_path.glob("*.sqlite")) + list(db_path.glob("*.db"))
        
        logger.info(f"Found {len(sqlite_files)} database files")
        
        mapping_results = {
            'databases': [],
            'domain_summary': {},
            'complexity_analysis': {},
            'total_databases': len(sqlite_files)
        }
        
        # Analyze each database
        for db_file in sqlite_files:
            logger.info(f"Analyzing {db_file.name}...")
            
            result = self.analyze_database_file(str(db_file))
            if result:
                mapping_results['databases'].append(result)
                
                # Update domain mappings
                domain = result['domain']
                self.domain_mappings[domain]['databases'].append(result['db_id'])
        
        # Generate domain summary
        for domain, info in self.domain_mappings.items():
            if info['databases']:
                domain_dbs = [db for db in mapping_results['databases'] if db['domain'] == domain]
                mapping_results['domain_summary'][domain] = {
                    'count': len(info['databases']),
                    'databases': info['databases'],
                    'description': info['description'],
                    'avg_complexity': sum(db['complexity_score'] for db in domain_dbs) / len(domain_dbs),
                    'total_tables': sum(db['table_count'] for db in domain_dbs),
                    'total_rows': sum(db['total_rows'] for db in domain_dbs)
                }
        
        # Complexity analysis
        all_scores = [db['complexity_score'] for db in mapping_results['databases']]
        mapping_results['complexity_analysis'] = {
            'min_complexity': min(all_scores) if all_scores else 0,
            'max_complexity': max(all_scores) if all_scores else 0,
            'avg_complexity': sum(all_scores) / len(all_scores) if all_scores else 0,
            'most_complex': max(mapping_results['databases'], key=lambda x: x['complexity_score'])['db_id'] if all_scores else None,
            'least_complex': min(mapping_results['databases'], key=lambda x: x['complexity_score'])['db_id'] if all_scores else None
        }
        
        return mapping_results
    
    def generate_snowflake_schema_script(self, mapping_results: Dict) -> str:
        """Generate Snowflake schema creation script based on mapping"""
        
        script_parts = [
            "-- BIRD Dataset Domain-Specific Schema Creation Script",
            "-- Generated by BIRD Database Mapper\n",
            "USE DATABASE BIRD_DB;",
            "USE SCHEMA PUBLIC;\n"
        ]
        
        # Create domain-specific schemas
        for domain, summary in mapping_results['domain_summary'].items():
            if summary['count'] > 0:
                schema_name = f"BIRD_{domain.upper()}"
                script_parts.extend([
                    f"-- {summary['description']} ({summary['count']} databases)",
                    f"CREATE SCHEMA IF NOT EXISTS {schema_name};",
                    f"USE SCHEMA {schema_name};\n"
                ])
                
                # Create sample tables for this domain
                for db_id in summary['databases'][:3]:  # Limit to first 3 for brevity
                    db_data = next(db for db in mapping_results['databases'] if db['db_id'] == db_id)
                    script_parts.append(f"-- Tables for database: {db_id}")
                    
                    for table_name, table_info in db_data['table_details'].items():
                        table_ddl = self._generate_table_ddl(f"{db_id}_{table_name}", table_info)
                        script_parts.append(table_ddl)
                    
                    script_parts.append("")
        
        return "\n".join(script_parts)
    
    def _generate_table_ddl(self, table_name: str, table_info: Dict) -> str:
        """Generate DDL for a single table"""
        
        lines = [f"CREATE OR REPLACE TABLE {table_name.upper()} ("]
        
        # Add columns
        column_lines = []
        for i, (col_name, col_type) in enumerate(zip(table_info['columns'], table_info['column_types'])):
            snowflake_type = self._convert_sqlite_type(col_type)
            is_pk = col_name in table_info['primary_keys']
            
            col_line = f"    {col_name.upper()} {snowflake_type}"
            if is_pk:
                col_line += " PRIMARY KEY"
            
            column_lines.append(col_line)
        
        lines.append(",\n".join(column_lines))
        lines.append(");")
        
        return "\n".join(lines)
    
    def _convert_sqlite_type(self, sqlite_type: str) -> str:
        """Convert SQLite types to Snowflake types"""
        
        type_mapping = {
            'INTEGER': 'INTEGER',
            'TEXT': 'VARCHAR(16777216)',
            'REAL': 'FLOAT',
            'NUMERIC': 'NUMERIC(38,2)',
            'BLOB': 'VARIANT'
        }
        
        sqlite_upper = sqlite_type.upper()
        for sqlite_type, snowflake_type in type_mapping.items():
            if sqlite_type in sqlite_upper:
                return snowflake_type
        
        return 'VARCHAR(16777216)'  # Default
    
    def save_mapping_results(self, mapping_results: Dict, output_file: str = "bird_database_mapping.json"):
        """Save mapping results to JSON file"""
        
        # Make results JSON serializable
        serializable_results = {
            'domain_summary': mapping_results['domain_summary'],
            'complexity_analysis': mapping_results['complexity_analysis'],
            'total_databases': mapping_results['total_databases'],
            'database_list': [
                {
                    'db_id': db['db_id'],
                    'domain': db['domain'],
                    'table_count': db['table_count'],
                    'total_columns': db['total_columns'],
                    'total_foreign_keys': db['total_foreign_keys'],
                    'total_rows': db['total_rows'],
                    'complexity_score': db['complexity_score'],
                    'tables': db['tables']
                }
                for db in mapping_results['databases']
            ]
        }
        
        with open(output_file, 'w') as f:
            json.dump(serializable_results, f, indent=2)
        
        logger.info(f"Saved mapping results to {output_file}")

def main():
    """Main function for database mapping"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Map BIRD databases by domain')
    parser.add_argument('db_directory', help='Directory containing SQLite database files')
    parser.add_argument('--output', '-o', default='bird_database_mapping.json', 
                       help='Output file for mapping results')
    parser.add_argument('--schema-script', '-s', help='Generate Snowflake schema script')
    
    args = parser.parse_args()
    
    mapper = BirdDatabaseMapper()
    results = mapper.map_all_databases(args.db_directory)
    
    if results['databases']:
        logger.info(f"Successfully mapped {len(results['databases'])} databases")
        logger.info(f"Found {len(results['domain_summary'])} domains")
        
        # Print summary
        print("\nDomain Summary:")
        for domain, summary in results['domain_summary'].items():
            print(f"  {domain}: {summary['count']} databases, avg complexity: {summary['avg_complexity']:.2f}")
        
        # Save results
        mapper.save_mapping_results(results, args.output)
        
        # Generate schema script if requested
        if args.schema_script:
            schema_script = mapper.generate_snowflake_schema_script(results)
            with open(args.schema_script, 'w') as f:
                f.write(schema_script)
            logger.info(f"Generated Snowflake schema script: {args.schema_script}")
    
    else:
        logger.error("No databases found or analyzed successfully")

if __name__ == "__main__":
    main()