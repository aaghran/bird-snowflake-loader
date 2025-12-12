#!/usr/bin/env python3
"""
Inspect BIRD datasets to understand their structure
"""

from datasets import load_dataset
import json

def inspect_datasets():
    print("🔍 Inspecting BIRD datasets from HuggingFace...")
    
    # Load main dataset
    print("\n1. Loading main dataset (birdsql/bird-critic-1.0-flash-exp)...")
    try:
        main_dataset = load_dataset("birdsql/bird-critic-1.0-flash-exp")
        print(f"✅ Loaded main dataset")
        print(f"Splits: {list(main_dataset.keys())}")
        
        if 'flash' in main_dataset:
            flash_data = main_dataset['flash']
            print(f"Flash split has {len(flash_data)} examples")
            
            # Inspect first example
            if len(flash_data) > 0:
                first_example = flash_data[0]
                print(f"First example keys: {list(first_example.keys())}")
                print(f"Sample question: {first_example.get('question', 'N/A')[:100]}...")
                print(f"Database ID: {first_example.get('db_id', 'N/A')}")
                print(f"SQL: {first_example.get('SQL', 'N/A')[:100]}...")
    except Exception as e:
        print(f"❌ Failed to load main dataset: {e}")
    
    # Load mini-dev dataset
    print("\n2. Loading mini-dev dataset (birdsql/bird_mini_dev)...")
    try:
        mini_dataset = load_dataset("birdsql/bird_mini_dev")
        print(f"✅ Loaded mini-dev dataset")
        print(f"Splits: {list(mini_dataset.keys())}")
        
        for split_name, split_data in mini_dataset.items():
            print(f"\nSplit '{split_name}' has {len(split_data)} examples")
            
            if len(split_data) > 0:
                first_example = split_data[0]
                print(f"Keys: {list(first_example.keys())}")
                
                # Check for database-related fields
                for key in first_example.keys():
                    if any(db_word in key.lower() for db_word in ['db', 'database', 'sqlite']):
                        value = first_example[key]
                        if isinstance(value, bytes):
                            print(f"  {key}: <bytes, length {len(value)}>")
                        elif isinstance(value, str):
                            print(f"  {key}: {value[:100]}...")
                        else:
                            print(f"  {key}: {type(value)} - {value}")
                
    except Exception as e:
        print(f"❌ Failed to load mini-dev dataset: {e}")
    
    # Load LiveSQL dataset
    print("\n3. Loading LiveSQL dataset (birdsql/livesqlbench-base-lite-sqlite)...")
    try:
        livesql_dataset = load_dataset("birdsql/livesqlbench-base-lite-sqlite")
        print(f"✅ Loaded LiveSQL dataset")
        print(f"Splits: {list(livesql_dataset.keys())}")
        
        for split_name, split_data in livesql_dataset.items():
            print(f"\nSplit '{split_name}' has {len(split_data)} examples")
            
            if len(split_data) > 0:
                first_example = split_data[0]
                print(f"Keys: {list(first_example.keys())}")
                
                # Check for database-related fields
                for key in first_example.keys():
                    if any(db_word in key.lower() for db_word in ['db', 'database', 'sqlite']):
                        value = first_example[key]
                        if isinstance(value, bytes):
                            print(f"  {key}: <bytes, length {len(value)}>")
                        elif isinstance(value, str):
                            print(f"  {key}: {value[:100]}...")
                        else:
                            print(f"  {key}: {type(value)} - {value}")
    
    except Exception as e:
        print(f"❌ Failed to load LiveSQL dataset: {e}")

if __name__ == "__main__":
    inspect_datasets()