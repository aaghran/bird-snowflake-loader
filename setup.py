#!/usr/bin/env python3
"""
BIRD Dataset Snowflake Loader Setup Script

This script helps you get started with loading the BIRD dataset into Snowflake.
"""

import os
import sys
import json
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 7):
        print("❌ Python 3.7 or higher is required")
        return False
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    return True

def check_virtual_environment():
    """Check if running in virtual environment"""
    in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    if in_venv:
        print("✅ Running in virtual environment")
    else:
        print("⚠️  Not in virtual environment (recommended)")
        print("   Run: python3 -m venv venv && source venv/bin/activate")
    return True

def check_dependencies():
    """Check if required packages are installed"""
    required_packages = [
        'snowflake-connector-python',
        'pandas', 
        'datasets',
        'numpy',
        'requests'
    ]
    
    missing = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            missing.append(package)
            print(f"❌ {package}")
    
    if missing:
        print(f"\n📦 Install missing packages:")
        print(f"pip install {' '.join(missing)}")
        return False
    
    return True

def setup_config():
    """Setup configuration file"""
    config_file = Path("snowflake_config.json")
    template_file = Path("snowflake_config.template.json")
    
    if config_file.exists():
        print("✅ snowflake_config.json exists")
        
        # Check if it has real values
        with open(config_file, 'r') as f:
            config = json.load(f)
            
        if config.get('account', '').startswith('YOUR_'):
            print("⚠️  Configuration has template values")
            print("   Please update snowflake_config.json with your Snowflake credentials")
            return False
        else:
            print("✅ Configuration appears to be set")
            return True
    
    elif template_file.exists():
        print("📝 Creating snowflake_config.json from template")
        with open(template_file, 'r') as f:
            template = f.read()
        
        with open(config_file, 'w') as f:
            f.write(template)
        
        print("⚠️  Please edit snowflake_config.json with your credentials")
        return False
    
    else:
        print("❌ No configuration template found")
        return False

def check_environment_variables():
    """Check environment variables"""
    env_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
    
    has_env = all(os.getenv(var) for var in env_vars)
    if has_env:
        print("✅ Snowflake environment variables set")
        return True
    else:
        missing_vars = [var for var in env_vars if not os.getenv(var)]
        print(f"❌ Missing environment variables: {missing_vars}")
        return False

def main():
    """Main setup function"""
    print("🗄️ BIRD Dataset Snowflake Loader Setup")
    print("=" * 50)
    
    checks = [
        ("Python Version", check_python_version),
        ("Virtual Environment", check_virtual_environment), 
        ("Dependencies", check_dependencies),
        ("Configuration", setup_config),
        ("Environment Variables", check_environment_variables)
    ]
    
    all_passed = True
    for name, check_func in checks:
        print(f"\n🔍 Checking {name}...")
        if not check_func():
            all_passed = False
    
    print("\n" + "=" * 50)
    
    if all_passed:
        print("🎉 Setup complete! Ready to load BIRD dataset")
        print("\nNext steps:")
        print("1. python download_bird_databases.py")  
        print("2. python snowflake_bird_full_loader.py")
    else:
        print("⚠️  Setup incomplete. Please address the issues above.")
        
        # Show help for common issues
        print("\n💡 Quick fixes:")
        print("• Install dependencies: pip install -r requirements.txt")
        print("• Set environment variables or update snowflake_config.json")
        print("• Use virtual environment: python3 -m venv venv && source venv/bin/activate")

if __name__ == "__main__":
    main()