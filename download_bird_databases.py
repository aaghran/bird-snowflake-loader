#!/usr/bin/env python3
"""
BIRD SQLite Database Downloader

This script downloads the actual SQLite database files from the BIRD dataset
for use with the Snowflake loader.

Sources:
- Official Aliyun OSS: https://bird-bench.oss-cn-beijing.aliyuncs.com/minidev.zip
- Google Drive: https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view?usp=sharing
"""

import os
import sys
import json
import requests
import zipfile
import tempfile
from pathlib import Path
import logging
from typing import Dict, List
import shutil
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BirdDatabaseDownloader:
    """Downloads BIRD SQLite databases from official sources"""
    
    def __init__(self, download_dir: str = "./bird_databases"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.temp_dir = None
        
        # Official download sources
        self.download_sources = [
            {
                'name': 'Aliyun OSS (Official)',
                'url': 'https://bird-bench.oss-cn-beijing.aliyuncs.com/minidev.zip',
                'type': 'zip',
                'size_gb': 0.1  # Approximate size
            },
            {
                'name': 'Google Drive (Backup)',
                'url': 'https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view?usp=sharing',
                'direct_url': 'https://drive.google.com/uc?id=13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG',
                'type': 'zip', 
                'size_gb': 0.1
            }
        ]
        
    def download_databases(self) -> Dict[str, str]:
        """Download BIRD SQLite databases from official sources"""
        
        logger.info("🔍 Starting BIRD database download...")
        logger.info(f"Download directory: {self.download_dir}")
        
        self.temp_dir = tempfile.mkdtemp()
        logger.info(f"Temporary directory: {self.temp_dir}")
        
        db_paths = {}
        
        # Try each download source
        for source in self.download_sources:
            logger.info(f"\n📥 Attempting download from: {source['name']}")
            logger.info(f"Size: ~{source['size_gb']} GB")
            
            try:
                success, extracted_paths = self._download_from_source(source)
                if success and extracted_paths:
                    db_paths.update(extracted_paths)
                    logger.info(f"✅ Successfully downloaded from {source['name']}")
                    break
                else:
                    logger.warning(f"❌ Failed to download from {source['name']}")
            except Exception as e:
                logger.error(f"❌ Error downloading from {source['name']}: {e}")
        
        if not db_paths:
            logger.warning("❌ No databases could be downloaded automatically")
            self._show_manual_instructions()
        else:
            logger.info(f"✅ Successfully extracted {len(db_paths)} database files")
            
        return db_paths
    
    def _download_from_source(self, source: Dict) -> tuple:
        """Download and extract from a specific source"""
        
        # Determine the URL to use
        url = source.get('direct_url', source['url'])
        
        # Download the file
        download_path = os.path.join(self.temp_dir, f"minidev.{source['type']}")
        
        if not self._download_file(url, download_path):
            return False, {}
        
        # Extract the downloaded file
        if source['type'] == 'zip':
            return self._extract_zip_file(download_path)
        else:
            logger.error(f"Unknown file type: {source['type']}")
            return False, {}
    
    def _download_file(self, url: str, output_path: str) -> bool:
        """Download a file with progress tracking"""
        
        logger.info(f"Downloading from: {url}")
        
        try:
            # Start the download
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get file size if available
            total_size = int(response.headers.get('content-length', 0))
            
            downloaded_size = 0
            chunk_size = 8192
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Show progress every 10MB
                        if downloaded_size % (10 * 1024 * 1024) == 0 or total_size == 0:
                            if total_size > 0:
                                progress = (downloaded_size / total_size) * 100
                                logger.info(f"Progress: {downloaded_size / 1024 / 1024:.1f}MB / {total_size / 1024 / 1024:.1f}MB ({progress:.1f}%)")
                            else:
                                logger.info(f"Downloaded: {downloaded_size / 1024 / 1024:.1f}MB")
            
            logger.info(f"✅ Download completed: {downloaded_size / 1024 / 1024:.1f}MB")
            return True
            
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            return False
    
    def _extract_zip_file(self, zip_path: str) -> tuple:
        """Extract ZIP file and find SQLite databases"""
        
        logger.info("📦 Extracting ZIP file...")
        
        extract_path = os.path.join(self.temp_dir, "extracted")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            
            logger.info(f"✅ Extracted to: {extract_path}")
            
            # Find SQLite database files
            db_paths = self._find_sqlite_databases(extract_path)
            
            if db_paths:
                # Copy databases to permanent location
                final_db_paths = self._copy_databases_to_final_location(db_paths)
                return True, final_db_paths
            else:
                logger.warning("❌ No SQLite database files found in extracted content")
                return False, {}
                
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            return False, {}
    
    def _find_sqlite_databases(self, search_path: str) -> Dict[str, str]:
        """Recursively find SQLite database files"""
        
        logger.info("🔍 Searching for SQLite database files...")
        
        db_paths = {}
        search_dir = Path(search_path)
        
        # Look for .sqlite, .db files and directories containing databases
        for file_path in search_dir.rglob("*"):
            if file_path.is_file():
                # Check file extension
                if file_path.suffix.lower() in ['.sqlite', '.db', '.sqlite3']:
                    db_id = file_path.stem
                    db_paths[db_id] = str(file_path)
                    logger.info(f"Found database: {db_id} at {file_path}")
                
                # Also check if file is a SQLite database by content
                elif file_path.suffix == '' and file_path.stat().st_size > 1024:
                    if self._is_sqlite_file(file_path):
                        db_id = file_path.name
                        db_paths[db_id] = str(file_path)
                        logger.info(f"Found SQLite database: {db_id} at {file_path}")
        
        # Also look for database directories (common BIRD structure)
        db_dirs = list(search_dir.rglob("*database*"))
        for db_dir in db_dirs:
            if db_dir.is_dir():
                logger.info(f"Found database directory: {db_dir}")
                # Look for SQLite files in database directories
                for db_file in db_dir.rglob("*.sqlite"):
                    if db_file.is_file():
                        db_id = f"{db_dir.name}_{db_file.stem}"
                        db_paths[db_id] = str(db_file)
                        logger.info(f"Found database in directory: {db_id}")
        
        logger.info(f"✅ Found {len(db_paths)} SQLite database files")
        return db_paths
    
    def _is_sqlite_file(self, file_path: Path) -> bool:
        """Check if a file is a SQLite database by reading its header"""
        try:
            with open(file_path, 'rb') as f:
                header = f.read(16)
                return header.startswith(b'SQLite format 3')
        except:
            return False
    
    def _copy_databases_to_final_location(self, db_paths: Dict[str, str]) -> Dict[str, str]:
        """Copy database files to the final download directory"""
        
        logger.info("📁 Copying databases to final location...")
        
        final_paths = {}
        
        for db_id, temp_path in db_paths.items():
            # Create a clean filename
            clean_db_id = "".join(c for c in db_id if c.isalnum() or c in '_-')
            final_filename = f"{clean_db_id}.sqlite"
            final_path = self.download_dir / final_filename
            
            try:
                shutil.copy2(temp_path, final_path)
                final_paths[db_id] = str(final_path)
                
                # Get file size
                size_mb = final_path.stat().st_size / (1024 * 1024)
                logger.info(f"✅ Copied {db_id} ({size_mb:.1f}MB) -> {final_filename}")
                
            except Exception as e:
                logger.error(f"❌ Failed to copy {db_id}: {e}")
        
        logger.info(f"✅ Successfully copied {len(final_paths)} databases to {self.download_dir}")
        return final_paths
    
    def _show_manual_instructions(self):
        """Show manual download instructions"""
        logger.info("\n" + "="*60)
        logger.info("📋 MANUAL DOWNLOAD INSTRUCTIONS")
        logger.info("="*60)
        logger.info("Since automatic download failed, please manually download:")
        logger.info("")
        logger.info("Option 1 - Direct Download:")
        logger.info("  🔗 https://bird-bench.oss-cn-beijing.aliyuncs.com/minidev.zip")
        logger.info("")
        logger.info("Option 2 - Google Drive:")
        logger.info("  🔗 https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view?usp=sharing")
        logger.info("")
        logger.info("After download:")
        logger.info(f"  1. Extract the ZIP file")
        logger.info(f"  2. Copy SQLite files to: {self.download_dir}")
        logger.info(f"  3. Re-run the Snowflake loader")
        logger.info("="*60)
    
    def list_downloaded_databases(self) -> Dict[str, Dict]:
        """List all downloaded databases with metadata"""
        
        logger.info("📊 Listing downloaded databases...")
        
        databases = {}
        
        for db_file in self.download_dir.glob("*.sqlite"):
            db_id = db_file.stem
            
            try:
                # Get basic file info
                size_mb = db_file.stat().st_size / (1024 * 1024)
                
                # Try to get table info from SQLite
                table_count = 0
                total_rows = 0
                
                try:
                    import sqlite3
                    conn = sqlite3.connect(db_file)
                    cursor = conn.cursor()
                    
                    # Get table count
                    cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                    table_count = cursor.fetchone()[0]
                    
                    # Get total rows across all tables
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = cursor.fetchall()
                    
                    for (table_name,) in tables:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                            total_rows += cursor.fetchone()[0]
                        except:
                            pass
                    
                    conn.close()
                    
                except Exception as e:
                    logger.warning(f"Could not analyze {db_id}: {e}")
                
                databases[db_id] = {
                    'file_path': str(db_file),
                    'size_mb': round(size_mb, 2),
                    'table_count': table_count,
                    'total_rows': total_rows
                }
                
                logger.info(f"  📦 {db_id}: {size_mb:.1f}MB, {table_count} tables, {total_rows:,} rows")
                
            except Exception as e:
                logger.error(f"❌ Error analyzing {db_id}: {e}")
        
        return databases
    
    def cleanup(self):
        """Clean up temporary files"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            logger.info("🧹 Cleaned up temporary files")

def main():
    """Main function"""
    
    print("🗄️ BIRD SQLite Database Downloader")
    print("=" * 50)
    
    # Create downloader
    downloader = BirdDatabaseDownloader()
    
    try:
        # Download databases
        db_paths = downloader.download_databases()
        
        if db_paths:
            print(f"\n✅ Successfully downloaded {len(db_paths)} databases!")
            
            # List downloaded databases
            databases = downloader.list_downloaded_databases()
            
            print(f"\n📊 Database Summary:")
            total_size = sum(db['size_mb'] for db in databases.values())
            total_tables = sum(db['table_count'] for db in databases.values())
            total_rows = sum(db['total_rows'] for db in databases.values())
            
            print(f"  Total databases: {len(databases)}")
            print(f"  Total size: {total_size:.1f}MB")
            print(f"  Total tables: {total_tables}")
            print(f"  Total rows: {total_rows:,}")
            
            print(f"\n💡 Ready to load into Snowflake!")
            print(f"💡 Run: python snowflake_bird_full_loader.py")
            
        else:
            print("\n❌ No databases were downloaded")
            print("💡 Try the manual download instructions above")
        
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        return 1
    
    finally:
        downloader.cleanup()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())