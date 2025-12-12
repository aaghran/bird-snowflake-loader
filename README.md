# BIRD Dataset Snowflake Loader

A comprehensive loader for the BIRD (BIg Bench for LaRge-scale Database Grounded Text-to-SQL Evaluation) dataset into Snowflake for benchmarking text-to-SQL solutions.

## 🎯 Features

- ✅ **Complete Dataset Loading** - Loads all BIRD databases with full data (no sampling)
- ✅ **Schema Analysis** - Extracts table structures, columns, foreign keys, and relationships  
- ✅ **Question Mapping** - Links questions to their corresponding databases
- ✅ **Automatic Download** - Downloads SQLite databases from official BIRD sources
- ✅ **Flexible Configuration** - Environment variables or config file setup
- ✅ **Progress Tracking** - Comprehensive logging and validation

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Snowflake Credentials

**Option A: Environment Variables**
```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"  
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_ROLE="your_role"
```

**Option B: Config File**
```bash
# Copy template and edit with your credentials
cp snowflake_config.template.json snowflake_config.json
```

### 3. Download BIRD Databases

```bash
python download_bird_databases.py
```

### 4. Load Complete Dataset

```bash
python snowflake_bird_full_loader.py
```

## 📊 Dataset Overview

After loading, you'll have:

- **500 BIRD Questions** - Text-to-SQL pairs with evidence and difficulty ratings
- **22 Databases** - Complete SQLite databases covering multiple domains
- **158+ Tables** - All tables with complete data (no sampling)
- **1,600+ Schema Records** - Complete column metadata and types
- **200+ Foreign Key Relationships** - Table relationship mappings

### Key Tables Created

| Table | Description |
|-------|-------------|
| `BIRD_QUESTIONS` | All questions with SQL queries and metadata |
| `BIRD_DATABASES` | Database metadata and domain information |
| `BIRD_TABLE_SCHEMAS` | Complete table and column definitions |
| `BIRD_FOREIGN_KEYS` | Foreign key relationships |
| `BIRD_TABLE_DATA_INFO` | Data loading status and row counts |
| `BIRD_*` | Actual data tables (e.g., `BIRD_CARD_GAMES_CARDS`) |

## 💡 Usage Examples

### Query Questions by Database
```sql
SELECT db_id, question, sql_query 
FROM bird_questions 
WHERE db_id = 'card_games'
LIMIT 5;
```

### Check Data Availability
```sql
SELECT db_id, COUNT(*) as table_count, SUM(row_count) as total_rows
FROM bird_table_data_info 
WHERE data_loaded = TRUE
GROUP BY db_id
ORDER BY total_rows DESC;
```

### Test Benchmark Query
```sql
-- Example: Run a BIRD question against actual data
SELECT name FROM BIRD_CARD_GAMES_CARDS 
WHERE name IN ('Serra Angel', 'Shrine Keeper') 
ORDER BY convertedManaCost DESC 
LIMIT 1;
```