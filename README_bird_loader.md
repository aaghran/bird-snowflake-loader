# BIRD Dataset to Snowflake Loader

This script loads the BIRD SQL dataset (BIg Bench for LaRge-scale Database Grounded Text-to-SQL Evaluation) into Snowflake for analysis and research.

## Dataset Information

BIRD is a comprehensive text-to-SQL benchmark containing:
- **12,751** text-to-SQL pairs
- **95** databases covering **37** professional domains
- **33.4 GB** total size
- Focus on database values and real-world complexity

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Snowflake Credentials

**Option A: Environment Variables**
```bash
export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="BIRD_DB"
export SNOWFLAKE_SCHEMA="PUBLIC"
```

**Option B: Configuration File**
```bash
cp snowflake_config.json.example snowflake_config.json
# Edit snowflake_config.json with your credentials
```

### 3. Run the Loader

```bash
python snowflake_bird_loader.py
```

## Database Schema

The loader creates the following tables:

### `bird_questions`
- `id`: Unique question identifier
- `db_id`: Source database identifier
- `question`: Natural language question
- `evidence`: External knowledge evidence
- `sql_query`: Gold standard SQL query
- `difficulty`: Question difficulty level
- `domain`: Professional domain (finance, healthcare, etc.)

### `bird_databases`
- `db_id`: Database identifier
- `db_name`: Database name
- `domain`: Professional domain
- `num_tables`: Number of tables
- `description`: Database description
- `size_mb`: Database size in MB

### `bird_tables`
- `id`: Table identifier
- `db_id`: Parent database
- `table_name`: Table name
- `column_names`: Array of column names
- `column_types`: Array of column types
- `primary_keys`: Array of primary keys
- `foreign_keys`: Array of foreign keys

### `bird_execution_results`
- `question_id`: Reference to question
- `execution_time_ms`: Query execution time
- `result_rows`: Number of result rows
- `execution_success`: Success flag
- `error_message`: Error details if failed

## Analysis Views

The loader creates helpful views for analysis:

### `bird_summary`
Domain-level statistics with question counts and complexity metrics.

### `complex_questions`
Questions with high difficulty or complex SQL queries.

### `domain_complexity`
Analysis of SQL complexity patterns by domain.

## Usage Examples

After loading, you can analyze the data:

```sql
-- Get overview by domain
SELECT * FROM bird_summary;

-- Find most complex questions
SELECT question, sql_query, difficulty 
FROM complex_questions 
LIMIT 10;

-- Analyze join patterns
SELECT domain, join_questions, total_questions,
       (join_questions::FLOAT / total_questions) * 100 as join_percentage
FROM domain_complexity
ORDER BY join_percentage DESC;

-- Sample questions for a domain
SELECT question, sql_query 
FROM bird_questions 
WHERE domain = 'financial' 
LIMIT 5;
```

## Features

- **Automatic download** from HuggingFace datasets
- **Robust error handling** with detailed logging
- **Data validation** after loading
- **Flexible configuration** via env vars or config file
- **Analysis views** for immediate insights
- **Domain classification** for organizational analysis

## Troubleshooting

### Connection Issues
- Verify your Snowflake account URL format
- Check that your user has necessary permissions
- Ensure warehouse is running

### Dataset Download Issues
- Check internet connectivity
- Verify HuggingFace datasets library is installed
- Some corporate firewalls may block HuggingFace

### Memory Issues
- The dataset is large; ensure sufficient RAM
- Consider processing in batches for very large datasets

## License

This loader script is provided as-is. The BIRD dataset follows CC BY-SA 4.0 license.
For dataset questions, contact: bird.bench25@gmail.com