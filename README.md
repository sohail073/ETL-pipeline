# Cricket Match Data ETL Pipeline

## Project Overview
Real-time ETL pipeline for cricket match data using Python and PostgreSQL. Fetches live data from CricAPI, processes it, and stores it in a PostgreSQL database.

## Technical Stack
- Python 3.8+
- PostgreSQL 12+
- CricAPI
- Libraries: psycopg2-binary, pandas, requests

## Features
```markdown
- Real-time data extraction from CricAPI
- Automated data transformation
- PostgreSQL integration with upsert capability
- Automatic timestamp tracking
- Error handling and logging
- Batch processing support
```

## Installation

### Prerequisites
```bash
# Install Python packages
pip install psycopg2-binary pandas requests

# Create PostgreSQL database
psql -U postgres
CREATE DATABASE cricket_db;
```

### Configuration
```python
db_params = {
    'host': 'your_host',
    'database': 'cricket_db',
    'user': 'your_username',
    'password': 'your_password',
    'port': '5432'
}
```

## Pipeline Components

### Extract
```python
def fetch_and_save_json(api_url: str, filename: str) -> None:
    # Fetches match data from CricAPI
    # Saves raw JSON data
```

### Transform
```python
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # Data cleaning
    # Team name extraction
    # Score formatting
    # Venue parsing
```

### Load
```python
def load_to_postgres(df: pd.DataFrame, db_params: dict) -> Optional[str]:
    # PostgreSQL integration
    # Upsert operations
    # Transaction management
```

## Database Schema
```sql
CREATE TABLE cricket_matches (
    id VARCHAR(100) PRIMARY KEY,
    Team1 VARCHAR(100),
    Team2 VARCHAR(100),
    Match_Number VARCHAR(50),
    matchType VARCHAR(50),
    status VARCHAR(100),
    score_of_team1 VARCHAR(50),
    score_of_team2 VARCHAR(50),
    Venue VARCHAR(200),
    City VARCHAR(100),
    system_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Error Handling
```python
try:
    # Pipeline execution
except Exception as e:
    print(f"Error occurred: {e}")
    print("Will retry in next iteration...")
```

## Performance Features
- Batch processing for database operations
- Connection pooling
- Optimized SQL queries
- Memory management
- 10-second update intervals


## Future Scope
1. Data visualization dashboard
2. Historical analysis
3. Performance optimization
4. Advanced error reporting
5. Add more data of the on going matches 

## Author
Sohil Ansari
- LinkedIn: [Your Profile]
- GitHub: [Your Profile]

## License
MIT License - see LICENSE file


---
**Note**: This project demonstrates ETL pipeline development, real-time data processing, and database integration skills.
