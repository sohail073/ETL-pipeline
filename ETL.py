import requests
import json
import pandas as pd
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from typing import Optional

def fetch_and_save_json(api_url: str, filename: str) -> None:
    response = requests.get(api_url)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")
    
    data = response.json()
    
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def load_json_to_dataframe(filename: str) -> pd.DataFrame:
    with open(filename, 'r') as json_file:
        data = json.load(json_file)
    
    if 'data' in data:
        matches = data['data']
        df = pd.DataFrame(matches)
        
        columns_to_display = ['id', 'name', 'matchType', 'status', 'venue', 'score']
        df = df[columns_to_display]
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        
        return df
    else:
        raise ValueError("Expected key 'data' not found in the JSON data.")

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # Original transform logic remains the same
    df_cleaned = df[df['status'] != 'No result due to rain']

    def split_teams(name):
        teams_part = name.split(',')[0]
        match_info = name.split(',')[1].strip() if len(name.split(',')) > 1 else ""
        
        teams = teams_part.split(' vs ')
        team1 = teams[0].strip()
        team2 = teams[1].strip()
        match_number = match_info.split()[0] if match_info else ""
        
        return pd.Series([team1, team2, match_number])

    df_cleaned[['Team1', 'Team2', 'Match_Number']] = df_cleaned['name'].apply(split_teams)

    def format_score(score_list):
        if not isinstance(score_list, list) or len(score_list) == 0:
            return pd.Series(['', ''])
        
        formatted_scores = []
        for score in score_list:
            runs = score.get('r', 0)
            wickets = score.get('w', 0)
            overs = score.get('o', 0)
            formatted_scores.append(f"{runs}/{wickets}({overs})")
            
        while len(formatted_scores) < 2:
            formatted_scores.append('')
            
        return pd.Series(formatted_scores[:2])

    df_cleaned[['score_of_team1', 'score_of_team2']] = df_cleaned['score'].apply(format_score)

    def split_venue(venue):
        parts = venue.split(',', 1)
        return pd.Series([parts[0].strip(), parts[1].strip()])

    df_cleaned[['Venue', 'City']] = df_cleaned['venue'].apply(split_venue)

    df_cleaned['matchType'] = df_cleaned['matchType'].str.upper()
    df_cleaned['system_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    columns_order = ['id', 'Team1', 'Team2', 'Match_Number', 'matchType', 'status', 
                    'score_of_team1', 'score_of_team2', 'Venue', 'City', 'system_time']
    df_cleaned = df_cleaned[columns_order]

    return df_cleaned

def create_postgres_table(conn) -> None:
    """Create the cricket_matches table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cricket_matches (
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
    
    -- Create or replace function to update timestamp
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';
    
    -- Create trigger if it doesn't exist
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_trigger
            WHERE tgname = 'update_cricket_matches_updated_at'
        ) THEN
            CREATE TRIGGER update_cricket_matches_updated_at
                BEFORE UPDATE ON cricket_matches
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
        END IF;
    END
    $$;
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()

def load_to_postgres(df: pd.DataFrame, db_params: dict) -> Optional[str]:
    """
    Load transformed data to PostgreSQL database using psycopg2.
    Returns error message if failed, None if successful.
    """
    try:
        conn = psycopg2.connect(
            host=db_params['host'],
            database=db_params['database'],
            user=db_params['user'],
            password=db_params['password'],
            port=db_params['port']
        )
        
        # Create table if it doesn't exist
        create_postgres_table(conn)
        
        # Prepare data for insertion
        data_tuples = [tuple(row) for row in df.values]
        
        # Upsert query
        upsert_query = """
        INSERT INTO cricket_matches (
            id, Team1, Team2, Match_Number, matchType, status,
            score_of_team1, score_of_team2, Venue, City, system_time
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            Team1 = EXCLUDED.Team1,
            Team2 = EXCLUDED.Team2,
            Match_Number = EXCLUDED.Match_Number,
            matchType = EXCLUDED.matchType,
            status = EXCLUDED.status,
            score_of_team1 = EXCLUDED.score_of_team1,
            score_of_team2 = EXCLUDED.score_of_team2,
            Venue = EXCLUDED.Venue,
            City = EXCLUDED.City,
            system_time = EXCLUDED.system_time;
        """
        
        # Execute batch insert
        with conn.cursor() as cur:
            execute_batch(cur, upsert_query, data_tuples, page_size=100)
        
        conn.commit()
        conn.close()
        return None
        
    except Exception as e:
        if conn:
            conn.close()
        return f"Error loading data to PostgreSQL: {str(e)}"

def main():
    # Database configuration
    db_params = {
        'host': 'localhost',  # Change as needed
        'database': 'cricket_db',  # Change as needed
        'user': 'postgres',  # Change as needed
        'password': 'sohail@123',  # Change as needed
        'port': '5432'  # Change as needed
    }
    
    api_url = 'https://api.cricapi.com/v1/currentMatches?apikey=96d1640e-6af8-4bc0-b114-38ffee85e021&offset=0'
    filename = 'current_matches.json'
    
    print("Starting live cricket match monitor with PostgreSQL integration...")
    print("Press Ctrl+C to stop the program")
    print("=" * 80)
    
    try:
        while True:
            print(f"\nUpdated at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)
            
            try:
                # Extract
                fetch_and_save_json(api_url, filename)
                df = load_json_to_dataframe(filename)
                print("\nExtracted Cricket Matches Data:")
                print("=" * 80)
                print(df)
                
                # Transform
                df_transformed = transform_data(df)
                print("\nTransformed Cricket Matches Data:")
                print("=" * 80)
                print(df_transformed)
                
                # Load
                error = load_to_postgres(df_transformed, db_params)
                if error:
                    print(f"\nDatabase Error: {error}")
                else:
                    print("\nSuccessfully loaded data to PostgreSQL")
                
            except Exception as e:
                print(f"Error occurred: {e}")
                print("Will retry in next iteration...")
            
            time.sleep(10)  # Wait for 10 seconds before next update
            
    except KeyboardInterrupt:
        print("\nProgram stopped by user")
        print("=" * 80)

if __name__ == "__main__":
    main()