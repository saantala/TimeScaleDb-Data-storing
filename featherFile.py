



import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import numpy as np


CONNECTION = 'postgres://tsdbadmin:l3bvoa326wbbkroo@i9jqhhswdl.l9ey76f5h9.tsdb.cloud.timescale.com:39127/tsdb?sslmode=require'

def create_connection():
    return psycopg2.connect(CONNECTION)

def create_table(conn):
    with conn.cursor() as cursor:
        
        cursor.execute("""
        DROP TABLE IF EXISTS futures_data CASCADE;
        """)
        
        
        cursor.execute("""
        CREATE TABLE futures_data (
            time TIMESTAMPTZ NOT NULL,
            date TIMESTAMPTZ NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            name TEXT NOT NULL,
            close FLOAT,
            expiry DATE NOT NULL,
            strike FLOAT,
            instrument_type TEXT NOT NULL,
            tradingsymbol TEXT NOT NULL
        );
        """)
        
      
        cursor.execute("""
        SELECT create_hypertable('futures_data', 'time', 
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
        """)
        
       
        cursor.execute("""
        CREATE INDEX idx_futures_time_symbol 
        ON futures_data (time, tradingsymbol);
        """)
        
        conn.commit()
        print("Table and indexes created successfully.")

def validate_data(df):
    """Validate and clean the dataframe"""
    
    required_columns = ['date', 'name', 'expiry', 'instrument_type', 'open', 'high', 'low','close']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    
    df['date'] = pd.to_datetime(df['date'])
    df['expiry'] = pd.to_datetime(df['expiry'])
    
   
    required_fields = ['name', 'expiry', 'instrument_type']
    df = df.dropna(subset=required_fields)
    
   
    df['strike'] = df['strike'].fillna(0)
    df['open'] = df['open'].fillna(0)
    df['high'] = df['high'].fillna(0)
    df['low'] = df['low'].fillna(0)
    
    return df

def generate_tradingsymbols(df):
    """Generate tradingsymbols vectorized"""
    
    df['strike_str'] = df['strike'].apply(lambda x: f"_{int(x)}" if x != 0 else '')
    
   
    df['tradingsymbol'] = (df['name'].astype(str) + '_' + 
                          df['expiry'].dt.strftime('%Y%m%d') + 
                          df['strike_str'] + '_' + 
                          df['instrument_type'].astype(str))
    
    return df.drop(['strike_str'], axis=1, errors='ignore')

def insert_data(conn, df, batch_size=5000):
    print("Processing data...")
    
    try:
        
        df = validate_data(df)
        print(f"Validated data: {len(df)} rows remain after cleaning")
        
        
        df = generate_tradingsymbols(df)
        
        
        df['time'] = datetime.now()
        
        
        columns = ['time', 'date', 'open', 'high','close', 'low', 'name', 
                  'expiry', 'strike', 'instrument_type', 'tradingsymbol']
        
        
        data_tuples = [tuple(x) for x in df[columns].values]
        total_rows = len(data_tuples)
        print(f"Starting insertion of {total_rows} rows...")
        
       
        with conn.cursor() as cursor:
            rows_inserted = 0
            for i in range(0, total_rows, batch_size):
                batch = data_tuples[i:i + batch_size]
                execute_values(
                    cursor,
                    """
                    INSERT INTO futures_data (
                        time, date, open, high,close, low, name, expiry, 
                        strike, instrument_type, tradingsymbol
                    ) VALUES %s;
                    """,
                    batch,
                    page_size=batch_size
                )
                rows_inserted += len(batch)
                print(f"Inserted {rows_inserted}/{total_rows} rows ({(rows_inserted/total_rows*100):.1f}%)")
                conn.commit()  
        
        print(f"Completed inserting all {total_rows} rows.")
        
    except Exception as e:
        print(f"Error during data processing: {str(e)}")
        print("Preview of problematic data:")
        print(df.head())
        raise

def main():
    try:
        
        print("Loading CSV file...")
        df = pd.read_csv('./feather.csv')
        print(f"Loaded {len(df)} rows from CSV")
        
        
        print("\nSample of loaded data:")
        print(df.head())
        print("\nColumns in CSV:", df.columns.tolist())
        
        
        with create_connection() as conn:
            create_table(conn)
            insert_data(conn, df)
            print("Processing completed successfully.")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Full error details:", e)

if __name__ == "__main__":
    main()


