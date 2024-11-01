import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Database connection string
CONNECTION = 'postgres://tsdbadmin:odwkmipgaq1b34s2@i9jqhhswdl.l9ey76f5h9.tsdb.cloud.timescale.com:39127/tsdb?sslmode=require'

def create_connection():
    try:
        conn = psycopg2.connect(CONNECTION)
        return conn
    except Exception as e:
        print("Error while connecting to the database:", e)
        return None

def create_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS neasted_data (
        time TIMESTAMPTZ NOT NULL,   -- Timestamp for TimescaleDB compatibility
        instrument_token BIGINT,
        exchange_token BIGINT,
        tradingsymbol TEXT PRIMARY KEY,
        name TEXT,
        last_price FLOAT,
        expiry DATE,
        strike FLOAT,
        tick_size FLOAT,
        lot_size INT,
        instrument_type TEXT,
        segment TEXT,
        exchange TEXT
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()
        print("Table neasted_data created successfully.")

# def insert_data(conn, data_frame):
#     # Ensure that the 'expiry' column is converted to datetime format
#     data_frame['expiry'] = pd.to_datetime(data_frame['expiry'], errors='coerce').dt.date

#     # Handle NaN values in 'expiry' column, decide how you want to handle these
#     data_frame = data_frame.dropna(subset=['expiry'])

#     # Create tuples for insertion
#     tuples = [
#         (
#             datetime.now(),  # Using current timestamp for each row
#             row.instrument_token,
#             row.exchange_token,
#             row.tradingsymbol,
#             row.name,
#             row.last_price,
#             row.expiry,
#             row.strike,
#             row.tick_size,
#             row.lot_size,
#             row.instrument_type,
#             row.segment,
#             row.exchange
#         )
#         for row in data_frame.itertuples(index=False)
#     ]

#     insert_query = """
#     INSERT INTO neasted_data (
#         time, instrument_token, exchange_token, tradingsymbol, name, last_price, expiry, strike, tick_size, lot_size, instrument_type, segment, exchange
#     ) VALUES %s
#     ON CONFLICT (tradingsymbol) DO NOTHING;
#     """

#     with conn.cursor() as cursor:
#         execute_values(cursor, insert_query, tuples)
#         conn.commit()
#         print("Data has been successfully inserted into futures_data table.")

import numpy as np

def insert_data(conn, data_frame):
    data_frame['expiry'] = data_frame['expiry'].replace({np.nan: None})

    tuples = [
        (
            datetime.now(),
            row.instrument_token,
            row.exchange_token,
            row.tradingsymbol,
            row.name,
            row.last_price,
            row.expiry,
            row.strike,
            row.tick_size,
            row.lot_size,
            row.instrument_type,
            row.segment,
            row.exchange
        )
        for row in data_frame.itertuples(index=False)
    ]

    insert_query = """
    INSERT INTO neasted_data (
        time, instrument_token, exchange_token, tradingsymbol, name, last_price, expiry, strike, tick_size, lot_size, instrument_type, segment, exchange
    ) VALUES %s
    ON CONFLICT (tradingsymbol) DO NOTHING;
    """

    with conn.cursor() as cursor:
        execute_values(cursor, insert_query, tuples)
        conn.commit()
        print("Data has been successfully inserted into futures_data table.")


def main():
    # Load data from the CSV file
    csv_file_path = './neasted.csv'  # Change this to the path of your CSV file
    df = pd.read_csv(csv_file_path)
    print("CSV data loaded successfully:\n", df.head())

   
    conn = create_connection()
    if conn:
        create_table(conn)
        insert_data(conn, df)
        conn.close()
        print("Connection closed.")
    else:
        print("Failed to create a connection to the database.")

if __name__ == "__main__":
    main()

















































# import pandas as pd
# import psycopg2
# from psycopg2.extras import execute_values
# from datetime import datetime
# import numpy as np

# # Database connection string
# CONNECTION = 'postgres://tsdbadmin:odwkmipgaq1b34s2@i9jqhhswdl.l9ey76f5h9.tsdb.cloud.timescale.com:39127/tsdb?sslmode=require'

# def create_connection():
#     return psycopg2.connect(CONNECTION)

# def create_table(conn):
#     with conn.cursor() as cursor:
#         # First drop existing hypertable and table
#         cursor.execute("""
#         DROP TABLE IF EXISTS futures_data CASCADE;
#         """)
        
#         # Create the regular table
#         cursor.execute("""
#         CREATE TABLE futures_data (
#             time TIMESTAMPTZ NOT NULL,
#             date TIMESTAMPTZ NOT NULL,
#             open FLOAT,
#             high FLOAT,
#             low FLOAT,
#             name TEXT NOT NULL,
#             expiry DATE NOT NULL,
#             strike FLOAT,
#             instrument_type TEXT NOT NULL,
#             tradingsymbol TEXT NOT NULL
#         );
#         """)
        
#         # Convert to hypertable
#         cursor.execute("""
#         SELECT create_hypertable('futures_data', 'time', 
#             if_not_exists => TRUE,
#             migrate_data => TRUE
#         );
#         """)
        
#         # Create compound index including time
#         cursor.execute("""
#         CREATE INDEX idx_futures_time_symbol 
#         ON futures_data (time, tradingsymbol);
#         """)
        
#         conn.commit()
#         print("Table and indexes created successfully.")

# def validate_data(df):
#     """Validate and clean the dataframe"""
#     # Check required columns
#     required_columns = ['date', 'name', 'expiry', 'instrument_type', 'open', 'high', 'low']
#     missing_columns = [col for col in required_columns if col not in df.columns]
#     if missing_columns:
#         raise ValueError(f"Missing required columns: {missing_columns}")
    
#     # Convert date columns
#     df['date'] = pd.to_datetime(df['date'])
#     df['expiry'] = pd.to_datetime(df['expiry'])
    
#     # Drop rows with null values in required fields
#     required_fields = ['name', 'expiry', 'instrument_type']
#     df = df.dropna(subset=required_fields)
    
#     # Fill NaN values for numeric columns
#     df['strike'] = df['strike'].fillna(0)
#     df['open'] = df['open'].fillna(0)
#     df['high'] = df['high'].fillna(0)
#     df['low'] = df['low'].fillna(0)
    
#     return df

# def generate_tradingsymbols(df):
#     """Generate tradingsymbols vectorized"""
#     # Ensure strike is properly formatted
#     df['strike_str'] = df['strike'].apply(lambda x: f"_{int(x)}" if x != 0 else '')
    
#     # Generate tradingsymbol
#     df['tradingsymbol'] = (df['name'].astype(str) + '_' + 
#                           df['expiry'].dt.strftime('%Y%m%d') + 
#                           df['strike_str'] + '_' + 
#                           df['instrument_type'].astype(str))
    
#     return df.drop(['strike_str'], axis=1, errors='ignore')

# def insert_data(conn, df, batch_size=5000):
#     print("Processing data...")
    
#     try:
#         # Validate and clean data
#         df = validate_data(df)
#         print(f"Validated data: {len(df)} rows remain after cleaning")
        
#         # Generate tradingsymbols
#         df = generate_tradingsymbols(df)
        
#         # Add time column
#         df['time'] = datetime.now()
        
#         # Prepare column list
#         columns = ['time', 'date', 'open', 'high', 'low', 'name', 
#                   'expiry', 'strike', 'instrument_type', 'tradingsymbol']
        
#         # Convert DataFrame to list of tuples
#         data_tuples = [tuple(x) for x in df[columns].values]
#         total_rows = len(data_tuples)
#         print(f"Starting insertion of {total_rows} rows...")
        
#         # Batch insert with progress tracking
#         with conn.cursor() as cursor:
#             rows_inserted = 0
#             for i in range(0, total_rows, batch_size):
#                 batch = data_tuples[i:i + batch_size]
#                 execute_values(
#                     cursor,
#                     """
#                     INSERT INTO futures_data (
#                         time, date, open, high, low, name, expiry, 
#                         strike, instrument_type, tradingsymbol
#                     ) VALUES %s;
#                     """,
#                     batch,
#                     page_size=batch_size
#                 )
#                 rows_inserted += len(batch)
#                 print(f"Inserted {rows_inserted}/{total_rows} rows ({(rows_inserted/total_rows*100):.1f}%)")
#                 conn.commit()  # Commit after each batch
        
#         print(f"Completed inserting all {total_rows} rows.")
        
#     except Exception as e:
#         print(f"Error during data processing: {str(e)}")
#         print("Preview of problematic data:")
#         print(df.head())
#         raise

# def main():
#     try:
#         # Load CSV with optimized settings
#         print("Loading CSV file...")
#         df = pd.read_csv('./test.csv')
#         print(f"Loaded {len(df)} rows from CSV")
        
#         # Print sample of data for verification
#         print("\nSample of loaded data:")
#         print(df.head())
#         print("\nColumns in CSV:", df.columns.tolist())
        
#         # Connect and process data
#         with create_connection() as conn:
#             create_table(conn)
#             insert_data(conn, df)
#             print("Processing completed successfully.")
            
#     except Exception as e:
#         print(f"Error: {str(e)}")
#         print("Full error details:", e)

# if __name__ == "__main__":
#     main()


