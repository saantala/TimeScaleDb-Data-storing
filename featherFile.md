# Futures Data Insertion Script

## Overview
This Python script is designed to load futures market data from a CSV file and insert it into a TimescaleDB database with efficient batch processing and data validation.

## Features
- Data validation and cleaning
- Batch insertion of large datasets
- Automatic trading symbol generation
- TimescaleDB hypertable creation
- Error handling and logging

## Prerequisites
- Python 3.7+
- Required Python packages:
  - pandas
  - psycopg2
  - numpy

## Installation

1. Clone the repository
```bash
git clone <your-repository-url>
cd <repository-directory>
```

2. Install required dependencies
```bash
pip install pandas psycopg2-binary numpy
```

## Configuration

### Database Connection
Update the `CONNECTION` variable in the script with your TimescaleDB connection details:
```python
CONNECTION = 'postgres://username:password@host:port/database?sslmode=require'
```

### Input Data
Prepare a CSV file named `feather.csv` with the following required columns:
- `date`: Date of the market data
- `name`: Instrument name
- `expiry`: Expiration date
- `instrument_type`: Type of instrument (e.g., 'CE', 'PE')
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price

Optional columns:
- `strike`: Strike price (will be set to 0 if not provided)

## Usage

Run the script:
```bash
python futures_data_insertion.py
```

## Script Workflow
1. Loads data from `feather.csv`
2. Validates and cleans the input data
3. Generates trading symbols
4. Creates a TimescaleDB hypertable
5. Inserts data in batches of 5000 rows
6. Provides progress tracking during insertion

## Database Schema
The script creates a `futures_data` table with the following columns:
- `time`: Insertion timestamp
- `date`: Market data date
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `name`: Instrument name
- `expiry`: Expiration date
- `strike`: Strike price
- `instrument_type`: Instrument type
- `tradingsymbol`: Automatically generated unique identifier

## Error Handling
- Drops rows with missing critical information
- Fills missing numeric columns with 0
- Prints detailed error messages and data preview on failure

## Performance Optimization
- Uses `execute_values` for efficient batch inserts
- Creates a time-based index for faster querying
- Uses TimescaleDB hypertable for improved time-series performance

## Logging
The script provides console output tracking:
- Number of rows loaded
- Data validation results
- Insertion progress
- Completion status

## Customization
Adjust `batch_size` in `insert_data()` function to optimize performance based on your system resources.

## Potential Improvements
- Add command-line argument support for input file
- Implement more sophisticated data validation
- Add logging to file
- Create configuration file for database connection

## License
[Add your license information]

## Contributing
[Add contribution guidelines]
