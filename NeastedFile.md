# TimescaleDB Data Insertion Script

## Overview

This Python script is designed to load data from a CSV file and insert it into a TimescaleDB database, specifically targeting a table named `neasted_files_data`. The script handles data preprocessing, database connection, and bulk insertion with conflict resolution.

## Prerequisites

### Dependencies

- Python 3.7+
- Required Python Libraries:
  - pandas
  - psycopg2
  - numpy

### Database

- TimescaleDB
- PostgreSQL connection details

## Installation

2. Install required dependencies

```bash
pip install pandas psycopg2 numpy
```

## Configuration

### Database Connection

Update the `CONNECTION` variable in the script with your TimescaleDB connection details:

```python
CONNECTION = 'postgres://username:password@host:port/database?sslmode=require'
```

### Input Data

Ensure your CSV file (`neasted.csv`) is located in the same directory as the script or update the `csv_file_path` variable in the `main()` function.

## CSV File Structure

The script expects a CSV with the following columns:

- `instrument_token`
- `exchange_token`
- `tradingsymbol`
- `name`
- `last_price`
- `expiry`
- `strike`
- `tick_size`
- `lot_size`
- `instrument_type`
- `segment`
- `exchange`

## Features

- Connects to TimescaleDB using psycopg2
- Creates table if not exists
- Handles NaN values in the expiry column
- Uses `ON CONFLICT` to avoid duplicate entries
- Timestamps each inserted record
