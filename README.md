# COVID-19 Data Engineering Project

A complete data engineering pipeline that extracts COVID-19 data from Our World in Data, processes it using PySpark, and loads it into PostgreSQL.

## Project Overview

This project demonstrates a typical ETL (Extract, Transform, Load) pipeline for COVID-19 data analysis:

- **Extract**: Downloads COVID-19 data from Our World in Data (OWID) public dataset
- **Transform**: Uses Pandas to clean, filter, and calculate rolling averages
- **Load**: Stores processed data in PostgreSQL database

## Data Source

The project uses the [Our World in Data COVID-19 dataset](https://ourworldindata.org/covid-19), which provides comprehensive COVID-19 statistics globally.

## Features

- Downloads latest COVID-19 data automatically
- Filters data for three countries: Nepal, India, and USA
- Removes rows with missing values
- Calculates 7-day rolling average of new cases
- Stores processed data in PostgreSQL
- Exports cleaned data as CSV files
- Docker support for containerized environment

## Project Structure

```
DE/
├── requirements.txt          # Python dependencies
├── README.md                # Project documentation
├── .env.example             # Environment variables template
├── docker-compose.yml       # Docker configuration
├── Dockerfile              # Docker image definition
├── scripts/
│   ├── setup_database.py   # PostgreSQL database setup
│   └── download_data.py    # Data download utility
├── src/
│   ├── __init__.py
│   ├── config.py           # Configuration settings
│   ├── database.py         # Database connection utilities
│   └── etl_pipeline.py     # Main ETL pipeline
├── data/
│   ├── raw/                # Raw downloaded data
│   └── processed/          # Processed CSV files
└── logs/                   # Application logs
```

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Java 8+ (required for PySpark)
- Docker (optional, for containerized setup)

## Installation

### Option 1: Local Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd DE
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

5. Set up PostgreSQL database:
```bash
python scripts/setup_database.py
```

### Option 2: Docker Setup

1. Build and run with Docker Compose:
```bash
docker-compose up --build
```

## Usage

### Running the ETL Pipeline

1. Download the latest COVID-19 data:
```bash
python scripts/download_data.py
```

2. Run the main ETL pipeline:
```bash
python src/etl_pipeline.py
```

### Expected Output

The pipeline will:
- Download COVID-19 data to `data/raw/`
- Process data using PySpark
- Generate cleaned CSV files in `data/processed/`
- Insert data into PostgreSQL tables

### Sample Output

The processed data will include:
- `covid_processed.csv`: Cleaned data with rolling averages
- PostgreSQL table `covid_data` with columns:
  - `date`: Date of the record
  - `country`: Country name
  - `new_cases`: Daily new cases
  - `total_cases`: Cumulative total cases
  - `new_cases_7day_avg`: 7-day rolling average of new cases

## Configuration

Edit `src/config.py` to modify:
- Target countries
- Date range filters
- Database connection settings
- File paths

## Database Schema

```sql
CREATE TABLE covid_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    country VARCHAR(100) NOT NULL,
    new_cases INTEGER,
    total_cases INTEGER,
    new_cases_7day_avg DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Monitoring and Logging

- Application logs are stored in `logs/`
- Database connection status is logged
- Data processing statistics are displayed

## Troubleshooting


### Debug Mode

Run with debug logging:
```bash
python src/etl_pipeline.py --debug
```

## Acknowledgments

- [Our World in Data](https://ourworldindata.org/) for providing the COVID-19 dataset
- Apache Spark community for the PySpark framework 
