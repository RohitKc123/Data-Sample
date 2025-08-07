# Quick Start Guide

This guide will help you get the COVID-19 Data Engineering Project running quickly.

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Java 8+ (required for PySpark)
- Docker (optional)

## Option 1: Local Setup (Recommended for Development)

### 1. Clone and Setup

```bash
# Navigate to project directory
cd DE

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy environment template
cp env.example .env

# Edit .env with your PostgreSQL credentials
# Example:
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=covid_data
# DB_USER=postgres
# DB_PASSWORD=your_password
```

### 3. Setup Database

```bash
# Create database and tables
python scripts/setup_database.py
```

### 4. Test Setup

```bash
# Run tests to verify everything is working
python scripts/test_setup.py
```

### 5. Download Data

```bash
# Download COVID-19 dataset
python scripts/download_data.py
```

### 6. Run ETL Pipeline

```bash
# Process data and load into database
python src/etl_pipeline.py
```

## Option 2: Docker Setup (Recommended for Production)

### 1. Build and Run

```bash
# Build and start all services
docker-compose up --build
```

This will:
- Start PostgreSQL database
- Download COVID-19 data
- Run the ETL pipeline
- Start pgAdmin (accessible at http://localhost:5050)

### 2. Access Services

- **PostgreSQL**: localhost:5432
- **pgAdmin**: http://localhost:5050 (admin@covid.com / admin123)

## Verify Results

### Check Database

```sql
-- Connect to PostgreSQL and run:
SELECT COUNT(*) FROM covid_data;
SELECT DISTINCT country FROM covid_data;
SELECT * FROM covid_data ORDER BY date DESC LIMIT 10;
```

### Check Output Files

- Raw data: `data/raw/owid-covid-data.csv`
- Processed data: `data/processed/covid_processed.csv`
- Logs: `logs/etl_pipeline.log`

## Troubleshooting

### Common Issues

1. **Java not found**: Install Java 8+ and set JAVA_HOME
2. **PostgreSQL connection failed**: Check credentials in `.env`
3. **Memory issues**: Reduce Spark memory in `src/config.py`

### Debug Mode

```bash
# Run with debug logging
python src/etl_pipeline.py --debug
```

### Reset Database

```bash
# Clear all data
python -c "from src.database import db_manager; db_manager.connect(); db_manager.clear_table()"
```

## Next Steps

- Modify `src/config.py` to change target countries
- Add more data transformations in `src/etl_pipeline.py`
- Create additional database tables for different metrics
- Set up automated scheduling with cron or Airflow

## Support

- Check logs in `logs/` directory
- Review README.md for detailed documentation
- Run `python scripts/test_setup.py` to diagnose issues 