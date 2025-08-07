"""
Create sample COVID-19 data for testing the ETL pipeline.
This is useful when network access is not available.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

def create_sample_covid_data():
    """Create sample COVID-19 data for testing."""
    
    # Create date range
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 12, 31)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Countries to include
    countries = ['Nepal', 'India', 'United States']
    
    # Create sample data
    data = []
    
    for country in countries:
        # Generate realistic COVID-19 data patterns
        base_cases = random.randint(10, 100)
        
        for date in dates:
            # Add some randomness and trends
            trend_factor = 1 + 0.1 * np.sin((date - start_date).days / 365 * 2 * np.pi)
            random_factor = random.uniform(0.5, 1.5)
            
            new_cases = max(0, int(base_cases * trend_factor * random_factor))
            
            # Calculate cumulative total (simplified)
            if len(data) > 0 and data[-1]['location'] == country:
                total_cases = data[-1]['total_cases'] + new_cases
            else:
                total_cases = new_cases
            
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'location': country,
                'new_cases': new_cases,
                'total_cases': total_cases,
                'new_cases_smoothed': int(new_cases * 0.8),  # Simulate smoothed data
                'total_cases_per_million': total_cases * 1000,  # Simulate per million
                'new_cases_per_million': new_cases * 1000,
                'new_cases_smoothed_per_million': int(new_cases * 0.8 * 1000),
                'reproduction_rate': random.uniform(0.8, 1.2),
                'icu_patients': random.randint(0, 100),
                'hosp_patients': random.randint(0, 500),
                'weekly_icu_admissions': random.randint(0, 50),
                'weekly_hosp_admissions': random.randint(0, 200),
                'total_tests': random.randint(1000, 10000),
                'new_tests': random.randint(100, 1000),
                'total_tests_per_thousand': random.uniform(1, 10),
                'new_tests_per_thousand': random.uniform(0.1, 1),
                'positive_rate': random.uniform(0.01, 0.2),
                'tests_per_case': random.uniform(5, 50),
                'people_vaccinated': random.randint(0, 1000000),
                'people_fully_vaccinated': random.randint(0, 800000),
                'total_boosters': random.randint(0, 500000),
                'new_vaccinations': random.randint(0, 10000),
                'new_vaccinations_smoothed': random.randint(0, 8000),
                'total_vaccinations_per_hundred': random.uniform(0, 80),
                'people_vaccinated_per_hundred': random.uniform(0, 70),
                'people_fully_vaccinated_per_hundred': random.uniform(0, 60),
                'total_boosters_per_hundred': random.uniform(0, 30),
                'new_vaccinations_smoothed_per_million': random.uniform(0, 1000),
                'stringency_index': random.uniform(0, 100),
                'population': random.randint(1000000, 1000000000),
                'population_density': random.uniform(10, 1000),
                'median_age': random.uniform(20, 45),
                'aged_65_older': random.uniform(5, 25),
                'aged_70_older': random.uniform(3, 20),
                'gdp_per_capita': random.uniform(1000, 50000),
                'extreme_poverty': random.uniform(0, 30),
                'cardiovasc_death_rate': random.uniform(100, 500),
                'diabetes_prevalence': random.uniform(5, 15),
                'female_smokers': random.uniform(5, 30),
                'male_smokers': random.uniform(10, 40),
                'handwashing_facilities': random.uniform(20, 100),
                'hospital_beds_per_thousand': random.uniform(1, 10),
                'life_expectancy': random.uniform(60, 85),
                'human_development_index': random.uniform(0.4, 1.0)
            })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Ensure data directory exists
    os.makedirs('data/raw', exist_ok=True)
    
    # Save to CSV
    output_path = 'data/raw/owid-covid-data.csv'
    df.to_csv(output_path, index=False)
    
    print(f"Sample COVID-19 data created: {output_path}")
    print(f"Total rows: {len(df)}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Countries: {df['location'].unique()}")
    print(f"Columns: {list(df.columns)}")
    
    return output_path

if __name__ == "__main__":
    create_sample_covid_data() 