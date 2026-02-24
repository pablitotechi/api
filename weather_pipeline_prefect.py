"""
Weather Pipeline Orchestration with Prefect

This module defines a Prefect flow that orchestrates the complete weather ETL pipeline
with scheduling support. It wraps the existing weather_pipeline.py functions as Prefect tasks.

Usage:
    # Run once (with defaults)
    python weather_pipeline_prefect.py

    # Schedule via Prefect UI or agent
    prefect deployment build weather_pipeline_prefect.py:weather_etl_flow -n "weather-daily" -q "default"
    prefect deployment apply weather_etl_flow-deployment.yaml
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from prefect import flow, task

# Import existing pipeline functions
from weather_pipeline import (
    Location,
    fetch_hourly_weather,
    get_lat_lon,
    get_mongo_client,
    load_to_mongo,
    to_staging_df,
    transform,
)

# Load environment
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

DEFAULT_CITY = os.getenv("DEFAULT_CITY", "San Jose")
DEFAULT_COUNTRY_CODE = os.getenv("DEFAULT_COUNTRY_CODE", "CR")
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "America/Costa_Rica")

# Scheduling: Run daily at 2:00 AM UTC (configurable via environment)
SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 2 * * *")  # 2:00 AM UTC


# ============================================================================
# PREFECT TASKS - Wrap existing pipeline functions with observability
# ============================================================================


@task(name="geocoding-task", retries=2, retry_delay_seconds=30)
def task_geocode(city: str, country_code: str) -> Location:
    """
    Task: Resolve city name to coordinates using Open-Meteo Geocoding API.
    
    Args:
        city: City name (e.g., "San Jose")
        country_code: ISO 3166-1 alpha-2 country code (e.g., "CR")
    
    Returns:
        Location dataclass with validated coordinates
    """
    print(f"🌍 Geocoding: {city}, {country_code}")
    location = get_lat_lon(city, country_code)
    print(f"✅ Found: {location.location_name} ({location.latitude}, {location.longitude})")
    return location


@task(name="forecast-extraction-task", retries=2, retry_delay_seconds=30)
def task_fetch_forecast(
    latitude: float,
    longitude: float,
    timezone_name: str,
    hourly_vars: str = "temperature_2m,relative_humidity_2m,precipitation,windspeed_10m",
) -> Dict[str, Any]:
    """
    Task: Fetch hourly weather forecast from Open-Meteo Forecast API.
    
    Args:
        latitude: Decimal latitude
        longitude: Decimal longitude
        timezone_name: IANA timezone string (e.g., "America/Costa_Rica")
        hourly_vars: Comma-separated list of forecast variables
    
    Returns:
        Raw JSON response from Open-Meteo API
    """
    print(f"📡 Fetching forecast for lat={latitude}, lon={longitude}")
    weather_json = fetch_hourly_weather(latitude, longitude, timezone_name, hourly_vars)
    hourly_data = weather_json.get("hourly", {})
    record_count = len(hourly_data.get("time", []))
    print(f"✅ Forecast: {record_count} hourly records")
    return weather_json


@task(name="staging-task")
def task_stage(weather_json: Dict[str, Any], location: Location, timezone_name: str):
    """
    Task: Convert raw JSON response to pandas DataFrame with metadata.
    
    Args:
        weather_json: Response from Open-Meteo Forecast API
        location: Location dataclass with city/country/coordinates
        timezone_name: IANA timezone string
    
    Returns:
        Staging DataFrame with raw + metadata columns
    """
    print("📋 Staging: Converting JSON to DataFrame")
    df = to_staging_df(weather_json, location, timezone_name)
    print(f"✅ Staging: {len(df)} rows")
    return df


@task(name="transform-task")
def task_transform(staging_df):
    """
    Task: Clean data, apply type conversions, add feature columns.
    
    Args:
        staging_df: Raw staging DataFrame
    
    Returns:
        Transformed DataFrame ready for MongoDB
    """
    print("🔧 Transform: Cleaning and feature engineering")
    curated_df = transform(staging_df)
    print(f"✅ Transform: {len(curated_df)} valid records")
    return curated_df


@task(name="load-to-mongo-task", retries=1, retry_delay_seconds=60)
def task_load(transformed_df):
    """
    Task: Upsert transformed data into MongoDB with idempotent indexes.
    
    Args:
        transformed_df: Cleaned DataFrame from transform stage
    
    Returns:
        Dictionary with upsert statistics (matched, modified, upserted)
    """
    print("💾 Load: Upserting to MongoDB")
    result = load_to_mongo(transformed_df)
    print(f"✅ MongoDB upsert: {result}")
    return result


# ============================================================================
# PREFECT FLOW - Orchestrate the complete ETL pipeline
# ============================================================================


@flow(
    name="weather-etl-flow",
    description="Complete weather data ETL: geocode → forecast → stage → transform → load",
)
def weather_etl_flow(
    city: str = DEFAULT_CITY,
    country_code: str = DEFAULT_COUNTRY_CODE,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> Dict[str, Any]:
    """
    Main Prefect Flow: Orchestrates the complete weather ETL pipeline.
    
    This flow:
    1. Geocodes the city to get coordinates
    2. Fetches hourly weather forecast
    3. Stages data into a DataFrame with metadata
    4. Transforms and cleans the data
    5. Loads (upserts) into MongoDB
    
    Args:
        city: City name (default from .env or "San Jose")
        country_code: ISO country code (default from .env or "CR")
        timezone_name: IANA timezone (default from .env or "America/Costa_Rica")
    
    Returns:
        MongoDB upsert result with statistics
    
    Example:
        result = weather_etl_flow(city="Madrid", country_code="ES")
    """
    print(f"\n{'='*70}")
    print(f"🚀 Starting Weather ETL Flow at {datetime.now(timezone.utc)}")
    print(f"{'='*70}")
    print(f"   city={city}, country_code={country_code}, timezone={timezone_name}")
    print(f"{'='*70}\n")

    # Task 1: Geocoding
    location = task_geocode(city, country_code)

    # Validate security (re-check country_code matches)
    if location.country_code.upper() != country_code.upper():
        raise ValueError(
            f"❌ Security check failed: geocoding country_code mismatch. "
            f"Expected={country_code}, got={location.country_code}"
        )

    # Task 2: Fetch Forecast
    weather_json = task_fetch_forecast(
        location.latitude,
        location.longitude,
        timezone_name,
    )

    # Task 3: Staging
    staging_df = task_stage(weather_json, location, timezone_name)

    # Task 4: Transform
    transformed_df = task_transform(staging_df)

    # Task 5: Load to MongoDB
    result = task_load(transformed_df)

    print(f"\n{'='*70}")
    print(f"✅ Weather ETL Flow completed successfully")
    print(f"{'='*70}\n")

    return result


# ============================================================================
# SCHEDULED FLOW - Deployable variant with cron scheduling
# ============================================================================


@flow(
    name="weather-etl-scheduled",
    description="Scheduled weather ETL (runs daily at configured time)",
)
def weather_etl_scheduled():
    """
    Scheduled flow that runs the weather ETL with default parameters.
    Configured to run at SCHEDULE_CRON (default: 2:00 AM UTC).
    
    To deploy this flow with scheduling:
    
    ```bash
    prefect deployment build weather_pipeline_prefect.py:weather_etl_scheduled \\
      -n "weather-daily-cr" \\
      -q "default" \\
      --cron "0 2 * * *"
    
    prefect deployment apply weather_etl_scheduled-deployment.yaml
    prefect agent start -q default
    ```
    """
    return weather_etl_flow(
        city=DEFAULT_CITY,
        country_code=DEFAULT_COUNTRY_CODE,
        timezone_name=DEFAULT_TIMEZONE,
    )


# ============================================================================
# CLI / ENTRYPOINT
# ============================================================================


if __name__ == "__main__":
    """
    Run the weather ETL flow immediately (for testing/debugging).
    
    To run with custom parameters:
    >>> from weather_pipeline_prefect import weather_etl_flow
    >>> weather_etl_flow(city="Madrid", country_code="ES", timezone_name="Europe/Madrid")
    """
    print(f"ℹ️  Running in immediate mode (not scheduled)")
    print(f"ℹ️  To enable scheduling, use: prefect deployment build weather_pipeline_prefect.py:weather_etl_scheduled")
    
    result = weather_etl_flow()
    print(f"\n📊 Final Result: {result}")
