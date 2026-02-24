# Copilot Instructions - Weather Pipeline API

## Architecture Overview
This project is a **weather data ETL pipeline** that fetches weather forecasts and stores them in MongoDB. It follows a **5-stage data pipeline pattern** with clear separation of concerns:

1. **EXTRACT (Geocoding)** → Resolve city name to lat/lon using Open-Meteo Geocoding API
2. **EXTRACT (Forecast)** → Fetch hourly weather data from Open-Meteo Forecast API
3. **STAGE** → Convert JSON response to pandas DataFrame with metadata (location, timezone)
4. **TRANSFORM** → Clean data, add feature columns, drop invalid rows, normalize for Mongo
5. **LOAD** → Upsert documents to MongoDB with idempotent unique indexes

## Key Files and Components

- **weather_pipeline.py** (~270 lines) – Core ETL functions organized in 5 stages: extract (geocoding), extract (forecast), stage, transform, and load
- **weather_pipeline_prefect.py** (~250 lines) – Prefect flow orchestration layer; wraps pipeline functions as tasks with scheduling, retries, and observability
- **mongo_test.py** – Connectivity verification script (tests TLS/certification)
- **.env** – Configuration (MONGO_URI, DB_NAME, COLLECTION_NAME, DEFAULT_CITY, SCHEDULE_CRON, etc.)
- **.venv** – Python virtual environment

## Project-Specific Patterns

### 1. Environment-First Configuration
- **All** external parameters loaded via `.env` or defaults (see lines 18-28 in weather_pipeline.py)
- No hardcoded credentials; `MONGO_URI` is mandatory (`if not MONGO_URI: raise ValueError`)
- Defaults: `DEFAULT_CITY="San Jose"`, `DEFAULT_COUNTRY_CODE="CR"`, `DEFAULT_TIMEZONE="America/Costa_Rica"`

### 2. Dataclass for Location Immutability
```python
@dataclass(frozen=True)
class Location:
    city_input: str
    country_code: str
    location_name: str
    latitude: float
    longitude: float
```
Use this pattern for validated, immutable data objects to avoid side effects in data transformations.

### 3. Geocoding Validation (Security-First)
The `get_lat_lon()` function (lines 47-97) prioritizes **correctness over convenience**:
- Filters by exact country_code match (not just availability)
- Raises `ValueError` for ambiguous results rather than guessing
- Security check in `run_pipeline()` (line 239) re-validates: `if loc.country_code.upper() != country_code.upper()`
- **Pattern**: Always validate external API responses, especially for location/coordinate data

### 4. MongoDB Upsert with Idempotent Unique Index
```python
col.create_index([("location_name", ASCENDING), ("time", ASCENDING)], unique=True)
ops.append(UpdateOne(key, {"$set": d}, upsert=True))
```
- Composite unique index on `(location_name, time)` prevents duplicates
- `UpdateOne` with `upsert=True` ensures re-running pipeline doesn't duplicate data
- **Pattern**: Always ensure indexes before bulk writes; use `upsert=True` for incremental loads

### 5. PyMongo TLS Configuration
Both scripts use explicit TLS setup:
```python
MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where(), 
            serverSelectionTimeoutMS=8000, connectTimeoutMS=8000)
```
- Required for MongoDB Atlas; timeouts prevent hanging on network issues
- Use `certifi.where()` to automatically resolve CA bundle (macOS/Linux compatible)
- Always include timeout parameters (8s default in this codebase)

### 6. Pandas DataFrame Conventions
- **Staging stage**: Raw data + metadata columns (location_name, city_input, country_code, latitude, longitude, timezone_name, source, ingested_at_utc)
- **Transform stage**: Clean types with `.astype()`, drop invalid rows on critical columns (`dropna(subset=["time", "temperature_2m"])`)
- **Mongo conversion**: Convert `pd.Timestamp` to Python `datetime` before insert
- **NaN handling**: Replace with `None` for Mongo compatibility (`df.where(pd.notnull(df), None)`)

### 7. API Response Chains
- **Geocoding API** (Open-Meteo): Returns `results` array; extract with `.get("results") or []`
- **Forecast API**: Returns nested `{"hourly": {time: [], temperature_2m: [], ...}}`
- **Pattern**: Always use `.get()` with defaults; validate presence before iterating

### 8. Error Handling Style
- Use `raise ValueError(...)` for data validation failures (not logging silently)
- Use `r.raise_for_status()` after HTTP requests
- Print debug statements with emoji prefixes for visual clarity: `print("🚀 Iniciando...")`, `print("❌ Error")`

## Developer Workflows

### Running the ETL Pipeline (Direct)
```bash
source .venv/bin/activate
python weather_pipeline.py  # Runs with DEFAULT_CITY="San Jose", DEFAULT_COUNTRY_CODE="CR"
```

### Running with Prefect (Immediate)
```bash
source .venv/bin/activate
python weather_pipeline_prefect.py  # Same as above, but with Prefect task observability
```

### Testing Mongo Connectivity
```bash
source .venv/bin/activate
python mongo_test.py
```

### Deploying Scheduled Flow with Prefect
```bash
# Build deployment with cron schedule (2:00 AM UTC by default)
prefect deployment build weather_pipeline_prefect.py:weather_etl_scheduled \
  -n "weather-daily-cr" \
  -q "default" \
  --cron "0 2 * * *"

# Apply deployment
prefect deployment apply weather_etl_scheduled-deployment.yaml

# Start agent to execute scheduled flows
prefect agent start -q default
```

### Custom Pipeline Run
```python
from weather_pipeline import run_pipeline
run_pipeline(city="Madrid", country_code="ES", timezone_name="Europe/Madrid")

# Or with Prefect:
from weather_pipeline_prefect import weather_etl_flow
weather_etl_flow(city="Madrid", country_code="ES", timezone_name="Europe/Madrid")
```

## Integration Points

- **Open-Meteo APIs** (no auth required, but respect rate limits)
  - Geocoding: `https://geocoding-api.open-meteo.com/v1/search`
  - Forecast: `https://api.open-meteo.com/v1/forecast`
- **MongoDB Atlas** (TLS/SSL required; connection pooling via MongoClient)
  - DB: `clima_data` (configurable)
  - Collection: `clima_data` (configurable)
  - Indexes: Unique on (location_name, time); secondary on (date, city_input)
- **Prefect** (workflow orchestration and scheduling)
  - Tasks wrap existing pipeline functions with retries and observability
  - Flow: `weather_etl_flow()` – runs one iteration; `weather_etl_scheduled()` – scheduled variant
  - Deployment scheduling via cron expressions (configurable via `SCHEDULE_CRON` in .env)

## When Adding New Features

1. **New data sources**: Add extract function, stage to DataFrame with metadata columns, transform, then upsert
2. **New fields in forecast**: Add to `hourly_vars` parameter in `fetch_hourly_weather()`, ensure transform includes type conversion
3. **New location logic**: Extend `Location` dataclass and `get_lat_lon()` validation
4. **Custom business logic**: Add before `load_to_mongo()`, after transform
5. **Scheduling**: Create separate orchestration script (Airflow, APScheduler, cron) that calls `run_pipeline()`

## Spanish Context
- Codebase is primarily in English (function names, docs) but with Spanish comments and emoji logging
- API responses from Open-Meteo include Spanish translations (e.g., `language: "es"` in geocoding)
- Default location: San José, Costa Rica; timezone: America/Costa_Rica
