from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import certifi
import pandas as pd
import requests
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, UpdateOne

# -----------------------------
# ENV / CONFIG
# -----------------------------
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "clima_data")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "clima_data")

# Defaults (puedes ponerlos también en .env)
DEFAULT_CITY = os.getenv("DEFAULT_CITY", "San Jose")
DEFAULT_COUNTRY_CODE = os.getenv("DEFAULT_COUNTRY_CODE", "CR")
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "America/Costa_Rica")

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


@dataclass(frozen=True)
class Location:
    city_input: str
    country_code: str
    location_name: str
    latitude: float
    longitude: float


# -----------------------------
# 1) EXTRACT - Geocoding
# -----------------------------
def get_lat_lon(city: str, country_code: str) -> Location:
    params = {
        "name": city,
        "count": 10,
        "language": "es",
        "format": "json",
        "country_code": country_code,
    }
    r = requests.get(GEOCODING_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    results = data.get("results") or []
    if not results:
        raise ValueError(f"No se encontraron resultados para city={city}, country_code={country_code}")

    cc = country_code.upper()

    # 1) filtrar por country_code exacto
    candidates = [x for x in results if (x.get("country_code") or "").upper() == cc]

    # 2) si hay varios, preferir el que tenga country “Costa Rica” (extra seguro en es)
    def score(x):
        s = 0
        if (x.get("country_code") or "").upper() == cc:
            s += 10
        if (x.get("country") or "").lower() in ["costa rica", "costa rica "]:
            s += 5
        # si tiene admin1 (provincia/estado), suele ser más específico
        if x.get("admin1"):
            s += 1
        return s

    chosen = None
    if candidates:
        chosen = sorted(candidates, key=score, reverse=True)[0]
    else:
        # si no hay match real, mejor fallar para no cargar datos incorrectos
        raise ValueError(
            f"Geocoding ambiguo: no hubo resultados con country_code={country_code}. "
            f"Top result fue: {results[0].get('name')} / {results[0].get('country')} ({results[0].get('country_code')})"
        )

    lat = float(chosen["latitude"])
    lon = float(chosen["longitude"])
    pieces = [chosen.get("name"), chosen.get("admin1"), chosen.get("country")]
    location_name = ", ".join([p for p in pieces if p])

    return Location(
        city_input=city,
        country_code=chosen.get("country_code"),
        location_name=location_name,
        latitude=lat,
        longitude=lon,
    )

# -----------------------------
# 2) EXTRACT - Forecast
# -----------------------------
def fetch_hourly_weather(
    lat: float,
    lon: float,
    timezone_name: str,
    hourly_vars: str = "temperature_2m,relative_humidity_2m,precipitation,windspeed_10m",
) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": hourly_vars,
        "timezone": timezone_name,
    }
    r = requests.get(FORECAST_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# -----------------------------
# 3) STAGE - JSON -> DataFrame (staging)
# -----------------------------
def to_staging_df(weather_json: Dict[str, Any], location: Location, timezone_name: str) -> pd.DataFrame:
    hourly = weather_json.get("hourly") or {}

    df = pd.DataFrame(
        {
            "time": hourly.get("time", []),
            "temperature_2m": hourly.get("temperature_2m", []),
            "relative_humidity_2m": hourly.get("relative_humidity_2m", []),
            "precipitation": hourly.get("precipitation", []),
            "windspeed_10m": hourly.get("windspeed_10m", []),
        }
    )

    # Contexto (metadatos)
    df["location_name"] = location.location_name
    df["city_input"] = location.city_input
    df["country_code"] = location.country_code
    df["latitude"] = location.latitude
    df["longitude"] = location.longitude
    df["timezone_name"] = timezone_name

    # Trazabilidad
    df["source"] = "open-meteo"
    df["ingested_at_utc"] = datetime.now(timezone.utc)

    return df


# -----------------------------
# 4) TRANSFORM - limpieza + features
# -----------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["time"] = pd.to_datetime(out["time"], errors="coerce")
    out["temperature_2m"] = pd.to_numeric(out["temperature_2m"], errors="coerce")
    out["relative_humidity_2m"] = pd.to_numeric(out["relative_humidity_2m"], errors="coerce")
    out["precipitation"] = pd.to_numeric(out["precipitation"], errors="coerce")
    out["windspeed_10m"] = pd.to_numeric(out["windspeed_10m"], errors="coerce")

    # calidad mínima
    out = out.dropna(subset=["time", "temperature_2m"])

    # features útiles
    out["is_rain"] = out["precipitation"].fillna(0) > 0
    out["date"] = out["time"].dt.date.astype(str)
    out["hour"] = out["time"].dt.hour.astype(int)

    # normaliza NaNs para Mongo (opcional)
    out = out.where(pd.notnull(out), None)

    return out


# -----------------------------
# 5) LOAD - MongoDB (upsert + indexes)
# -----------------------------
def get_mongo_client() -> MongoClient:
    if not MONGO_URI:
        raise ValueError(f"MONGO_URI no está definida en {env_path}")

    print("DEBUG using python:", __import__("sys").executable)
    print("DEBUG certifi:", certifi.where())
    print("DEBUG tlsCAFile set? -> YES")

    return MongoClient(
        MONGO_URI,
        tls=True,  # <- importante
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=8000,
        connectTimeoutMS=8000,
        socketTimeoutMS=8000,
    )


def ensure_indexes(col) -> None:
    # idempotencia: no duplica (location_name + time)
    col.create_index([("location_name", ASCENDING), ("time", ASCENDING)], unique=True, name="uq_location_time")
    col.create_index([("date", ASCENDING)], name="idx_date")
    col.create_index([("city_input", ASCENDING)], name="idx_city_input")


def load_to_mongo(df: pd.DataFrame) -> Dict[str, int]:
    if df.empty:
        return {"matched": 0, "modified": 0, "upserted": 0}

    client = get_mongo_client()
    print("DEBUG ping from pipeline:", client.admin.command("ping"))
    col = client[DB_NAME][COLLECTION_NAME]
    ensure_indexes(col)

    docs: List[Dict[str, Any]] = df.to_dict(orient="records")

    # Asegurar datetime python nativo
    for d in docs:
        if isinstance(d.get("time"), pd.Timestamp):
            d["time"] = d["time"].to_pydatetime()

    ops = []
    for d in docs:
        key = {"location_name": d["location_name"], "time": d["time"]}
        ops.append(UpdateOne(key, {"$set": d}, upsert=True))

    res = col.bulk_write(ops, ordered=False)
    upserted = len(res.upserted_ids) if res.upserted_ids else 0

    return {"matched": res.matched_count, "modified": res.modified_count, "upserted": upserted}


# -----------------------------
# RUN PIPELINE
# -----------------------------
def run_pipeline(city: str = DEFAULT_CITY, country_code: str = DEFAULT_COUNTRY_CODE, timezone_name: str = DEFAULT_TIMEZONE):
    print(f"1) Geocoding -> city={city}, country_code={country_code}")
    loc = get_lat_lon(city, country_code)

    # 🔒 Validación de seguridad
    if loc.country_code.upper() != country_code.upper():
        raise ValueError(
            f"Geocoding no coincide con country_code solicitado. "
            f"Esperado={country_code}, obtenido={loc.country_code}"
        )

    print(f"   -> {loc.location_name} (lat={loc.latitude}, lon={loc.longitude})")
    print("2) Forecast -> Open-Meteo hourly")
    weather_json = fetch_hourly_weather(loc.latitude, loc.longitude, timezone_name)

    print("3) Staging -> DataFrame")
    staging = to_staging_df(weather_json, loc, timezone_name)
    print(f"   staging rows={len(staging)}")

    print("4) Transform")
    curated = transform(staging)
    print(f"   curated rows={len(curated)}")

    print(f"5) Load -> MongoDB {DB_NAME}.{COLLECTION_NAME}")
    result = load_to_mongo(curated)
    print("✅ Mongo result:", result)

    return result


if __name__ == "__main__":
    run_pipeline()