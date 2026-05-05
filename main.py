import logging
import logging.handlers
import os

import requests
import psycopg
import time


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(logger_file_handler)


WEATHER_API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=14.014&longitude=121.60&current_weather=true"
)


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_observations (
    id BIGSERIAL PRIMARY KEY,
    api_latitude NUMERIC(9, 6),
    api_longitude NUMERIC(9, 6),
    timezone TEXT,
    observation_time TIMESTAMP,
    temperature_c NUMERIC(5, 2),
    windspeed_kmh NUMERIC(5, 2),
    winddirection_deg INTEGER,
    is_day INTEGER,
    weathercode INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""


INSERT_WEATHER_SQL = """
INSERT INTO weather_observations (
    api_latitude,
    api_longitude,
    timezone,
    observation_time,
    temperature_c,
    windspeed_kmh,
    winddirection_deg,
    is_day,
    weathercode
)
VALUES (
    %(api_latitude)s,
    %(api_longitude)s,
    %(timezone)s,
    %(observation_time)s,
    %(temperature_c)s,
    %(windspeed_kmh)s,
    %(winddirection_deg)s,
    %(is_day)s,
    %(weathercode)s
);
"""

def get_current_weather():
    max_attempts = 3
    wait_seconds = 10

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(WEATHER_API_URL, timeout=10)
            response.raise_for_status()

            data = response.json()
            current_weather = data["current_weather"]

            weather_record = {
                "api_latitude": data["latitude"],
                "api_longitude": data["longitude"],
                "timezone": data["timezone"],
                "observation_time": current_weather["time"],
                "temperature_c": current_weather["temperature"],
                "windspeed_kmh": current_weather["windspeed"],
                "winddirection_deg": current_weather["winddirection"],
                "is_day": current_weather["is_day"],
                "weathercode": current_weather["weathercode"],
            }

            return weather_record

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else None

            logger.warning(
                f"Attempt {attempt} failed with HTTP status {status_code}: {e}"
            )

            if attempt == max_attempts:
                raise

            time.sleep(wait_seconds)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt} failed due to request error: {e}")

            if attempt == max_attempts:
                raise

            time.sleep(wait_seconds)


def save_weather_to_database(weather_record):
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(INSERT_WEATHER_SQL, weather_record)

        conn.commit()


if __name__ == "__main__":
    try:
        logger.info("Weather pipeline started")

        weather_record = get_current_weather()
        logger.info(f"Weather record extracted: {weather_record}")

        save_weather_to_database(weather_record)
        logger.info("Weather record inserted into Neon successfully")

    except Exception:
        logger.exception("Weather pipeline failed")
        raise