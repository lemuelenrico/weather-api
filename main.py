import logging
import logging.handlers
import os

import requests

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
logger.addHandler(logger_file_handler)

try:
    SOME_SECRET = os.environ["SOME_SECRET"]
except KeyError:
    SOME_SECRET = "Token not available!"


if __name__ == "__main__":
    logger.info(f"Token value: {SOME_SECRET}")

    url = "https://api.open-meteo.com/v1/forecast?latitude=14.014&longitude=121.60&current_weather=true"

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()  # raises error for bad status

        data = r.json()

        # 🔥 Updated parsing based on new API structure
        current_weather = data["current_weather"]

        temperature = current_weather["temperature"]
        windspeed = current_weather["windspeed"]
        winddirection = current_weather["winddirection"]
        is_day = current_weather["is_day"]
        weathercode = current_weather["weathercode"]
        time = current_weather["time"]

        logger.info(f"Time: {time}")
        logger.info(f"Temperature: {temperature} °C")
        logger.info(f"Wind Speed: {windspeed} km/h")
        logger.info(f"Wind Direction: {winddirection}°")
        logger.info(f"Is Day: {is_day}")
        logger.info(f"Weather Code: {weathercode}")

    except Exception as e:
        logger.exception("Request or parsing failed")