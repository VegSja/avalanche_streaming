import json
import os
import sys
from typing import Any, Dict, List, Tuple

import numpy as np
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from requests import RequestException

# Add project root to the system path
project_root = os.path.abspath(os.path.join(os.getcwd()))
if project_root not in sys.path:
    sys.path.append(project_root)

import datetime
import logging

from src.data_classes.avalanche_region import AvalancheRegion
from src.repositories.constants import AVALANCHE_REGIONS

default_args = {
    "owner": " vegsja",
    "start_date": datetime.datetime(2025, 1, 24, 10, 00),
}
logging.basicConfig(level=logging.INFO)


from typing import Any, Dict

import requests
from requests.exceptions import RequestException

from data_classes.weather_data import WeatherData
from src.data_classes.daily_data import DailyData, DailyUnits
from src.repositories.constants import AvalancheRegion


def dict_to_weatherdata(parsed_data: Dict[str, Any]) -> WeatherData:
    """Converts a parsed data dictionary into a WeatherData object.

    Args:
        parsed_data (Dict[str, Any]): The parsed data
         dictionary containing the necessary fields for creating a WeatherData object.

    Returns:
        WeatherData: The created WeatherData object.
    """
    daily_data: DailyData = DailyData(**parsed_data["daily"])
    daily_units: DailyUnits = DailyUnits(**parsed_data["daily_units"])
    weather = WeatherData(
        latitude=parsed_data["latitude"],
        longitude=parsed_data["longitude"],
        generationtime_ms=parsed_data["generationtime_ms"],
        utc_offset_seconds=parsed_data["utc_offset_seconds"],
        timezone=parsed_data["timezone"],
        timezone_abbreviation=parsed_data["timezone_abbreviation"],
        elevation=parsed_data["elevation"],
        daily=daily_data,
        daily_units=daily_units,
    )
    return weather


def fetch_weather_data_and_put_in_queue(
    region: AvalancheRegion, date: str, kafka_producer: KafkaProducer
) -> None:
    """Retrieves weather data for a specific region, start date, and end date.

    Args:
        region (AvalancheRegion): The region for which weather data is requested.
        date (str): Format: %Y-%m-%d. The date for which we fetch the weather summary.
                    The function sets the start date to the beginning of the given date
                    and the end date to the end of the same date.

    Returns:
        WeatherData: The retrieved WeatherData object
        for the specified region and date range.

    Raises:
        RequestException: If the request for weather data fails.
        ValueError: If there is an error parsing the API response for weather data.
    """
    # Convert date string to datetime object

    urls = generate_weather_api_urls(
        longitudes=(region.east_south_lon, region.west_north_lon),
        latitudes=(region.west_north_lat, region.east_south_lat),
        start_date=date,
        end_date=date,
        number_of_grid_cells_east=5,
    )

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=30)
            res: Dict[str, Any] = response.json()
            res["start_date"] = date
            res["end_date"] = date
            res["region_id"] = region.region_id
            res["region_name"] = region.name
            # Send data to Kafka topic
            kafka_producer.send("weather_forecast", json.dumps(res).encode("utf-8"))
        except RequestException as err:
            raise RequestException(
                f"The request for weather data failed: {err}"
            ) from err


def generate_weather_api_urls(
    latitudes: Tuple[float, float],
    longitudes: Tuple[float, float],
    start_date: str,
    end_date: str,
    number_of_grid_cells_east: int,
) -> List[str]:
    """Generates a list of URLs for retrieving weather data over a grid.

    Args:
        latitudes (Tuple[float, float]): The southernmost and northernmost latitudes.
        longitudes (Tuple[float, float]): The westernmost and easternmost longitudes.
        start_date (str): The start date of the desired weather data range.
        end_date (str): The end date of the desired weather data range.
        number_of_grid_cells_east (int): The number of grid cells in the eastward direction.

    Returns:
        List[str]: A list of generated URLs for retrieving weather data for each grid cell.
    """
    lat_min, lat_max = latitudes
    lon_min, lon_max = longitudes

    # Compute the step size for longitude
    lon_step = (lon_max - lon_min) / number_of_grid_cells_east

    # Estimate the latitude step to maintain square cells
    # Convert degrees to kilometers (approximate conversion at mid-latitudes)
    km_per_degree_lat = 111  # Approximate conversion factor
    km_per_degree_lon = np.cos(np.radians((lat_min + lat_max) / 2)) * 111
    lat_step = lon_step * (km_per_degree_lon / km_per_degree_lat)

    # Compute the number of latitude cells needed to maintain square cells
    number_of_grid_cells_north = int(round((lat_max - lat_min) / lat_step))

    # Generate grid of latitudes and longitudes
    latitudes = np.linspace(lat_min, lat_max, number_of_grid_cells_north + 1)
    longitudes = np.linspace(lon_min, lon_max, number_of_grid_cells_east + 1)

    urls = []
    for i in range(number_of_grid_cells_north):
        for j in range(number_of_grid_cells_east):
            lat = (latitudes[i] + latitudes[i + 1]) / 2  # Center of the cell
            lon = (longitudes[j] + longitudes[j + 1]) / 2  # Center of the cell
            url = (
                f"https://archive-api.open-meteo.com/v1/archive?latitude="
                f"{lat:.4f}&longitude={lon:.4f}&"
                f"start_date={start_date}&"
                f"end_date={end_date}&"
                f"daily=weathercode,temperature_2m_max,temperature_2m_min,temperature_2m_mean"
                f",rain_sum,snowfall_sum,precipitation_hours,windspeed_10m_max,windgusts_10m_max"
                f",winddirection_10m_dominant&timezone=Europe%2FBerlin"
            )
            urls.append(url)

    return urls


def fetch_data_and_store_in_kafka():
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)

    yesterday_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    try:
        for region in AVALANCHE_REGIONS:
            fetch_weather_data_and_put_in_queue(
                region=region, date=yesterday_date, kafka_producer=producer
            )
    except Exception as e:
        logging.error(f"An error occured: {e}")
        raise Exception(e)


with DAG(
    "weather_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api", python_callable=fetch_data_and_store_in_kafka
    )
