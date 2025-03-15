import os
import sys
from typing import Any, Dict, List

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
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
from Constants.avalanche_regions import AvalancheRegion
from requests.exceptions import RequestException

from data_classes.weather_data import DailyData, DailyUnits, WeatherData


def from_nested_dict(dict_input: Dict[str, Any]) -> Any:
    """Converts a nested dictionary into the
     appropriate data structure based on the "__type__" field.

    Args:

        dict_input (Dict[str, Any]): The input dictionary to be converted.

    Returns:
        Any: The converted nested data structure
         based on the "__type__" field in the input dictionary.
    """
    if "__type__" in dict_input:
        type_name = dict_input.pop("__type__")
        if type_name == "WeatherData":
            dict_input["daily"] = DailyData(**dict_input["daily"])
            dict_input["daily_units"] = DailyUnits(**dict_input["daily_units"])
            return WeatherData(**dict_input)
        if type_name == "DailyData":
            return DailyData(**dict_input)
        if type_name == "DailyUnits":
            return DailyUnits(**dict_input)
    return dict_input


from_nested_dict("hello")


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


def get_weather_data(
    region: AvalancheRegion, start_date: str, end_date: str
) -> WeatherData:
    """Retrieves weather data for a specific region, start date, and end date.

    Args:
        region (AvalancheRegion): The region for which weather data is requested.
        start_date (str): The start date of the desired weather data range.
        end_date (str): The end date of the desired weather data range.

    Returns:
        WeatherData: The retrieved WeatherData object
        for the specified region and date range.

    Raises:
        RequestException: If the request for weather data fails.
        ValueError: If there is an error parsing the API response for weather data.
    """
    url = generate_url(region.lat, region.lon, start_date, end_date)

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except RequestException as err:
        raise RequestException(f"The request for weather data failed: {err}") from err

    try:
        res: Dict[str, Any] = response.json()
        weather = dict_to_weatherdata(res)
        return weather
    except Exception as err:
        raise ValueError(
            f"Failed to parse API response for weather data: {err}"
        ) from err


def generate_url(
    latitude: float, longitude: float, start_date: str, end_date: str
) -> str:
    """Generates a URL for retrieving weather data.

    Args:
        latitude (float): The latitude of the location for which weather data is requested.
        longitude (float): The longitude of the location for which weather data is requested.
        start_date (str): The start date of the desired weather data range.
        end_date (str): The end date of the desired weather data range.

    Returns:
        str: The generated URL for retrieving weather data.
    """
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?latitude="
        f"{latitude:.4f}&longitude={longitude:.4f}&"
        f"start_date={start_date}&"
        f"end_date={end_date}&"
        f"daily=weathercode,temperature_2m_max,temperature_2m_min,temperature_2m_mean"
        f",rain_sum,snowfall_sum,precipitation_hours,windspeed_10m_max,windgusts_10m_max"
        f",winddirection_10m_dominant&timezone=Europe%2FBerlin"
    )
    return url


def fetch_data_and_store_in_kafka():
    import json

    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)

    today_date = datetime.date.today().strftime("%Y-%m-%d")

    try:
        for region in AVALANCHE_REGIONS:
            json_response = get_avalanche_data(
                region=region, start_date=today_date, end_date=today_date
            )[
                0
            ]  # Since we are only getting one date.
            # Send data to Kafka topic
            producer.send(
                "avalanche_region_warning", json.dumps(json_response).encode("utf-8")
            )
    except Exception as e:
        logging.error(f"An error occured: {e}")


with DAG(
    "avalanche_fetcher",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api", python_callable=fetch_data_and_store_in_kafka
    )
