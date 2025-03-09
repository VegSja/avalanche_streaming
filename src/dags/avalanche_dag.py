from typing import List, Dict, Any
import requests
import os
import sys

from requests import RequestException

from airflow import DAG
from airflow.operators.python import PythonOperator


# Add project root to the system path
project_root = os.path.abspath(os.path.join(os.getcwd()))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.repositories.constants import AVALANCHE_REGIONS
from src.data_classes.avalanche_region import AvalancheRegion
import logging
import datetime

default_args = {
    "owner": " vegsja",
    "start_date": datetime.datetime(2025, 1, 24, 10, 00),
}
logging.basicConfig(level=logging.INFO)


def generate_url(region_id: str, start_date: str, end_date: str) -> str:
    """
    Generate the URL for retrieving avalanche data
    from Varsom's API.

    Args:
        region_id: A string representing the region
        ID for which to retrieve avalanche data.
        start_date: A string representing the start date
        of the time range in the format "YYYY-MM-DD".
        end_date: A string representing the end date of
         the time range in the format "YYYY-MM-DD".

    Returns:
        The URL string for retrieving avalanche data
        from Varsom's API.
    """

    language_key = "1"
    url = (
        f"https://api01.nve.no/"
        f"hydrology/forecast/avalanche/v6.2.1/api/"
        f"AvalancheWarningByRegion/Simple/"
        f"{region_id}/{language_key}/{start_date}/{end_date}"
    )
    return url


def get_avalanche_data(
    region: AvalancheRegion, start_date: str, end_date: str
) -> List[Dict[str, Any]]:
    """
        Retrieve avalanche data from Varsom's
         API for a specific region and time range.

        Args:
            region: An AvalancheRegion object
            representing the region for which to retrieve avalanche data.
            start_date: A string representing
            the start date of the time range in the format "YYYY-MM-DD".
            end_date: A string representing the end
             date of the time range in the format "YYYY-MM-DD".
    services.scraper.
        Returns:
            A list of VarsomAvalancheResponse objects
             representing the retrieved avalanche warnings.

        Raises:
            ValueError: If the response from Varsom's API
            does not match the expected format.
            RequestException: If an unexpected error
             occurs during the avalanche data fetching process.
    """
    logging.info(f"Fetching avalanche data for region: {region.name}")

    url = generate_url(region.region_id, start_date, end_date)
    logging.info("Fetching avalanche data with url: " + url)

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.get(url=url, headers=headers, timeout=10)

    if response.status_code == requests.codes.ok:
        try:
            return response.json()
        except Exception as err:
            raise ValueError(
                f"The response we received "
                f"from Varsom's API did not match what we expected: {err}"
            ) from err
    else:
        raise RequestException(
            f"Uh oh! Something unexpected happened during avalanche fetching: {response}"
        )


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


fetch_data_and_store_in_kafka()

with DAG(
    "avalanche_fetcher",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api", python_callable=fetch_data_and_store_in_kafka
    )
