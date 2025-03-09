from datetime import datetime
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

default_args = {"owner": " vegsja", "start_date": datetime(2025, 1, 24, 10, 00)}

logging.basicConfig(level=logging.INFO)

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res["results"][0]

    return res


def format_data(res):
    data = {}
    location = res["location"]
    data['id'] = uuid.uuid4()
    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["address"] = (
        f"{str(location['street']['number'])} {location['street']['name']}, "
        f"{location['city']}, {location['state']}, {location['country']}"
    )
    data["post_code"] = location["postcode"]
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["dob"] = res["dob"]["date"]
    data["registered_date"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data["picture"] = res["picture"]["medium"]

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    logging.basicConfig(level=logging.INFO)

    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)
    curr_time = time.time()

    while True:
        # We stream for 60 seconds
        if time.time() > curr_time + 60:
            break
        try:
            # Send request to API
            res = get_data()
            # Format data
            res = format_data(res)

            # Send data to Kafka topic
            logging.info(f"Sending data {res['username']} to Kafka topic")
            producer.send("users_created", json.dumps(res).encode("utf-8"))
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue


with DAG(
    "user_automation",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=True,
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api", python_callable=stream_data
    )
