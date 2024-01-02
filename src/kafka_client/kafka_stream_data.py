from src.constants import (
    URL_API,
    PATH_LAST_PROCESSED,
    MAX_LIMIT,
    MAX_OFFSET,
)

from .transformations import transform_row

import kafka.errors
import json
import datetime
import requests
from kafka import KafkaProducer
from typing import List
import logging

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO, force=True)


def get_latest_timestamp():
    """
    Gets the latest timestamp from the last_processed.json file
    """
    with open(PATH_LAST_PROCESSED, "r") as file:
        data = json.load(file)
        if "last_processed" in data:
            return data["last_processed"]
        else:
            return datetime.datetime.min


def update_last_processed_file(data: List[dict]):
    """
    Updates the last_processed.json file with the latest timestamp. Since the comparison is strict
    on the field date_de_publication, we set the new last_processed day to the latest timestamp minus one day.
    """
    publication_dates_as_timestamps = [
        datetime.datetime.strptime(row["date_de_publication"], "%Y-%m-%d")
        for row in data
    ]
    last_processed = max(publication_dates_as_timestamps) - datetime.timedelta(days=1)
    last_processed_as_string = last_processed.strftime("%Y-%m-%d")
    with open(PATH_LAST_PROCESSED, "w") as file:
        json.dump({"last_processed": last_processed_as_string}, file)


def get_all_data(last_processed_timestamp: datetime.datetime) -> List[dict]:
    n_results = 0
    full_data = []
    while True:
        # The publication date must be greater than the last processed timestamp and the offset (n_results)
        # corresponds to the number of results already processed.
        url = URL_API.format(last_processed_timestamp, n_results)
        response = requests.get(url)
        data = response.json()
        current_results = data["results"]
        full_data.extend(current_results)
        n_results += len(current_results)
        if len(current_results) < MAX_LIMIT:
            break
        # The sum of offset + limit API parameter must be lower than 10000.
        if n_results + MAX_LIMIT >= MAX_OFFSET:
            # If it is the case, change the last_processed_timestamp parameter to the date_de_publication
            # of the last retrieved result, minus one day. In case of duplicates, they will be filtered
            # in the deduplicate_data function. We also reset n_results (or the offset parameter) to 0.
            last_timestamp = current_results[-1]["date_de_publication"]
            timestamp_as_date = datetime.datetime.strptime(last_timestamp, "%Y-%m-%d")
            timestamp_as_date = timestamp_as_date - datetime.timedelta(days=1)
            last_processed_timestamp = timestamp_as_date.strftime("%Y-%m-%d")
            n_results = 0

    logging.info(f"Got {len(full_data)} results from the API")

    return full_data


def deduplicate_data(data: List[dict]) -> List[dict]:
    return list({v["reference_fiche"]: v for v in data}.values())


def query_data() -> List[dict]:
    """
    Queries the data from the API
    """
    last_processed = get_latest_timestamp()
    full_data = get_all_data(last_processed)
    full_data = deduplicate_data(full_data)
    if full_data:
        update_last_processed_file(full_data)
    return full_data


def process_data(row):
    """
    Processes the data from the API
    """
    return transform_row(row)


def create_kafka_producer():
    """
    Creates the Kafka producer object
    """
    try:
        producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
    except kafka.errors.NoBrokersAvailable:
        logging.info(
            "We assume that we are running locally, so we use localhost instead of kafka and the external "
            "port 9094"
        )
        producer = KafkaProducer(bootstrap_servers=["localhost:9094"])

    return producer


def stream():
    """
    Writes the API data to Kafka topic rappel_conso
    """
    producer = create_kafka_producer()
    results = query_data()
    kafka_data_full = map(process_data, results)
    for kafka_data in kafka_data_full:
        producer.send("rappel_conso", json.dumps(kafka_data).encode("utf-8"))


if __name__ == "__main__":
    stream()
