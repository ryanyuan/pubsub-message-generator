#!/usr/bin/env python
import json
import time
from random import randint

import requests

from google.cloud import pubsub_v1

from .config_loader import load_config


def main(argv=None):
    config = load_config("./config.yml")
    project_id = config["project_id"]
    topic = config["topic"]
    topic_name = f"projects/{project_id}/topics/{topic}"
    print("Publishing messages on {}".format(topic_name))

    publisher = pubsub_v1.PublisherClient()
    api_url = config["api_url"]
    response = requests.post(api_url).json()
    results = response["results"]
    index = 0
    while True:
        index = 0 if index > len(results) - 1 else index

        msg = str.encode(json.dumps(results[index]))
        publisher.publish(topic_name, msg, spam="eggs")
        print("Published message:", msg)

        index += 1
        time.sleep(randint(0, 20))
