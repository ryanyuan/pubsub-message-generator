#!/usr/bin/env python
from config_loader import load_config
from google.cloud import pubsub_v1

config = load_config("../config.yml")
project_id = config["project_id"]
subscription = config["subscription"]
subscription_name = f"projects/{project_id}/subscriptions/{subscription}"
print(f"Subscription name: {subscription_name}")

subscriber = pubsub_v1.SubscriberClient()

print("Listening for messages on {}".format(subscription_name))
while True:
    response = subscriber.pull(subscription_name, max_messages=5)

    for msg in response.received_messages:
        print("Received message:", msg.message.data)

    ack_ids = [msg.ack_id for msg in response.received_messages]
    subscriber.acknowledge(subscription_name, ack_ids)
