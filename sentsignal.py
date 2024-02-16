from google.cloud import pubsub_v1
import json

# TODO(developer)
project_id = "arquisoft4019"
topic_id = "signals"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

message_json = {
    "placa": "ABC123",
    "timestamp": "2021-01-01T12:00:00",
    "latitude": 123.45,
    "longitude": 67.89,
    "velocity": 50,
    "direction": "N",
    "temperature": 20
}

# Convert to a byte string
message_bytes = json.dumps(message_json).encode("utf-8")

for n in range(1, 500):
    data_str = f"Message number {n}"
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    # When you publish a message, the client returns a future.
    try:
        future = publisher.publish(topic_path, message_bytes)
        print(future.result())
    except Exception as e:
        print(f'An error occurred: {e}')

print(f"Published messages to {topic_path}.")
