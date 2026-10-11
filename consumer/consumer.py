from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "network_packets",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ai-consumer-group"
)

print("Waiting for packets...\n")

for message in consumer:
    print("--------------------------------")
    print("Packet Received")
    print("--------------------------------")
    print(message.value)

    print("--------------------------------")
    print("Packet Details")
    print("--------------------------------")
    print("message :", message)