from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="192.168.8.2:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

packet = {
    "source_ip": "192.168.1.10",
    "destination_ip": "8.8.8.8",
    "protocol": "TCP",
    "packet_size": 512
}

future = producer.send("network_packets", packet)

metadata = future.get(timeout=10)

print("Message sent!")
print(f"Topic: {metadata.topic}")
print(f"Partition: {metadata.partition}")
print(f"Offset: {metadata.offset}")

producer.flush()