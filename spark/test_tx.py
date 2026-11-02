import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

tx = {
    "step": 1, "type": "TRANSFER", "amount": 500000.0,
    "nameOrig": "CTEST000001", "oldbalanceOrg": 500000.0,
    "newbalanceOrig": 0.0, "nameDest": "CTEST000002",
    "oldbalanceDest": 0.0, "newbalanceDest": 500000.0,
    "isFraud": 1, "isFlaggedFraud": 0,
    "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
}
producer.send("transactions", key=tx["nameOrig"], value=tx)
producer.flush()
print("sent test transaction")