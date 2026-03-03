import json
import uuid
import random
import time
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer


fake = Faker()
TOPIC = "nyc_taxi_trips"

def generate_trip():
    pickup_time = fake.date_time_this_year()
    trip_distance = round(random.uniform(0.5, 15), 2)

    duration_minutes = trip_distance * random.uniform(2.5, 4.5)
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)

    fare_amount = round(trip_distance * random.uniform(2.5, 4.0), 2)
    tip_amount = round(fare_amount * random.uniform(0, 0.3), 2)

    extra = 1.0
    mta_tax = 0.5
    improvement_surcharge = 1.0
    congestion_surcharge = 2.5

    total_amount = round(
        fare_amount + extra + mta_tax + improvement_surcharge + congestion_surcharge + tip_amount,
        2
    )

    return {
        "event_id": str(uuid.uuid4()),
        "vendor_id": random.choice([1, 2]),
        "pickup_datetime": pickup_time.isoformat(),
        "dropoff_datetime": dropoff_time.isoformat(),
        "passenger_count": random.randint(1, 4),
        "trip_distance": trip_distance,
        "ratecode_id": 1,
        "store_and_fwd_flag": "N",
        "pu_location_id": random.randint(1, 263),
        "do_location_id": random.randint(1, 263),
        "payment_type": random.choice([1, 2]),
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": 0.0,
        "improvement_surcharge": improvement_surcharge,
        "congestion_surcharge": congestion_surcharge,
        "airport_fee": 0.0,
        "total_amount": total_amount,
        "event_timestamp": datetime.utcnow().isoformat()
    }


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Kafka connected:", producer.bootstrap_connected())

    sent = 0
    try:
        while True:
            trip = generate_trip()
            producer.send(TOPIC, trip)
            sent += 1

            if sent % 10 == 0:
                producer.flush()
                print(f"Sent {sent} events (latest={trip['event_id']})")

            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()