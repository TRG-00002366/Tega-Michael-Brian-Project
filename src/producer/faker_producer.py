import argparse
import json
import os
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

TOPIC = "nyc_taxi_trips"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_VERSION = "1.0"


PAYMENT_TYPES = [1, 2, 3, 4]  # 1=Credit card, 2=Cash, 3=No charge, 4=Dispute
PAYMENT_WEIGHTS = [0.70, 0.22, 0.05, 0.03]

RATE_CODES = [1, 2, 3, 4, 5, 6]
RATE_CODE_WEIGHTS = [0.82, 0.06, 0.04, 0.03, 0.03, 0.02]

VENDOR_IDS = [1, 2]
STORE_AND_FWD_FLAGS = ["N", "Y"]

# Simulated airport / premium zones for slightly richer analytics
AIRPORT_ZONES = {132, 138}  # JFK, LGA-like placeholders
ALL_ZONES = list(range(1, 264))


def weighted_choice(values: list[int], weights: list[float]) -> int:
    """Return one value using the provided probability weights."""
    return random.choices(values, weights=weights, k=1)[0]


def generate_pickup_time() -> datetime:
    """
    Generate a pickup timestamp with simple daypart weighting so the data
    looks less uniform than a fully random Faker timestamp.
    """
    now = datetime.now(UTC)

    # Bias toward recent dates so the demo report looks current.
    days_back = random.randint(0, 45)
    base_date = now - timedelta(days=days_back)

    # Daypart distribution: morning, midday, evening, late night
    hour_blocks = [
        (6, 10),   # morning
        (10, 16),  # midday
        (16, 21),  # evening rush
        (21, 24),  # late evening
        (0, 6),    # overnight
    ]
    hour_weights = [0.22, 0.26, 0.30, 0.12, 0.10]
    start_hour, end_hour = random.choices(hour_blocks, weights=hour_weights, k=1)[0]

    hour = random.randint(start_hour, end_hour - 1)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return base_date.replace(hour=hour, minute=minute, second=second, microsecond=0)


def generate_trip_distance() -> float:
    """
    Generate a right-skewed trip distance.
    Most taxi rides are short; a smaller number are medium or long.
    """
    bucket = random.choices(
        population=["short", "medium", "long"],
        weights=[0.68, 0.24, 0.08],
        k=1
    )[0]

    if bucket == "short":
        return round(random.uniform(0.7, 4.5), 2)
    if bucket == "medium":
        return round(random.uniform(4.5, 12.0), 2)
    return round(random.uniform(12.0, 28.0), 2)


def generate_location_pair() -> tuple[int, int]:
    """
    Occasionally create airport-related trips to enrich revenue and surcharge patterns.
    """
    airport_trip = random.random() < 0.08

    if airport_trip:
        pu = random.choice(list(AIRPORT_ZONES)) if random.random() < 0.5 else random.choice(ALL_ZONES)
        do = random.choice(ALL_ZONES) if pu in AIRPORT_ZONES else random.choice(list(AIRPORT_ZONES))
    else:
        pu = random.choice(ALL_ZONES)
        do = random.choice(ALL_ZONES)

    return pu, do


def estimate_duration_minutes(trip_distance: float, pickup_time: datetime) -> int:
    """
    Roughly simulate slower traffic during commute windows and faster trips overnight.
    """
    hour = pickup_time.hour

    if 7 <= hour <= 9 or 16 <= hour <= 19:
        mph = random.uniform(8, 16)   # heavier traffic
    elif 0 <= hour <= 5:
        mph = random.uniform(18, 28)  # light traffic
    else:
        mph = random.uniform(12, 22)

    duration_hours = trip_distance / max(mph, 1.0)
    duration_minutes = max(3, int(duration_hours * 60))
    return duration_minutes


def calculate_fare_components(
    trip_distance: float,
    duration_minutes: int,
    pickup_zone: int,
    dropoff_zone: int,
    payment_type: int,
    ratecode_id: int
) -> dict:
    """
    more realistic fare behavior.
    still synthetic, but much better for analytics
    """
    base_fare = 2.50
    distance_charge = trip_distance * random.uniform(2.3, 3.4)
    time_charge = duration_minutes * random.uniform(0.30, 0.55)

    fare_amount = round(base_fare + distance_charge + time_charge, 2)

    extra = 1.0 if random.random() < 0.65 else 0.0
    mta_tax = 0.5
    improvement_surcharge = 1.0

    tolls_amount = round(random.choice([0.0, 0.0, 0.0, 3.5, 6.94, 9.5]), 2)

    congestion_surcharge = 2.5 if payment_type == 1 else 0.0
    airport_fee = 1.25 if (pickup_zone in AIRPORT_ZONES or dropoff_zone in AIRPORT_ZONES) else 0.0

    # Higher ratecode can indicate premium / special handling
    if ratecode_id in {2, 5}:
        fare_amount = round(fare_amount * random.uniform(1.10, 1.35), 2)

    # Tip only really makes sense for card trips
    if payment_type == 1:
        tip_amount = round(fare_amount * random.uniform(0.08, 0.28), 2)
    else:
        tip_amount = round(fare_amount * random.uniform(0.00, 0.08), 2)

    total_amount = round(
        fare_amount
        + extra
        + mta_tax
        + improvement_surcharge
        + tolls_amount
        + congestion_surcharge
        + airport_fee
        + tip_amount,
        2
    )

    return {
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "improvement_surcharge": improvement_surcharge,
        "congestion_surcharge": congestion_surcharge,
        "airport_fee": airport_fee,
        "total_amount": total_amount,
    }


def generate_trip() -> dict:
    """Generate one synthetic taxi trip event."""
    pickup_time = generate_pickup_time()
    trip_distance = generate_trip_distance()
    duration_minutes = estimate_duration_minutes(trip_distance, pickup_time)
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)

    vendor_id = random.choice(VENDOR_IDS)
    payment_type = weighted_choice(PAYMENT_TYPES, PAYMENT_WEIGHTS)
    ratecode_id = weighted_choice(RATE_CODES, RATE_CODE_WEIGHTS)
    pu_location_id, do_location_id = generate_location_pair()

    fare_components = calculate_fare_components(
        trip_distance=trip_distance,
        duration_minutes=duration_minutes,
        pickup_zone=pu_location_id,
        dropoff_zone=do_location_id,
        payment_type=payment_type,
        ratecode_id=ratecode_id,
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "vendor_id": vendor_id,
        "pickup_datetime": pickup_time.isoformat(),
        "dropoff_datetime": dropoff_time.isoformat(),
        "passenger_count": random.randint(1, 6),
        "trip_distance": trip_distance,
        "ratecode_id": ratecode_id,
        "store_and_fwd_flag": random.choices(STORE_AND_FWD_FLAGS, weights=[0.96, 0.04], k=1)[0],
        "pu_location_id": pu_location_id,
        "do_location_id": do_location_id,
        "payment_type": payment_type,
        **fare_components,
        "event_timestamp": datetime.now(UTC).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce fake NYC taxi trip events to Kafka")
    parser.add_argument("--num-events", type=int, default=500, help="Number of events to produce")
    parser.add_argument("--sleep-seconds", type=float, default=0.10, help="Delay between events")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",  # safer delivery semantics for the demo
        retries=3,
    )

    print("Kafka connected:", producer.bootstrap_connected())

    sent = 0
    try:
        for _ in range(args.num_events):
            trip = generate_trip()
            producer.send(TOPIC, trip)
            sent += 1

            if sent % 25 == 0:
                producer.flush()
                print(
                    f"Sent {sent} events | "
                    f"latest_event_id={trip['event_id']} | "
                    f"pickup={trip['pickup_datetime']} | "
                    f"distance={trip['trip_distance']}"
                )

            time.sleep(args.sleep_seconds)

        print(f"Finished producing {sent} events.")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()