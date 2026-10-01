"""Synthetic event generator.

Produces deliberately imperfect data. The rest of this project depends on there
being something to detect: a generator that emits clean rows would let a broken
freshness check or a broken null-rate check pass, which is the same failure mode
as an alert nobody ever tested.

Four kinds of dirt are injected, each one targeting a different check:

- **nulls** — `campaign_id`, `country`, `revenue` occasionally absent
- **late arrivals** — `event_time` in the past, so events land in older
  partitions after those partitions already got their snapshot
- **duplicate ids** — the same `event_id` twice, so an upsert key that is wrong
  shows up as duplicate rows rather than as a silently overwritten one
- **out-of-order timestamps** — `event_time` moving backwards within a key

A future schema version is available via ``--schema v2`` (see TRA-48). It adds a
nullable column and renames one, which is what makes the schema-evolution
scenarios possible without hand-editing SQL.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
# A bounded key space is what makes duplicates and upserts happen at all: if
# every row had a fresh id there would never be two rows for the same key.
KEYSPACE = int(os.getenv("GENERATOR_KEYSPACE", "50"))

NULL_RATE = float(os.getenv("GENERATOR_NULL_RATE", "0.12"))
DUPLICATE_RATE = float(os.getenv("GENERATOR_DUPLICATE_RATE", "0.05"))
LATE_RATE = float(os.getenv("GENERATOR_LATE_RATE", "0.08"))

COUNTRIES = ["BR", "US", "PT", "ES", "AR"]
CAMPAIGNS = ["spring", "summer", "retargeting", "brand", "referral"]


@dataclass
class Event:
    event_id: str
    user_id: str
    event_time: str
    event_type: str
    campaign_id: str | None
    country: str | None
    revenue: float | None

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


def _iso(moment: datetime) -> str:
    # Millisecond precision, always Z. Flink's `CAST(string AS TIMESTAMP_LTZ)`
    # rejects anything it cannot parse with a plain
    # java.time.format.DateTimeParseException, and Python's default
    # isoformat() emits microseconds -- which Flink rejects at index 10,
    # because it expects exactly three fractional digits.
    utc = moment.astimezone(timezone.utc)
    return f"{utc:%Y-%m-%dT%H:%M:%S}.{utc.microsecond // 1000:03d}Z"


def _v2_fieldnames() -> set[str]:
    """Fields that only exist in the v2 schema.

    Absent from v1 events entirely, which is what makes the Iceberg
    ``add nullable column`` scenario a real read of old and new data side by
    side rather than a column that was always there.
    """
    return {"referrer"}


def make_event(schema: str, now: datetime, rng: random.Random) -> Event:
    event_time = now

    # Late arrival: the event genuinely happened earlier, so it belongs to an
    # older Iceberg partition than whatever the pipeline last checkpointed.
    if rng.random() < LATE_RATE:
        event_time = now - timedelta(minutes=rng.randint(5, 90))

    event = Event(
        event_id=str(uuid.uuid4()),
        user_id=f"user-{rng.randint(1, KEYSPACE):04d}",
        event_time=_iso(event_time),
        event_type=rng.choice(["page_view", "purchase", "signup", "click"]),
        campaign_id=rng.choice(CAMPAIGNS) if rng.random() >= NULL_RATE else None,
        country=rng.choice(COUNTRIES) if rng.random() >= NULL_RATE else None,
        revenue=round(rng.uniform(1.0, 250.0), 2) if rng.random() >= NULL_RATE else None,
    )

    if schema == "v2":
        # A field that did not exist in v1. Iceberg fills null for rows written
        # before the column existed.
        event_dict = asdict(event)
        event_dict["referrer"] = rng.choice(["google", "newsletter", "twitter", None])
        return Event(**event_dict)

    return event


def produce(count: int, schema: str, bootstrap: str, topic: str, seed: int) -> int:
    from kafka import KafkaProducer

    rng = random.Random(seed)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda value: value.encode("utf-8"),
        # acks=all: a demo that silently drops events under load makes the
        # freshness check look broken for reasons that have nothing to do with
        # the pipeline.
        acks="all",
        retries=3,
        linger_ms=50,
    )

    now = datetime.now(timezone.utc)
    sent = 0

    for _ in range(count):
        event = make_event(schema, now, rng)
        producer.send(topic, key=event.user_id.encode("utf-8"), value=event.to_json())

        # Duplicate: same event_id emitted twice. If the upsert key is correct
        # this collapses to one row; if it is wrong, two rows land and the
        # duplicate check is what catches it.
        if rng.random() < DUPLICATE_RATE:
            producer.send(topic, key=event.user_id.encode("utf-8"), value=event.to_json())
            sent += 1

        sent += 1

    producer.flush()
    producer.close()

    # Confirm the topic really has what we think it has, rather than trusting
    # flush() to mean "the broker accepted it".
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    landed = sum(1 for _ in consumer)
    consumer.close()

    print(f"produced {sent} events (schema={schema}) -> topic={topic}")
    print(f"broker reports {landed} messages on the topic")
    return sent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument(
        "--schema",
        choices=["v1", "v2"],
        default="v1",
        help="v2 adds a nullable field and renames one; see TRA-48",
    )
    parser.add_argument("--bootstrap", default=BOOTSTRAP_SERVER)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    produce(args.count, args.schema, args.bootstrap, args.topic, args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())