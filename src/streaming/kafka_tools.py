"""Small Kafka admin helpers (topic lifecycle, offsets) shared by the producer, the recovery test and the verification."""
from __future__ import annotations

import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from src.streaming import config as C


def admin(bootstrap: str = C.KAFKA_BOOTSTRAP) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap})


def topic_exists(topic: str, bootstrap: str = C.KAFKA_BOOTSTRAP) -> bool:
    return topic in admin(bootstrap).list_topics(timeout=10).topics


def ensure_topic(topic: str = C.TOPIC, partitions: int = C.TOPIC_PARTITIONS, bootstrap: str = C.KAFKA_BOOTSTRAP,
                 recreate: bool = False) -> None:
    a = admin(bootstrap)
    if recreate and topic_exists(topic, bootstrap):
        for f in a.delete_topics([topic], operation_timeout=30).values():
            f.result()
        for _ in range(60):                                   # deletion is asynchronous on the broker
            if not topic_exists(topic, bootstrap):
                break
            time.sleep(0.5)
    if not topic_exists(topic, bootstrap):
        cfg = {"retention.ms": str(7 * 24 * 3600 * 1000), "min.insync.replicas": "1"}
        for f in a.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1, config=cfg)]).values():
            f.result()


def end_offsets(topic: str = C.TOPIC, bootstrap: str = C.KAFKA_BOOTSTRAP) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "trafficsense-offset-probe", "enable.auto.commit": False})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    finally:
        c.close()
