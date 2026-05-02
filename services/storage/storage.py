import os 
import json 
import logging
import signal 
import sys 

from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient, ASCENDING 
from pymongo.errors import PyMongoError 
from datetime import datetime

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9095,kafka-3:9097")
TOPIC = os.getenv("TOPIC_TELEMETRY", "flight.telemetry")
GROUP_ID = os.getenv("GROUP_ID", "storage-group")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = os.getenv("MONGO_DB", "flight_archive")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "departures_history")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("storage") 

def kafka_ssl_base() -> dict:
    return{
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
        "ssl.ca.location": os.getenv("KAFKA_SSL_CA_LOCATION", "/app/security/ca.crt"),
        "ssl.certificate.location": os.getenv(
            "KAFKA_SSL_CERTIFICATE_LOCATION",
            "/app/security/client-creds/kafka.client.certificate.pem",
        ),
        "ssl.key.location": os.getenv(
            "KAFKA_SSL_KEY_LOCATION",
            "/app/security/client-creds/kafka.client.key",
        ),
    }

def setup_mongo():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME] 
    coll.create_index(
        [("airport", ASCENDING), ("scheduled_departure", ASCENDING), ("flight_code", ASCENDING)],
        unique=True,
        name="uniq_flight_event",
    )
    coll.create_index(
    [("scheduled_departure_dt", ASCENDING)],
    expireAfterSeconds=24 * 3600,
    name="ttl_flight_24h",
)
    log.info("Mongo connected: db=%s collection=%s", DB_NAME, COLLECTION_NAME)
    return client, coll 

def upsert_flight(coll, data:dict) -> bool:
    sched = data.get("scheduled_departure")
    if sched:
        try:
            # store an extra field as native BSON Date for TTL to work
            update["$set"]["scheduled_departure_dt"] = datetime.fromisoformat(
                sched.replace("Z", "+00:00")
            )
        except ValueError:
            pass
    flight = {
        "airport": data["airport"],
        "scheduled_departure": data["scheduled_departure"],
        "flight_code": data["flight_code"],
    }
    update = {
        "$set": data,
        "$setOnInsert": {"first_seen_ts": data.get("ts")},
    }
    try:
        result = coll.update_one(flight, update, upsert=True)
        action = "inserted" if result.upserted_id else "updated"
        log.info(
            "%s %s:%s status=%s gate=%s",
            action,
            data["airport"],
            data["flight_code"],
            data.get("status"),
            data.get("gate"),
        )
        return True 
    except PyMongoError as e:
        log.error("Mongo insert failed: %s",e)
        return False

def flight_filter(data: dict) -> dict:
    return {
        "airport": data["airport"],
        "scheduled_departure": data["scheduled_departure"],
        "flight_code": data["flight_code"],
    }


def delete_flight(coll, data: dict) -> bool:
    try:
        result = coll.delete_one(flight_filter(data))
        log.info(
            "deleted %s:%s scheduled=%s deleted_count=%d",
            data["airport"],
            data["flight_code"],
            data["scheduled_departure"],
            result.deleted_count,
        )
        return True
    except PyMongoError as e:
        log.error("Mongo delete failed: %s", e)
        return False


def handle_flight(coll, data: dict) -> bool:
    if data.get("event_type") == "DELETE":
        return delete_flight(coll, data)
    return upsert_flight(coll, data)
    
def start_storage():
    log.info("Staring storage service...")
    log.info("Topic=%s GroupID=%s", TOPIC, GROUP_ID)

    mongo_client, coll = setup_mongo()

    consumer = Consumer({
        **kafka_ssl_base(),
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "flight-storage",
    })
    consumer.subscribe([TOPIC])
    running = {"flag": True}

    def stop(*_):
        log.info("Shutdown signal reveived")
        running["flag"] = False
    
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        while running["flag"]:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Kafka consumer error: %s", msg.error())
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.error("Malformed message at offset %d: %s", msg.offset(), e)
                consumer.commit(message=msg, asynchronous=False)
                continue

            ok = handle_flight(coll, data)
            if ok:
                consumer.commit(message=msg, asynchronous=False)
            else:
                log.warning("Skipping commit for offset %d, retry", msg.offset())
    except KafkaException as e:
        log.error("Fatal Kafka error: %s", e)
    finally:
        log.info("Closing consumer and Mongo client")
        consumer.close()
        mongo_client.close()
        sys.exit(0)


if __name__ == "__main__":
    start_storage()