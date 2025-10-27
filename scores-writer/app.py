import os
import json
import time
import logging
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scores-writer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_SCORES_TOPIC      = os.getenv("KAFKA_SCORES_TOPIC", "scores")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "scores-writer")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

PGHOST      = os.getenv("PGHOST", "postgres")
PGPORT      = int(os.getenv("PGPORT", "5432"))
PGUSER      = os.getenv("PGUSER", "postgres")
PGPASSWORD  = os.getenv("PGPASSWORD", "postgres")
PGDATABASE  = os.getenv("PGDATABASE", "frauddb")
SCORES_TABLE = os.getenv("SCORES_TABLE", "scores")

DDL = f"""
CREATE TABLE IF NOT EXISTS {SCORES_TABLE} (
    transaction_id TEXT,
    score DOUBLE PRECISION,
    fraud_flag INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

INSERT_SQL = f"""
INSERT INTO {SCORES_TABLE} (transaction_id, score, fraud_flag)
VALUES %s
"""

def pg_conn():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
    )

def ensure_table():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()
    log.info(f"Ensured table exists: {SCORES_TABLE}")

def main():
    ensure_table()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_SCORES_TOPIC])
    log.info(f"Subscribed to topic: {KAFKA_SCORES_TOPIC}")

    batch = []
    BATCH_SIZE = 50
    POLL_TIMEOUT = 1.0

    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:
                # если накопили батч — сбросим
                if batch:
                    with pg_conn() as conn, conn.cursor() as cur:
                        execute_values(cur, INSERT_SQL, batch)
                        conn.commit()
                    consumer.commit(asynchronous=False)
                    log.info(f"Flushed batch: {len(batch)} rows")
                    batch.clear()
                continue

            if msg.error():
                log.error(f"Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                tx   = data.get("transaction_id")
                score = float(data["score"])
                flag  = int(data["fraud_flag"])
                batch.append((None if tx is None else str(tx), score, flag))

                if len(batch) >= BATCH_SIZE:
                    with pg_conn() as conn, conn.cursor() as cur:
                        execute_values(cur, INSERT_SQL, batch)
                        conn.commit()
                    consumer.commit(asynchronous=False)
                    log.info(f"Inserted {len(batch)} messages")
                    batch.clear()

            except Exception as e:
                log.exception(f"Process error: {e}")

    finally:
        try:
            consumer.close()
        except Exception:
            pass

if __name__ == "__main__":
    time.sleep(2) 
    main()
