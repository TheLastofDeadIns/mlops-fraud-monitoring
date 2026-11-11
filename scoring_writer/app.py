import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, start_http_server

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
SCORE_BUCKETS = [0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 1.0]
WRITE_BUCKETS = [0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]

WRITTEN = Counter(
    'writer_scores_written',
    'Скоры, записанные в таблицу scores',
    ['model_version'],
)
FRAUD_WRITTEN = Counter(
    'writer_fraud_written',
    'Записанные транзакции с fraud_flag = 1',
    ['model_version'],
)
DB_WRITE_SECONDS = Histogram(
    'writer_db_write_seconds',
    'Время INSERT и COMMIT одного скора',
    buckets=WRITE_BUCKETS,
)
WRITTEN_SCORE = Histogram(
    'writer_score',
    'Распределение записанных скоров',
    buckets=SCORE_BUCKETS,
)
ERRORS = Counter(
    'writer_errors',
    'Сообщения, которые не удалось записать, по причине',
    ['reason'],
)
DB_RECONNECTS = Counter(
    'writer_db_reconnects',
    'Переподключения к PostgreSQL после ошибки соединения',
)

for _reason in ("bad_message", "db_error"):
    ERRORS.labels(reason=_reason)

REQUIRED_KEYS = ('transaction_id', 'score', 'fraud_flag')


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def connect_db():
    config = get_db_config()
    for attempt in range(1, 31):
        try:
            return psycopg2.connect(**config)
        except psycopg2.OperationalError as exc:
            logger.info("Postgres ещё не готов (%s/30): %s", attempt, exc)
            time.sleep(2)
    return psycopg2.connect(**config)


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scores (
                id SERIAL PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                score FLOAT NOT NULL,
                fraud_flag INT NOT NULL,
                model_version TEXT,
                us_state TEXT,
                merch TEXT,
                cat_id TEXT,
                amount DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Таблица могла остаться от прошлых заданий без новых колонок
        for column, column_type in (
            ("model_version", "TEXT"),
            ("us_state", "TEXT"),
            ("merch", "TEXT"),
            ("cat_id", "TEXT"),
            ("amount", "DOUBLE PRECISION"),
        ):
            cur.execute(f"ALTER TABLE scores ADD COLUMN IF NOT EXISTS {column} {column_type};")
        cur.execute("CREATE INDEX IF NOT EXISTS scores_created_at_idx ON scores (created_at);")
        cur.execute("CREATE INDEX IF NOT EXISTS scores_model_version_idx ON scores (model_version);")
        conn.commit()
        logger.info("Таблица 'scores' проверена или создана.")


def insert_score(conn, data):
    started = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO scores (transaction_id, score, fraud_flag, model_version, us_state, merch, cat_id, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            data["transaction_id"],
            data["score"],
            data["fraud_flag"],
            data.get("model_version"),
            data.get("us_state"),
            data.get("merch"),
            data.get("cat_id"),
            data.get("amount"),
        ))
        conn.commit()
    DB_WRITE_SECONDS.observe(time.perf_counter() - started)

    version = data.get("model_version") or "unknown"
    WRITTEN.labels(model_version=version).inc()
    WRITTEN_SCORE.observe(data["score"])
    if data["fraud_flag"] == 1:
        FRAUD_WRITTEN.labels(model_version=version).inc()


def run_consumer():
    start_http_server(METRICS_PORT)
    logger.info("Prometheus метрики доступны на порту %s", METRICS_PORT)

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    scoring_topic = os.getenv("KAFKA_SCORING_TOPIC")

    consumer_config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'scoring-writer',
        'auto.offset.reset': 'earliest',
    }

    logger.info(f"Подключение к Kafka: {kafka_bootstrap_servers}, топик: {scoring_topic}")
    consumer = Consumer(consumer_config)
    consumer.subscribe([scoring_topic])

    conn = connect_db()
    create_table(conn)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                logger.debug("Ожидание сообщений...")
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                data_list = json.loads(msg.value().decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError):
                ERRORS.labels(reason="bad_message").inc()
                logger.exception("Ошибка декодирования JSON")
                continue
            if not isinstance(data_list, list):
                ERRORS.labels(reason="bad_message").inc()
                logger.error(f"Ожидался список, получен: {type(data_list)}")
                continue

            for data in data_list:
                if not isinstance(data, dict) or not all(k in data for k in REQUIRED_KEYS):
                    ERRORS.labels(reason="bad_message").inc()
                    logger.error(f"Некорректный формат элемента: {data}")
                    continue
                try:
                    insert_score(conn, data)
                    logger.info(f"Записано в БД: {data['transaction_id']}")
                except psycopg2.Error:
                    ERRORS.labels(reason="db_error").inc()
                    logger.exception("Ошибка записи в PostgreSQL")
                    if conn.closed:
                        DB_RECONNECTS.inc()
                        conn = connect_db()
                    else:
                        conn.rollback()

    except KeyboardInterrupt:
        logger.info("Потребитель остановлен пользователем.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    logger.info("Запуск потребителя и писателя в БД...")
    run_consumer()
