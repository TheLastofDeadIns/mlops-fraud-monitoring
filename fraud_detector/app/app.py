import json
import logging
import os
import sys
import time
from collections import deque

import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import TIMESTAMP_NOT_AVAILABLE
from prometheus_client import start_http_server

sys.path.append(os.path.abspath('./src'))  # noqa: PTH100
sys.path.append(os.path.dirname(os.path.abspath(__file__)))  # noqa: PTH100, PTH120
import metrics as m
from preprocessing import run_preproc
from scorer import THRESHOLD, ensure_bootstrap, make_pred


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
FRAUD_SHARE_WINDOW = int(os.getenv("FRAUD_SHARE_WINDOW", "500"))

# Поля транзакции, которые уходят в топик scoring рядом со скором: по ним
# Grafana строит срезы по штатам, мерчантам и категориям.
PASSTHROUGH_FIELDS = ("us_state", "merch", "cat_id", "amount")


class FraudShareWindow:
    """Доля фрода среди последних size транзакций."""

    def __init__(self, size):
        self.flags = deque(maxlen=size)
        self.flagged = 0

    def add(self, flag):
        if len(self.flags) == self.flags.maxlen:
            self.flagged -= self.flags[0]
        self.flags.append(flag)
        self.flagged += flag
        return self.flagged / len(self.flags)


class ModelVersionTracker:
    """Следит за версией модели и отмечает горячую подмену в метриках."""

    def __init__(self):
        self.current = None

    def observe(self, version):
        if version == self.current:
            return
        if self.current is None:
            logger.info("Скоринг стартовал на модели %s", version)
        else:
            m.MODEL_INFO.remove(self.current)
            m.MODEL_SWITCHES.labels(version=version).inc()
            logger.info("Модель сменилась: %s -> %s", self.current, version)
        m.MODEL_INFO.labels(version=version).set(1)
        self.current = version


def event_lag_seconds(msg):
    """Сколько секунд прошло с момента записи сообщения в Kafka."""
    ts_type, ts_ms = msg.timestamp()
    if ts_type == TIMESTAMP_NOT_AVAILABLE or ts_ms <= 0:
        return None
    return max(time.time() - ts_ms / 1000.0, 0.0)


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
        }
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)
        self.fraud_window = FraudShareWindow(FRAUD_SHARE_WINDOW)
        self.model_tracker = ModelVersionTracker()
        start_http_server(METRICS_PORT)
        logger.info("Prometheus метрики доступны на порту %s", METRICS_PORT)

    def process_message(self, msg):
        started = time.perf_counter()
        stage = "decode"
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            transaction_id = payload['transaction_id']
            data = payload['data']

            stage = "preprocess"
            with m.STAGE_SECONDS.labels(stage=stage).time():
                processed_df = run_preproc(pd.DataFrame([data]))

            stage = "predict"
            with m.STAGE_SECONDS.labels(stage=stage).time():
                _, y_proba, model_version = make_pred(processed_df, "kafka_stream")
            score = float(y_proba[0])
            fraud_flag = int(score > THRESHOLD)
            self.model_tracker.observe(model_version)

            result = {
                "transaction_id": transaction_id,
                "score": score,
                "fraud_flag": fraud_flag,
                "model_version": model_version,
            }
            for field in PASSTHROUGH_FIELDS:
                result[field] = data.get(field)

            # Отправка результата в топик scoring
            stage = "produce"
            with m.STAGE_SECONDS.labels(stage=stage).time():
                self.producer.produce(
                    SCORING_TOPIC,
                    key=transaction_id,
                    value=json.dumps([result]),
                )
                self.producer.flush()
        except Exception:
            m.ERRORS.labels(stage=stage).inc()
            logger.exception("Не удалось обработать сообщение на стадии %s", stage)
            return False

        m.TRANSACTIONS.labels(model_version=model_version).inc()
        if fraud_flag:
            m.FLAGGED.labels(model_version=model_version).inc()
        m.SCORE.observe(score)
        m.FRAUD_SHARE.set(self.fraud_window.add(fraud_flag))
        m.FRAUD_SHARE_WINDOW.set(len(self.fraud_window.flags))
        m.PROCESSING_SECONDS.observe(time.perf_counter() - started)
        lag = event_lag_seconds(msg)
        if lag is not None:
            m.EVENT_LAG_SECONDS.observe(lag)
        return True

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            self.process_message(msg)


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    ensure_bootstrap()
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
