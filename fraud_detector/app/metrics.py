"""Метрики сервиса скоринга в формате Prometheus.

Все имена начинаются с scorer_, чтобы на дашбордах и в алертах было сразу
видно, какой сервис их отдаёт. Описание каждой метрики продублировано в README.
"""

from prometheus_client import Counter, Gauge, Histogram

# Скоры у модели сильно смещены к нулю, поэтому корзины у нуля мельче.
SCORE_BUCKETS = [0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 1.0]
LATENCY_BUCKETS = [0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0]
LAG_BUCKETS = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]

STAGES = ("decode", "preprocess", "predict", "produce")

TRANSACTIONS = Counter(
    "scorer_transactions",
    "Транзакции, которые сервис отскорил и отправил в топик scoring",
    ["model_version"],
)
FLAGGED = Counter(
    "scorer_flagged",
    "Транзакции, помеченные как фрод (score > порога)",
    ["model_version"],
)
ERRORS = Counter(
    "scorer_errors",
    "Сообщения, которые не удалось обработать, по стадии падения",
    ["stage"],
)
PROCESSING_SECONDS = Histogram(
    "scorer_processing_seconds",
    "Полное время обработки одной транзакции: от разбора JSON до отправки скора",
    buckets=LATENCY_BUCKETS,
)
STAGE_SECONDS = Histogram(
    "scorer_stage_seconds",
    "Время отдельных стадий обработки транзакции",
    ["stage"],
    buckets=LATENCY_BUCKETS,
)
EVENT_LAG_SECONDS = Histogram(
    "scorer_event_lag_seconds",
    "Задержка от записи транзакции в Kafka до готового скора",
    buckets=LAG_BUCKETS,
)
SCORE = Histogram(
    "scorer_score",
    "Распределение скоров мошенничества",
    buckets=SCORE_BUCKETS,
)
FRAUD_SHARE = Gauge(
    "scorer_fraud_share",
    "Доля помеченных как фрод среди последних N отскоренных транзакций",
)
FRAUD_SHARE_WINDOW = Gauge(
    "scorer_fraud_share_window_size",
    "Сколько транзакций сейчас в окне, по которому считается scorer_fraud_share",
)
MODEL_INFO = Gauge(
    "scorer_model_info",
    "Версия модели, которой сейчас скорит сервис (значение всегда 1)",
    ["version"],
)
MODEL_SWITCHES = Counter(
    "scorer_model_switches",
    "Горячая подмена модели: сервис подхватил новый файл без перезапуска",
    ["version"],
)

for _stage in STAGES:
    ERRORS.labels(stage=_stage)
for _stage in STAGES[1:]:
    STAGE_SECONDS.labels(stage=_stage)
