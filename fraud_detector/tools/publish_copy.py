"""Быстрая публикация новой версии модели для проверки горячей подмены.

Копирует текущую модель под новым именем версии и атомарно переписывает
latest.txt, как это делает задача publish_model в DAG retrain_catboost.
Веса не меняются, зато не нужно ждать переобучения: через пару секунд
скоринг подхватит версию, а на дашборде появится отметка.

Запуск: docker compose exec fraud_detector python tools/publish_copy.py
"""

import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

MODELS_DIR = Path(os.getenv("MODELS_DIR", "/models"))


def main():
    latest = MODELS_DIR / "latest.txt"
    current = latest.read_text(encoding="utf-8").strip()
    source = MODELS_DIR / f"my_catboost_{current}.cbm"
    if not source.exists():
        sys.exit(f"Нет файла текущей модели: {source}")

    version = "manual_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    shutil.copyfile(source, MODELS_DIR / f"my_catboost_{version}.cbm")
    tmp = MODELS_DIR / f".latest.{version}.txt"
    tmp.write_text(version + "\n", encoding="utf-8")
    os.replace(tmp, latest)
    print(f"{current} -> {version}")


if __name__ == "__main__":
    main()
