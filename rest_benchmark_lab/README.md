# REST API Benchmark Lab

Проект для лабораторной работы по нагрузочному тестированию REST API.

## Состав проекта

- `apps/app_fastapi.py` — приложение FastAPI;
- `apps/app_aiohttp.py` — приложение Aiohttp;
- `apps/app_tornado.py` — приложение Tornado;
- `apps/common.py` — общие функции для endpoint'ов;
- `scripts/run_benchmarks.py` — автоматический запуск Apache Benchmark;
- `scripts/generate_graphs.py` — генерация графиков;
- `scripts/ab_commands.sh` — команды `ab` для ручного запуска;
- `locustfile.py` — альтернативный сценарий для Locust;
- `results/benchmark_results.csv` — таблица результатов;
- `graphs/` — графики;
- `report.md` — готовый отчёт.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

На Windows:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Запуск серверов вручную

FastAPI:

```bash
cd apps
CPU_ITERATIONS=1000000 uvicorn app_fastapi:app --host 127.0.0.1 --port 8000 --workers 1
```

Aiohttp:

```bash
cd apps
CPU_ITERATIONS=1000000 python app_aiohttp.py 8001
```

Tornado:

```bash
cd apps
CPU_ITERATIONS=1000000 python app_tornado.py 8002
```

## Запуск тестов

```bash
python scripts/run_benchmarks.py
python scripts/generate_graphs.py
```

Для ручного запуска Apache Benchmark можно использовать команды из файла:

```bash
scripts/ab_commands.sh
```

## Отчёт

Готовый отчёт находится в файле `report.md`.
