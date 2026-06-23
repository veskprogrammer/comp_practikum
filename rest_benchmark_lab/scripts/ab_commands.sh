#!/usr/bin/env bash
# Команды для ручного повторения тестов Apache Benchmark.
# Серверы нужно запускать отдельно из папки apps.

# FastAPI:
# CPU_ITERATIONS=1000000 uvicorn app_fastapi:app --host 127.0.0.1 --port 8000 --workers 1
# Aiohttp:
# CPU_ITERATIONS=1000000 python app_aiohttp.py 8001
# Tornado:
# CPU_ITERATIONS=1000000 python app_tornado.py 8002

ab -n 500 -c 50 http://127.0.0.1:8000/
ab -n 60 -c 20 http://127.0.0.1:8000/slow_endpoint
ab -n 100 -c 20 http://127.0.0.1:8000/slow_endpoint_fixed
ab -n 30 -c 10 http://127.0.0.1:8000/high_cpu_endpoint
ab -n 30 -c 10 http://127.0.0.1:8000/high_cpu_endpoint_fixed
