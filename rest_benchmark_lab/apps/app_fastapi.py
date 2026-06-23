from fastapi import FastAPI
import asyncio
from common import cpu_intensive_task, blocking_sleep, async_sleep

app = FastAPI(title="FastAPI benchmark app")

@app.get("/")
async def read_root():
    return {"framework": "fastapi", "message": "ok"}

@app.get("/slow_endpoint")
async def slow_endpoint():
    # Плохой вариант: блокирует event loop.
    blocking_sleep()
    return {"framework": "fastapi", "message": "blocking sleep completed"}

@app.get("/slow_endpoint_fixed")
async def slow_endpoint_fixed():
    # Хороший вариант для I/O-bound ожидания: event loop остается свободным.
    await async_sleep()
    return {"framework": "fastapi", "message": "async sleep completed"}

@app.get("/high_cpu_endpoint")
async def high_cpu_endpoint():
    # Плохой вариант: CPU-задача выполняется прямо в event loop.
    result = cpu_intensive_task()
    return {"framework": "fastapi", "result": result}

@app.get("/high_cpu_endpoint_fixed")
async def high_cpu_endpoint_fixed():
    # Исправленный вариант: CPU-задача вынесена в отдельный поток.
    # Это не ускоряет сам расчет, но не блокирует event loop для других запросов.
    result = await asyncio.to_thread(cpu_intensive_task)
    return {"framework": "fastapi", "result": result}
