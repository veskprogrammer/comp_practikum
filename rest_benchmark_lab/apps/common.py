import os
import time
import asyncio

CPU_ITERATIONS = int(os.getenv('CPU_ITERATIONS', '1000000'))
SLEEP_SECONDS = float(os.getenv('SLEEP_SECONDS', '0.1'))

def cpu_intensive_task(iterations: int = CPU_ITERATIONS) -> int:
    total = 0
    for i in range(iterations):
        total += i
    return total

def blocking_sleep():
    time.sleep(SLEEP_SECONDS)

async def async_sleep():
    await asyncio.sleep(SLEEP_SECONDS)
