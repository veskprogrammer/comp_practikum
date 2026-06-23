from aiohttp import web
import asyncio
from common import cpu_intensive_task, blocking_sleep, async_sleep

async def root(request):
    return web.json_response({"framework": "aiohttp", "message": "ok"})

async def slow_endpoint(request):
    blocking_sleep()
    return web.json_response({"framework": "aiohttp", "message": "blocking sleep completed"})

async def slow_endpoint_fixed(request):
    await async_sleep()
    return web.json_response({"framework": "aiohttp", "message": "async sleep completed"})

async def high_cpu_endpoint(request):
    result = cpu_intensive_task()
    return web.json_response({"framework": "aiohttp", "result": result})

async def high_cpu_endpoint_fixed(request):
    result = await asyncio.to_thread(cpu_intensive_task)
    return web.json_response({"framework": "aiohttp", "result": result})

app = web.Application()
app.add_routes([
    web.get('/', root),
    web.get('/slow_endpoint', slow_endpoint),
    web.get('/slow_endpoint_fixed', slow_endpoint_fixed),
    web.get('/high_cpu_endpoint', high_cpu_endpoint),
    web.get('/high_cpu_endpoint_fixed', high_cpu_endpoint_fixed),
])

if __name__ == '__main__':
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    web.run_app(app, host='127.0.0.1', port=port)
