import json
import asyncio
import tornado.ioloop
import tornado.web
from common import cpu_intensive_task, blocking_sleep, async_sleep

class JsonHandler(tornado.web.RequestHandler):
    def write_json(self, data):
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(data))

class RootHandler(JsonHandler):
    async def get(self):
        self.write_json({"framework": "tornado", "message": "ok"})

class SlowEndpointHandler(JsonHandler):
    async def get(self):
        blocking_sleep()
        self.write_json({"framework": "tornado", "message": "blocking sleep completed"})

class SlowEndpointFixedHandler(JsonHandler):
    async def get(self):
        await async_sleep()
        self.write_json({"framework": "tornado", "message": "async sleep completed"})

class HighCpuEndpointHandler(JsonHandler):
    async def get(self):
        result = cpu_intensive_task()
        self.write_json({"framework": "tornado", "result": result})

class HighCpuEndpointFixedHandler(JsonHandler):
    async def get(self):
        result = await asyncio.to_thread(cpu_intensive_task)
        self.write_json({"framework": "tornado", "result": result})

def make_app():
    return tornado.web.Application([
        (r"/", RootHandler),
        (r"/slow_endpoint", SlowEndpointHandler),
        (r"/slow_endpoint_fixed", SlowEndpointFixedHandler),
        (r"/high_cpu_endpoint", HighCpuEndpointHandler),
        (r"/high_cpu_endpoint_fixed", HighCpuEndpointFixedHandler),
    ])

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8002
    app = make_app()
    app.listen(port, address="127.0.0.1")
    tornado.ioloop.IOLoop.current().start()
