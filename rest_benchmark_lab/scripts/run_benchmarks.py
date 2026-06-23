import os
import re
import csv
import time
import signal
import subprocess
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]
APPS = ROOT / 'apps'
RESULTS = ROOT / 'results'
RESULTS.mkdir(exist_ok=True)

CPU_ITERATIONS = os.getenv('CPU_ITERATIONS', '1000000')
SLEEP_SECONDS = os.getenv('SLEEP_SECONDS', '0.1')

SERVERS = [
    {
        'framework': 'FastAPI',
        'port': 8000,
        'cmd': ['python3', '-m', 'uvicorn', 'app_fastapi:app', '--host', '127.0.0.1', '--port', '8000', '--workers', '1'],
    },
    {
        'framework': 'Aiohttp',
        'port': 8001,
        'cmd': ['python3', 'app_aiohttp.py', '8001'],
    },
    {
        'framework': 'Tornado',
        'port': 8002,
        'cmd': ['python3', 'app_tornado.py', '8002'],
    },
]

TESTS = [
    {'endpoint': '/', 'requests': 500, 'concurrency': 50, 'label': 'root'},
    {'endpoint': '/slow_endpoint', 'requests': 60, 'concurrency': 20, 'label': 'slow_blocking'},
    {'endpoint': '/slow_endpoint_fixed', 'requests': 100, 'concurrency': 20, 'label': 'slow_fixed'},
    {'endpoint': '/high_cpu_endpoint', 'requests': 30, 'concurrency': 10, 'label': 'cpu_blocking'},
    {'endpoint': '/high_cpu_endpoint_fixed', 'requests': 30, 'concurrency': 10, 'label': 'cpu_fixed'},
]

re_float = r'([0-9]+(?:\.[0-9]+)?)'

def wait_for_server(port, timeout=10):
    url = f'http://127.0.0.1:{port}/'
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, timeout=0.5)
            if r.status_code == 200:
                return True
        except Exception:
            time.sleep(0.1)
    return False

def parse_ab(output):
    def find(pattern, default=None, cast=float):
        m = re.search(pattern, output, re.MULTILINE)
        if not m:
            return default
        return cast(m.group(1))
    # Percentile table lines: "  95    105"
    p50 = find(r'^\s*50%\s+(\d+)', None, int)
    p95 = find(r'^\s*95%\s+(\d+)', None, int)
    p99 = find(r'^\s*99%\s+(\d+)', None, int)
    return {
        'complete_requests': find(r'Complete requests:\s+(\d+)', 0, int),
        'failed_requests': find(r'Failed requests:\s+(\d+)', 0, int),
        'rps': find(r'Requests per second:\s+' + re_float, 0.0, float),
        'mean_ms': find(r'Time per request:\s+' + re_float + r' \[ms\] \(mean\)', 0.0, float),
        'p50_ms': p50,
        'p95_ms': p95,
        'p99_ms': p99,
        'transfer_kb_sec': find(r'Transfer rate:\s+' + re_float, 0.0, float),
    }

def run_ab(url, n, c):
    cmd = ['ab', '-n', str(n), '-c', str(c), url]
    completed = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
    return completed.stdout

def main():
    rows = []
    env = os.environ.copy()
    env['PYTHONPATH'] = str(APPS)
    env['CPU_ITERATIONS'] = CPU_ITERATIONS
    env['SLEEP_SECONDS'] = SLEEP_SECONDS

    for srv in SERVERS:
        print(f"Starting {srv['framework']} on {srv['port']}")
        proc = subprocess.Popen(srv['cmd'], cwd=APPS, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, preexec_fn=os.setsid)
        try:
            if not wait_for_server(srv['port']):
                out = proc.stdout.read() if proc.stdout else ''
                raise RuntimeError(f"Server {srv['framework']} failed to start. Output:\n{out}")
            for test in TESTS:
                url = f"http://127.0.0.1:{srv['port']}{test['endpoint']}"
                print(f"  ab {srv['framework']} {test['endpoint']} n={test['requests']} c={test['concurrency']}")
                out = run_ab(url, test['requests'], test['concurrency'])
                raw_name = f"{srv['framework'].lower()}_{test['label']}.txt"
                (RESULTS / raw_name).write_text(out, encoding='utf-8')
                data = parse_ab(out)
                data.update({
                    'framework': srv['framework'],
                    'endpoint': test['endpoint'],
                    'label': test['label'],
                    'requests': test['requests'],
                    'concurrency': test['concurrency'],
                    'cpu_iterations': int(CPU_ITERATIONS),
                    'sleep_seconds': float(SLEEP_SECONDS),
                    'raw_output': raw_name,
                })
                rows.append(data)
        finally:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    pass

    csv_path = RESULTS / 'benchmark_results.csv'
    with csv_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {csv_path}")

if __name__ == '__main__':
    main()
