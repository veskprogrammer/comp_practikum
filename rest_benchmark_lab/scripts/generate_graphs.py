from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'results'
GRAPHS = ROOT / 'graphs'
GRAPHS.mkdir(exist_ok=True)

df = pd.read_csv(RESULTS / 'benchmark_results.csv')
labels = {
    'root': 'GET /',
    'slow_blocking': 'slow blocking',
    'slow_fixed': 'slow fixed',
    'cpu_blocking': 'CPU blocking',
    'cpu_fixed': 'CPU fixed',
}
df['test'] = df['label'].map(labels)
order = ['root', 'slow_blocking', 'slow_fixed', 'cpu_blocking', 'cpu_fixed']

def grouped_bar(metric: str, title: str, ylabel: str, filename: str):
    pivot = df.pivot(index='test', columns='framework', values=metric).loc[[labels[x] for x in order]]
    ax = pivot.plot(kind='bar', figsize=(11, 6), rot=20)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel('Сценарий тестирования')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(GRAPHS / filename, dpi=180)
    plt.close()

grouped_bar('rps', 'Пропускная способность REST-фреймворков', 'Requests per second', 'rps_by_framework.png')
grouped_bar('mean_ms', 'Среднее время ответа', 'Latency, ms', 'mean_latency_by_framework.png')
grouped_bar('p95_ms', '95-й процентиль времени ответа', 'p95 latency, ms', 'p95_latency_by_framework.png')

# График, который отдельно показывает эффект исправления slow_endpoint.
slow = df[df['label'].isin(['slow_blocking', 'slow_fixed'])].copy()
slow['test'] = slow['label'].map(labels)
pivot = slow.pivot(index='framework', columns='test', values='rps')
ax = pivot.plot(kind='bar', figsize=(9, 5), rot=0)
ax.set_title('Эффект замены time.sleep на await asyncio.sleep')
ax.set_ylabel('Requests per second')
ax.set_xlabel('Фреймворк')
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(GRAPHS / 'slow_endpoint_effect.png', dpi=180)
plt.close()

print('Graphs saved to', GRAPHS)
