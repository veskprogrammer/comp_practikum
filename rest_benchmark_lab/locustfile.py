from locust import HttpUser, task, between

class BenchmarkUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(5)
    def root(self):
        self.client.get("/")

    @task(2)
    def slow_blocking(self):
        self.client.get("/slow_endpoint")

    @task(3)
    def slow_fixed(self):
        self.client.get("/slow_endpoint_fixed")

    @task(1)
    def cpu_blocking(self):
        self.client.get("/high_cpu_endpoint")

    @task(1)
    def cpu_fixed(self):
        self.client.get("/high_cpu_endpoint_fixed")
