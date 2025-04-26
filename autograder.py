import argparse
import asyncio
import logging
import numpy as np
import os
import pandas as pd
import psutil
import queue
import threading
import time

from collections import deque
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter

# Need to import config before gRPC
from job import Job
from grpc import aio

from protos.autograder_pb2 import SubmissionRequest, SubmissionResponse
from protos.autograder_pb2_grpc import AutograderServicer, add_AutograderServicer_to_server


class Autograder(AutograderServicer):
    MIN_WORKERS = 5
    SCALE_INTERVAL = 0.5
    SCALE_UP_THRESHOLD = 4
    MAX_IDLE_RATIO = 0.2
    WINDOW_SIZE = 50

    def __init__(self, server_id: str):
        self.server_id = server_id

        # Ensure the logs/ directory exists
        os.makedirs("logs", exist_ok=True)

        # Initialize the logger
        log_filename = f"logs/autograder_{server_id}.log"
        logging.basicConfig(filename=log_filename,
                            filemode="w",
                            level=logging.INFO,
                            format="%(asctime)s %(levelname)s %(message)s")
        self.logger = logging.getLogger("autograder")

        # Initialize the state
        self.workers: list[Worker] = [Worker() for _ in range(Autograder.MIN_WORKERS)]
        self.shutdown_event = threading.Event()
        self.lock = threading.Lock()

        # Stats trackers
        self.proc = psutil.Process()
        self.rows = deque()
        self.latencies = deque(maxlen=Autograder.WINDOW_SIZE)

        # Autoscaler thread
        self.scaler_thread = threading.Thread(target=self.scale, daemon=True)
        self.scaler_thread.start()

        # Stat recorder thread
        self.stats_thread = threading.Thread(target=self.record_stats, daemon=True)
        self.stats_thread.start()

    def stop(self):
        self.shutdown_event.set()
        self.scaler_thread.join()

        for worker in self.workers:
            worker.stop()

        # Generate a plot
        df = pd.DataFrame(list(self.rows),
                          columns=[
                              "ts", "workers", "saturation", "cpu", "rss",
                              "q50", "q95", "q99", "e50", "e95", "e99"
                          ])
        render_plots(df, f"logs/autograder_{self.server_id}_stats.png")

    async def Grade(self, request: SubmissionRequest, context: aio.ServicerContext) -> SubmissionResponse:
        with self.lock:
            # Check if we need to scale up
            job = Job(request)
            if all(worker.queue.qsize() >= Autograder.SCALE_UP_THRESHOLD for worker in self.workers):
                worker = Worker()
                self.workers.append(worker)
                worker.queue.put(job)
            else:
                # Give it to the least busy worker
                worker = min(self.workers, key=lambda w: w.queue.qsize())
                worker.queue.put(job)

        # Wait asynchronously for the result
        result: SubmissionResponse = await job.future
        self.latencies.append((job.get_queued_time(), job.get_execution_time()))

        return result

    def scale(self):
        while not self.shutdown_event.is_set():
            with self.lock:
                # Scale down if more than 25% are idle and we have more than MIN_WORKERS
                if len(self.workers) > Autograder.MIN_WORKERS:
                    idle_workers = [w for w in self.workers if w.queue.qsize() == 0]
                    idle_ratio = len(idle_workers) / len(self.workers)

                    if idle_ratio > Autograder.MAX_IDLE_RATIO:
                        worker_to_remove = idle_workers[0]
                        worker_to_remove.stop()
                        self.workers.remove(worker_to_remove)

            # Log and sleep
            time.sleep(Autograder.SCALE_INTERVAL)

    def record_stats(self):
        while not self.shutdown_event.is_set():
            # Compute saturation levels
            saturation_levels = [w.queue.qsize() / Autograder.SCALE_UP_THRESHOLD for w in self.workers]
            avg_saturation = sum(saturation_levels) / len(self.workers)

            # Compute latency percentiles
            if self.latencies:
                lat = np.array(self.latencies)
                q50, q95, q99 = np.percentile(lat[:, 0], (50, 95, 99))
                e50, e95, e99 = np.percentile(lat[:, 1], (50, 95, 99))
            else:
                q50 = q95 = q99 = e50 = e95 = e99 = 0.0

            # psutil.Process.cpu_percent() can exceed 100 on multicore boxes.
            # Divide by cpu_count() to normalise to 0-100 % of the whole machine.
            # Correct 0–1 normalization
            # cores = psutil.cpu_count(logical=True)
            cpu_usage = self.proc.cpu_percent()

            # Memory usage
            memory_usage = self.proc.memory_info().rss / 1_048_576

            self.rows.append((time.time(),
                              len(self.workers),
                              avg_saturation,
                              cpu_usage,
                              memory_usage,
                              q50, q95, q99, e50, e95, e99))
            time.sleep(0.5)


def render_plots(df: pd.DataFrame, filename: str):
    fig, ax = plt.subplots(5, 1,
                           figsize=(10, 9),
                           sharex=True,
                           constrained_layout=True)

    t0 = df["ts"].iloc[0]
    elapsed = df["ts"] - t0

    ax[0].plot(elapsed, df["workers"])
    ax[0].set_ylabel("Worker count")
    ax[0].set_ylim(bottom=0)

    ax[1].plot(elapsed, df["saturation"])
    ax[1].set_ylabel("Average saturation")
    ax[1].set_ylim(0, 1)

    ax[2].plot(elapsed, df["cpu"])
    ax[2].set_ylabel("CPU Utilization %")
    ax[2].set_ylim(bottom=0)

    ax[3].plot(elapsed, df["rss"])
    ax[3].set_ylabel("Memory usage (MB)")

    ax[4].plot(elapsed, df["q50"], label="queue p50")
    ax[4].plot(elapsed, df["q95"], label="queue p95")
    ax[4].plot(elapsed, df["q99"], label="queue p99")
    ax[4].plot(elapsed, df["e50"], label="exec p50")
    ax[4].plot(elapsed, df["e95"], label="exec p95")
    ax[4].plot(elapsed, df["e99"], label="exec p99")

    ax[4].set_ylabel("seconds")
    ax[4].set_ylim(bottom=0)
    ax[4].legend(ncol=3, fontsize="small")
    ax[4].set_xlabel("Elapsed time (mm:ss)")

    # ── nice mm:ss tick labels on *all* axes -------------------------------
    fmt = FuncFormatter(lambda x, _pos: f"{int(x // 60):02d}:{int(x % 60):02d}")
    for a in ax:
        a.xaxis.set_major_formatter(fmt)

    # hide tick-number text on the upper four panels
    for a in ax[:-1]:
        a.tick_params(labelbottom=False)

    plt.savefig(filename, dpi=120)
    plt.close(fig)


class Worker:
    def __init__(self):
        self.lock = threading.Lock()
        self.queue: queue.Queue[Job] = queue.Queue()
        self.shutdown_event = threading.Event()

        self.service_thread = threading.Thread(target=self.serve, daemon=True)
        self.service_thread.start()

    def stop(self):
        self.shutdown_event.set()
        self.service_thread.join()

    def serve(self):
        while not self.shutdown_event.is_set():
            try:
                job = self.queue.get(timeout=0.1)
                job.run()
            except queue.Empty:
                continue


async def serve(ip: str, port: int):
    server = aio.server()
    autograder = Autograder(f"{ip}:{port}")
    add_AutograderServicer_to_server(autograder, server)
    server.add_insecure_port(f"{ip}:{port}")

    await server.start()
    print(f"Autograder server started on {ip}:{port}")
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        print("Received cancellation. Shutting down server...")
        autograder.stop()
        print("Autograder shut down.")
        await server.stop(grace=1)
        raise


def main():
    parser = argparse.ArgumentParser(description="Start the autograder.")
    parser.add_argument("--ip", required=True, help="IP address of the autograder")
    parser.add_argument("--port", required=True, type=int, help="Port of the autograder")
    args = parser.parse_args()

    try:
        asyncio.run(serve(args.ip, args.port))
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Exiting.")


if __name__ == '__main__':
    main()
