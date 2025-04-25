import argparse
import asyncio
import logging
import os
import queue
import threading
import time

# Need to import config before gRPC
from job import Job
from grpc import aio

from protos.autograder_pb2 import SubmissionRequest, SubmissionResponse
from protos.autograder_pb2_grpc import AutograderServicer, add_AutograderServicer_to_server


class Autograder(AutograderServicer):
    MIN_WORKERS = 5
    SCALE_INTERVAL = 0.5
    SCALE_UP_THRESHOLD = 5
    SCALE_DOWN_PERCENTAGE = 0.2

    def __init__(self, server_id: str):
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

        # Latency trackers
        self.queueing_latency_tracker = EWMA()
        self.execution_latency_tracker = EWMA()

        # Autoscaler thread
        self.scaler_thread = threading.Thread(target=self.scale, daemon=True)
        self.scaler_thread.start()

    def stop(self):
        self.shutdown_event.set()
        self.scaler_thread.join()

        for worker in self.workers:
            worker.stop()

    async def Grade(self, request: SubmissionRequest, context: aio.ServicerContext) -> SubmissionResponse:
        with self.lock:
            # Check if we need to scale up
            job = Job(request)
            if all(worker.queue.qsize() >= Autograder.SCALE_UP_THRESHOLD for worker in self.workers):
                worker = Worker()
                self.workers.append(Worker())
                worker.queue.put(job)
            else:
                # Give it to the least busy worker
                worker = min(self.workers, key=lambda w: w.queue.qsize())
                worker.queue.put(job)

        # Wait asynchronously for the result
        result: SubmissionResponse = await job.future

        # Track latencies
        self.queueing_latency_tracker.add(job.get_queued_time())
        self.execution_latency_tracker.add(job.get_execution_time())

        return result

    def scale(self):
        while not self.shutdown_event.is_set():
            with self.lock:
                # Scale down if more than 25% are idle and we have more than MIN_WORKERS
                if len(self.workers) > Autograder.MIN_WORKERS:
                    idle_workers = [w for w in self.workers if w.queue.qsize() == 0]
                    idle_ratio = len(idle_workers) / len(self.workers)

                    if idle_ratio > Autograder.SCALE_DOWN_PERCENTAGE:
                        worker_to_remove = idle_workers[0]
                        worker_to_remove.stop()
                        self.workers.remove(worker_to_remove)

            # Log and sleep
            self.log()
            time.sleep(Autograder.SCALE_INTERVAL)

    def log(self):
        # Calculate the average saturation
        saturation_levels = [w.queue.qsize() / Autograder.SCALE_UP_THRESHOLD for w in self.workers]
        avg_saturation = sum(saturation_levels) / len(self.workers)

        # Calculate the latencies
        avg_queueing_latency = self.queueing_latency_tracker.get()
        avg_execution_latency = self.execution_latency_tracker.get()

        # Log information
        self.logger.info(f"\n[Stats]\n"
                         f"\tWorker count          : {len(self.workers)}\n"
                         f"\tAvg queue saturation  : {avg_saturation:.2f}\n"
                         f"\tAvg queueing latency  : {avg_queueing_latency:.2f} s\n"
                         f"\tAvg execution latency : {avg_execution_latency:.2f} s\n")


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


class EWMA:
    def __init__(self, alpha: float = 0.2):
        self.lock = threading.Lock()
        self.alpha = alpha
        self.avg = 0

    def add(self, value: float):
        with self.lock:
            self.avg = self.alpha * value + (1 - self.alpha) * self.avg

    def get(self) -> float:
        with self.lock:
            return self.avg


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
