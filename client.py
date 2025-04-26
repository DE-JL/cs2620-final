import grpc
import random
import time
from concurrent.futures import ALL_COMPLETED, CancelledError, ThreadPoolExecutor
from concurrent.futures import wait

from config import config
from protos.autograder_pb2 import SubmissionRequest, SubmissionResponse
from protos.autograder_pb2_grpc import LoadBalancerStub

MIN_DELAY = 15.0
MAX_DELAY = 20.0

MIN_BURST = 40
MAX_BURST = 200

TASK_FILES = [
    ("test_bank/solution_lower.py", 1),
    ("test_bank/solution_sort.py", 2),
    ("test_bank/solution_pal.py", 3),
]

SOURCES = [(tid, open(path, "r", encoding="utf-8").read())
           for path, tid in TASK_FILES]


def send_submission(task_id: int, source_code: str) -> None:
    lb_ip, lb_port = config["load_balancer"]
    with grpc.insecure_channel(f"{lb_ip}:{lb_port}") as ch:
        stub = LoadBalancerStub(ch)
        req = SubmissionRequest(task_id=task_id, source_code=source_code)
        try:
            resp: SubmissionResponse = stub.Submit(req)
            print(resp)
        except (grpc.RpcError, CancelledError) as e:
            print(e)


def burst(executor: ThreadPoolExecutor, n: int) -> None:
    futures = [
        executor.submit(send_submission, *random.choice(SOURCES))
        for _ in range(n)
    ]

    # block until every RPC in this burst finishes
    wait(futures, return_when=ALL_COMPLETED)


def main():
    executor = ThreadPoolExecutor(max_workers=MAX_BURST)
    try:
        for i in range(1, 3):
            n = random.randint(MIN_BURST, MAX_BURST)
            print(f"Burst #{i}: {n} requests")
            burst(executor, n)
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    except KeyboardInterrupt:
        print("Ctrl-C received - cancelling queued tasks ...")
        executor.shutdown(wait=False, cancel_futures=True)

    print("Client terminated.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Client terminated.")
