import grpc
import threading

from config import config
from protos.autograder_pb2 import SubmissionRequest, SubmissionResponse
from protos.autograder_pb2_grpc import LoadBalancerStub


def submit(task_id: int, source_code: str):
    # Connect to the server
    load_balancer_address: tuple[str, int] = config["load_balancer"]
    ip, port = load_balancer_address

    with grpc.insecure_channel(f"{ip}:{port}") as channel:
        print("Client connected to load balancer")
        stub = LoadBalancerStub(channel)

        # Create a submission message
        req = SubmissionRequest(task_id=task_id,
                                source_code=source_code)

        # Send the request and get the response
        response: SubmissionResponse = stub.Submit(req)
        print(response)


def main():
    task_id = 1
    code = """
    s = input()
    print(s.lower())
    """

    num_threads = 500
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=submit, args=(task_id, code))
        t.start()
        threads.append(t)

    # Optionally wait for all threads to complete
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
