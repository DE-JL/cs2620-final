#!/usr/bin/env python3
import grpc
import argparse
import time

from protos.autograder_pb2 import SubmissionRequest, SubmissionResponse
from protos.autograder_pb2_grpc import LoadBalancerStub
from config import LOAD_BALANCER

def send_request():
    # Construct the load balancer's address from configuration
    host = LOAD_BALANCER['host']
    port = LOAD_BALANCER['port']
    address = f"{host}:{port}"
    channel = grpc.insecure_channel(address)
    stub = LoadBalancerStub(channel)

    # Create a dummy SubmissionRequest.
    # Adjust and populate the fields as defined in your proto file.
    request = SubmissionRequest()
    request.task_id = 1
    request.source_code = "print('Hello, World!')"

    try:
        response: SubmissionResponse = stub.Submit(request)
        print("Response Received:")
        print("  status:", response.status)
        print("  output:", response.output)
    except grpc.RpcError as e:
        print("Encountered an error while calling the gRPC server:")
        print(e)
    finally:
        channel.close()

def main():
    parser = argparse.ArgumentParser(description="Synchronous gRPC Client for the Load Balancer")
    parser.add_argument("--freq", type=float, default=1, help="Request frequency, how many requests per second")
    args = parser.parse_args()

    period = 1 / args.freq

    while True:
        send_request()
        time.sleep(period)

if __name__ == "__main__":
    main()
