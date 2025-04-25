import argparse
import asyncio
import logging
import random
import os

# Need to import config before gRPC
from config import config
from grpc import aio, RpcError

from protos.autograder_pb2 import SubmissionRequest, SubmissionResponse, Status
from protos.autograder_pb2_grpc import LoadBalancerServicer, AutograderStub, add_LoadBalancerServicer_to_server


class LoadBalancer(LoadBalancerServicer):
    def __init__(self, server_id: str):
        # Ensure the logs/ directory exists
        os.makedirs("logs", exist_ok=True)

        # Initialize the logger
        log_filename = f"logs/load_balancer_{server_id}.log"
        logging.basicConfig(filename=log_filename,
                            filemode="w",
                            level=logging.INFO,
                            format="%(asctime)s %(levelname)s %(message)s")
        self.logger = logging.getLogger("autograder")

        # Initialize the list of autograder servers
        self.autograders: list[tuple[str, int]] = config["autograders"]

    async def Submit(self, request: SubmissionRequest, context: aio.ServicerContext) -> SubmissionResponse:
        # Choose a random server
        host, port = random.choice(self.autograders)
        autograder_address = f"{host}:{port}"

        # Log the incoming request
        self.logger.info(f"Received submission for task {request.task_id} -> routing to {autograder_address}")

        try:
            async with aio.insecure_channel(autograder_address) as channel:
                stub = AutograderStub(channel)
                response = await stub.Grade(request)
                return response

        except RpcError as e:
            self.logger.error(f"gRPC call to {autograder_address} failed: {e}")

        return SubmissionResponse(status=Status.ERROR,
                                  output="gRPC call failed.")


async def serve(ip: str, port: int):
    server = aio.server()
    load_balancer = LoadBalancer(f"{ip}:{port}")
    add_LoadBalancerServicer_to_server(load_balancer, server)
    server.add_insecure_port(f"{ip}:{port}")

    await server.start()
    print(f"Load balancer started on {ip}:{port}")
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        print("Received cancellation. Shutting down load balancer...")
        await server.stop(grace=1)
        print("Load balancer shut down.")
        raise


def main():
    # parser = argparse.ArgumentParser(description="Start the load balancer.")
    # parser.add_argument("--ip", required=True, help="IP address to bind to")
    # parser.add_argument("--port", required=True, type=int, help="Port to listen on")
    # args = parser.parse_args()

    load_balancer_address: tuple[str, int] = config["load_balancer"]
    ip, port = load_balancer_address

    try:
        asyncio.run(serve(ip, port))
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Exiting.")


if __name__ == "__main__":
    main()
