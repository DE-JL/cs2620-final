import sys
import asyncio
from grpc import aio
import argparse


from concurrent import futures

from protos.autograder_pb2 import SubmissionRequest, SubmissionResponse, Status
from protos.autograder_pb2_grpc import LoadBalancerServicer, AutograderStub
import asyncio
from grpc import aio
import grpc


import utils

from config import LOAD_BALANCER, DEBUG

class LoadBalancerServer(LoadBalancerServicer):
    def __init__(self, machine_map):
        self.num_machines = len(machine_map)
        self.machine_map = machine_map
        self.machine_ids = list(self.machine_map.keys())

        self.current_machine_idx = 0

    def Submit(self, request: SubmissionRequest, context: grpc.ServicerContext) -> SubmissionResponse:
        address = self.get_machine_address()

        response = None

        try:
            channel = grpc.insecure_channel(address)
            stub = AutograderStub(channel)
            response = stub.Submit(request)    
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNKNOWN:
                print(e)
                sys.exit(1)
        finally:
            if channel is not None:
                channel.close()
        
        # advance machine idx ptr to next machine
        self.current_machine_idx = (self.current_machine_idx + 1) % self.num_machines

        # if no response from Autograder, return error
        if response is None:
            response = SubmissionResponse(status=Status.ERROR, output="No response from Autograder")

        return response
    
    def get_machine_address(self):
        data = self.machine_map[self.machine_ids[self.current_machine_idx]]
        return f"{data['host']}:{data['port']}"


def serve_blocking():
    # load config variables
    host = LOAD_BALANCER['host']
    port = LOAD_BALANCER['port']
    address = f"{host}:{port}"

    # load worker machine configs for tracking Autograder workers
    machine_map = utils.get_id_to_addr_map()

    # Create a GRPC server and bind
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

    if server.add_insecure_port(address) == 0:
        raise ValueError(f"Failed to bind to port {address}")
    print(f"[Load Balancer] Bound to {address}...")

    # Add the LoadBalancer server to the GRPC server
    lb_server = LoadBalancerServer(machine_map)
    LoadBalancerServicer.add_to_server(lb_server, server)
    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(3)


class AsyncLoadBalancerServer(LoadBalancerServicer):
    def __init__(self, machine_map):
        self.num_machines = len(machine_map)
        self.machine_map = machine_map
        self.machine_ids = list(self.machine_map.keys())
        self.current_machine_idx = 0
        self.lock = asyncio.Lock()  # Async lock

    async def Submit(self, request: SubmissionRequest, context: grpc.aio.ServicerContext) -> SubmissionResponse:
        async with self.lock:
            address = self.get_machine_address()
            self.current_machine_idx = (self.current_machine_idx + 1) % self.num_machines

        response = None
        # Using an asynchronous channel
        async with grpc.aio.insecure_channel(address) as channel:
            stub = AutograderStub(channel)
            try:
                response = await stub.Submit(request)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNKNOWN:
                    print(e)
                    sys.exit(1)

        if response is None:
            response = SubmissionResponse(status=Status.ERROR, output="No response from Autograder")
        return response

    def get_machine_address(self):
        data = self.machine_map[self.machine_ids[self.current_machine_idx]]
        return f"{data['host']}:{data['port']}"

async def serve():
    host = LOAD_BALANCER['host']
    port = LOAD_BALANCER['port']
    address = f"{host}:{port}"

    machine_map = utils.get_id_to_addr_map()
    server = grpc.aio.server()

    # Add our AsyncLoadBalancerServer to the asynchronous server
    lb_server = AsyncLoadBalancerServer(machine_map)
    LoadBalancerServicer.add_to_server(lb_server, server)
    if server.add_insecure_port(address) == 0:
        raise ValueError(f"Failed to bind to port {address}")
    print(f"[Async Load Balancer] Bound to {address}...")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--asy", action="store_true", help="Reset the database if specified.")
    args = parser.parse_args()

    if args.asy:
        asyncio.run(serve())
    else:
        serve_blocking()
