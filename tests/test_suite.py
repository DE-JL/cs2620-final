import os
import pytest
import signal
import subprocess
import sys
import time

from concurrent.futures import ThreadPoolExecutor

# Add the root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from client import burst


class ServerManager:
    """Encapsulates starting/stopping servers for more direct usage in tests."""

    def __init__(self):
        self.load_balancer = None
        self.graders: dict[int, subprocess.Popen] = {}

    def start_all(self):
        """Start all servers."""
        self.load_balancer = subprocess.Popen(["python", "load_balancer.py"])
        self.graders[1] = subprocess.Popen(["python", "autograder.py",
                                            "--ip", "localhost",
                                            "--port", "60001"])
        self.graders[2] = subprocess.Popen(["python", "autograder.py",
                                            "--ip", "localhost",
                                            "--port", "60002"])

        time.sleep(1)
        print("started all servers")

    def stop_all(self):
        """Stop all servers."""
        self.load_balancer.send_signal(signal.SIGINT)
        self.graders[1].send_signal(signal.SIGINT)
        self.graders[2].send_signal(signal.SIGINT)

        try:
            self.load_balancer.wait(timeout=3)
            self.graders[1].wait(timeout=3)
            self.graders[2].wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.load_balancer.kill()
            self.graders[1].kill()
            self.graders[2].kill()

        print("killed all servers")


@pytest.fixture
def server_manager():
    """Pytest fixture that starts all servers and allows control during the test."""

    # Create the server manager and start all the servers
    manager = ServerManager()
    manager.start_all()

    # Yield the manager to the test case
    yield manager

    # Stop the servers
    manager.stop_all()


def test_burst(server_manager):
    executor = ThreadPoolExecutor(max_workers=10)
    burst(executor, 100)
