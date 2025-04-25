import json
import os

# Set the gRPC verbosity
os.environ["GRPC_VERBOSITY"] = "ERROR"

_config_path = os.path.join(os.path.dirname(__file__), "config.json")
with open(_config_path, "r") as f:
    config: dict = json.load(f)
