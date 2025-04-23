import yaml
import importlib.resources

config_path = importlib.resources.files(__package__) / "config.yaml"


with config_path.open() as f:
    config = yaml.safe_load(f)

DEBUG = config["debug"]
NETWOK = config["network"]
LOAD_BALANCER = config["load_balancer"]
MACHINES_LOCAL = config["machines_local"]
MACHINES_PUBLIC = config["machines_public"]

__all__ = ["DEBUG", "NETWORK", "LOAD_BALANCER", "MACHINES_LOCAL", "MACHINES_PUBLIC"]
