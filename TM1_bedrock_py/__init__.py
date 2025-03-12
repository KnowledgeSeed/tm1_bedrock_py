import logging.config
import json
import os

# Get the path of the logging.json file
log_config_path = os.path.join(os.path.dirname(__file__), "logging.json")

# Load JSON configuration if the file exists
if os.path.exists(log_config_path):
    with open(log_config_path, "r") as f:
        log_config = json.load(f)
    logging.config.dictConfig(log_config)
else:
    logging.basicConfig(level=logging.INFO)  # Fallback if JSON config is missing

# Get the logger for the package
logger = logging.getLogger("TM1_bedrock_py")

__all__ = ["logger"]
