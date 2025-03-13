import logging.config
import json
import os

# Define log directory and file
log_dir = "logs"
log_file = os.path.join(log_dir, "my_package.log")

# Ensure the log directory exists
os.makedirs(log_dir, exist_ok=True)

# Get the path of the logging.json file
log_config_path = os.path.join(os.path.dirname(__file__), "logging.json")

# Load JSON configuration if the file exists
if os.path.exists(log_config_path):
    with open(log_config_path, "r") as f:
        log_config = json.load(f)

    log_file = log_config["handlers"]["file"]["filename"]

    # Ensure the log directory exists
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)

    logging.config.dictConfig(log_config)
else:
    logging.basicConfig(level=logging.ERROR)  # Fallback if JSON config is missing

# Get the logger for the package
basic_logger = logging.getLogger("TM1_bedrock_py")
exec_metrics_logger = logging.getLogger("exec_metrics")

__all__ = ["basic_logger", "exec_metrics_logger"]
