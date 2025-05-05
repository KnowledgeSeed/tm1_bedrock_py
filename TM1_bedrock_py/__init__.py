import logging.config
import json
import os
import re

__version__ = "0.4.1"

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


def update_version():
    version_file = os.path.join(os.path.dirname(__file__), '__init__.py')
    with open(version_file, 'r') as f:
        content = f.read()
    new_version = os.environ.get('VERSION')
    content_new = re.sub(r'__version__ = ["\'].*["\']', f'__version__ = "{new_version}"', content, 1)
    with open(version_file, 'w') as f:
        f.write(content_new)


def get_version():
    return __version__


def get_provider_info():
    return {
        "package-name": "TM1_bedrock_py",
        "name": "tm1_bedrock_py",
        "description": "A python modul for TM1 Bedrock.",
        "version": [get_version()],
    }
