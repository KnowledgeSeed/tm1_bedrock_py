import json
import logging.config
import os
import re
from datetime import datetime

__version__ = "0.4.1"

# Get the path of the logging.json file
log_config_path = os.path.join(os.path.dirname(__file__), "logging.json")

# Load JSON configuration if the file exists
if os.path.exists(log_config_path):
    with open(log_config_path, "r") as f:
        log_config = json.load(f)

    log_directory = os.getenv("TM1_BEDROCK_LOG_DIR")
    if log_directory is None:
        log_directory = log_config["handlers"]["file"]["filename"]

    try:
        log_directory = os.path.abspath(log_directory)
        os.makedirs(log_directory, exist_ok=True)

        handlers = log_config.get("handlers", {})
        timestamp = datetime.now().strftime("%Y-%m-%d")
        for handler_config in handlers.values():
            filename = handler_config.get("filename")
            if not filename:
                continue
            base_name = os.path.basename(filename)
            name, ext = os.path.splitext(base_name)
            handler_config["filename"] = os.path.join(
                log_directory,
                f"{name}_{timestamp}{ext}"
            )
        logging.config.dictConfig(log_config)

    except Exception as e:
        loggers = log_config.get("loggers", {})
        for logger_config in loggers.values():
            handlers = logger_config.get("handlers", [])
            logger_config["handlers"] = [
                name for name in handlers
                if "console" in name
            ]
        logging.config.dictConfig(log_config)

else:
    logging.basicConfig(level=logging.ERROR)  # Fallback if JSON config is missing

# Get the logger for the package
basic_logger = logging.getLogger("TM1_bedrock_py")
exec_metrics_logger = logging.getLogger("exec_metrics")
benchmark_metrics_logger = logging.getLogger("benchmark_metrics")

_MISSING_AIRFLOW_EXTRA_MSG = (
    "TM1 Bedrock Airflow integration requires optional dependencies. "
    "Install them with 'pip install tm1-bedrock-py[airflow]' or the more "
    "specific backend extras (e.g. '[airflow-postgres]')."
)
_AIRFLOW_EXECUTOR_MODULE = None

__all__ = ["basic_logger", "exec_metrics_logger", "benchmark_metrics_logger"]


def update_version(new_version):
    version_file = os.path.join(os.path.dirname(__file__), '__init__.py')
    with open(version_file, 'r') as f:
        content = f.read()
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


def _load_airflow_executor():
    global _AIRFLOW_EXECUTOR_MODULE

    if _AIRFLOW_EXECUTOR_MODULE is not None:
        return _AIRFLOW_EXECUTOR_MODULE

    try:
        from . import airflow_executor as _module  # noqa: F401
    except ImportError as exc:
        raise ImportError(_MISSING_AIRFLOW_EXTRA_MSG) from exc

    _AIRFLOW_EXECUTOR_MODULE = _module
    return _AIRFLOW_EXECUTOR_MODULE


def __getattr__(name):
    if name == "airflow_executor":
        return _load_airflow_executor()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__():
    return sorted(list(globals().keys()) + ["airflow_executor"])
