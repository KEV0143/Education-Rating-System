import logging
import re
import click
from datetime import datetime
from utils.services.runtime import runtime_data_dir

click.echo = lambda *args, **kwargs: None
click.secho = lambda *args, **kwargs: None

class CustomFormatter(logging.Formatter):
    def __init__(self, use_color=False):
        super().__init__()
        self.use_color = use_color

    def format(self, record):
        level = record.levelname.title()
        if level == "Warning":
            level = "Warn"
        level_str = f"[ {level} ]"

        now = datetime.fromtimestamp(record.created)
        date_str = now.strftime("%d-%m-%y")
        time_str = now.strftime("%H:%M:%S")

        message = record.getMessage()
        if record.name == "werkzeug":
            message = re.sub(r'\x1b\[[0-9;]*[mK]', '', message)
            match = re.search(r'"([^"]+)"\s+(\d+)', message)
            if match:
                req_path, status = match.groups()
                if self.use_color:
                    if status.startswith("2"):
                        status_str = f"\033[92m{status}\033[0m"
                    elif status.startswith("3"):
                        status_str = f"\033[96m{status}\033[0m"
                    elif status.startswith("4"):
                        status_str = f"\033[93m{status}\033[0m"
                    elif status.startswith("5"):
                        status_str = f"\033[91m{status}\033[0m"
                    else:
                        status_str = status
                else:
                    status_str = status
                message = f'"{req_path}" {status_str}'

        return f"{level_str} - [ {date_str} ] - [ {time_str} ] - {message}"

class StartupFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        clutter = [
            "WARNING: This is a development server",
            "Press CTRL+C to quit",
            "Restarting with",
            "Debugger is active",
            "Debugger PIN",
            "Serving Flask app",
            "Debug mode: on",
            "Running on http"
        ]
        for term in clutter:
            if term in msg:
                return False
        return True

def setup_custom_logging():
    try:
        import colorama
        colorama.init()
    except Exception:
        pass

    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter(use_color=True))
    handler.addFilter(StartupFilter())

    data_dir = runtime_data_dir("EducationRatingSystem")
    log_dir = data_dir / "db" / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setFormatter(CustomFormatter(use_color=False))
    file_handler.addFilter(StartupFilter())

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
    root_logger.addHandler(handler)
    root_logger.addHandler(file_handler)

    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.setLevel(logging.INFO)
    for h in list(werkzeug_logger.handlers):
        werkzeug_logger.removeHandler(h)
    werkzeug_logger.propagate = True
