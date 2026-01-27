import logging
import sys
from pathlib import Path
import socket
import subprocess
import os
from functools import wraps

def get_git_info():
    """Retrieve Git project name and current branch."""
    try:
        repo_name = os.path.basename(os.getcwd())
        branch_name = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            text=True
        ).strip()
        return repo_name, branch_name
    except Exception:
        return "Unknown_Project", "Unknown_Branch"


def get_vm_name():
    """Retrieve the name of the VM (hostname)."""
    try:
        return socket.gethostname()
    except Exception:
        return "Unknown_VM"

def _in_airflow() -> bool:
    # cheap heuristic; any of these envs exist under Airflow workers
    return (
        "AIRFLOW_CTX_DAG_ID" in os.environ
        or "AIRFLOW_HOME" in os.environ
        or any(k.startswith("AIRFLOW__") for k in os.environ)
        or os.environ.get("CG_LOG_SIMPLE") == "1"   # manual override
    )

def add_traceback(log_method):
    """
    Decorator to add traceback info to error logs by default.
    """

    @wraps(log_method)
    def wrapper(*args, **kwargs):
        # Ensure `exc_info=True` is passed for errors and exceptions
        if 'exc_info' not in kwargs:
            kwargs['exc_info'] = True
        return log_method(*args, **kwargs)

    return wrapper

def setup_logger(
    logger_name: str = __name__,
    log_dir: str | None = None,
    level: int = logging.DEBUG
) -> logging.Logger:
    """
    Console logs:
      - Under Airflow (or CG_LOG_SIMPLE=1): NO timestamp (Airflow adds its own)
      - Otherwise: include timestamp

    File logs:
      - Always include timestamp -> log/log.log
    """
    if log_dir is None:
        main_module = sys.modules.get("__main__")
        if main_module and hasattr(main_module, "__file__"):
            main_script_path = Path(main_module.__file__).resolve()
            path_dir = main_script_path.parent / "log"
        else:
            path_dir = Path.cwd() / "log"
    else:
        path_dir = Path(log_dir)
    path_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = path_dir / "log.log"

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Avoid duplicate handlers if called multiple times
    if not logger.handlers:
        simple_console = _in_airflow()

        # Console formatter: drop timestamp under Airflow to avoid double datetime
        if simple_console:
            console_fmt = "%(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
            console_formatter = logging.Formatter(console_fmt)
        else:
            console_fmt = "%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
            console_formatter = logging.Formatter(console_fmt, datefmt="%Y-%m-%dT%H:%M:%S%z")

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(console_formatter)
        logger.addHandler(stream_handler)

        # File handler always keeps timestamps
        file_fmt = "%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
        file_formatter = logging.Formatter(file_fmt, datefmt="%Y-%m-%dT%H:%M:%S%z")
        file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Don’t bubble to root (prevents duplicated lines)
        logger.propagate = False

        logger.error = add_traceback(logger.error)
        logger.exception = add_traceback(logger.exception)

    return logger


def log_exception(exc_type, exc_value, exc_traceback):
    logger = logging.getLogger(__name__)
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


# Default configuration
logger = setup_logger()
sys.excepthook = log_exception

# NEW VERSION TO TEST---------------------------------
# import logging
# import sys
# from pathlib import Path
# import socket
# import subprocess
# import os
# from functools import wraps
# from functools import lru_cache
# from typing import Iterable
#
# # -----------------------
# # Root detection (no env)
# # -----------------------
# DEFAULT_ROOT_MARKERS = (
#     ".git", ".gitignore",
#     "pyproject.toml", "poetry.lock",
#     "requirements.txt",
#     "setup.cfg", "setup.py",
#     "README.md", "README.rst",
#     ".venv", ".python-version",
# )
#
# @lru_cache(maxsize=None)
# def detect_project_root(
#     start: Path | None = None,
#     markers: Iterable[str] = DEFAULT_ROOT_MARKERS
# ) -> Path:
#     """
#     Walk upward from 'start' (or the main script dir / CWD) until any marker exists.
#     If nothing is found, try `git rev-parse --show-toplevel`.
#     If that fails, fall back to the starting directory.
#     """
#     # choose a stable starting point: main script dir > CWD
#     if start is None:
#         main_module = sys.modules.get("__main__")
#         if main_module and hasattr(main_module, "__file__"):
#             start = Path(main_module.__file__).resolve().parent
#         else:
#             start = Path.cwd().resolve()
#
#     p = start
#     while True:
#         for m in markers:
#             if (p / m).exists():
#                 return p
#         if p.parent == p:  # filesystem root
#             break
#         p = p.parent
#
#     # last resort: git root (works even if markers missing but repo present)
#     try:
#         git_root = subprocess.check_output(
#             ["git", "rev-parse", "--show-toplevel"],
#             text=True
#         ).strip()
#         return Path(git_root)
#     except Exception:
#         return start
#
#
# def get_git_info():
#     """Retrieve Git project name and current branch."""
#     try:
#         repo_root = detect_project_root()
#         repo_name = repo_root.name
#         branch_name = subprocess.check_output(
#             ["git", "rev-parse", "--abbrev-ref", "HEAD"],
#             text=True
#         ).strip()
#         return repo_name, branch_name
#     except Exception:
#         return "Unknown_Project", "Unknown_Branch"
#
#
# def get_vm_name():
#     """Retrieve the name of the VM (hostname)."""
#     try:
#         return socket.gethostname()
#     except Exception:
#         return "Unknown_VM"
#
#
# def _in_airflow() -> bool:
#     return (
#         "AIRFLOW_CTX_DAG_ID" in os.environ
#         or "AIRFLOW_HOME" in os.environ
#         or any(k.startswith("AIRFLOW__") for k in os.environ)
#         or os.environ.get("CG_LOG_SIMPLE") == "1"
#     )
#
#
# def add_traceback(log_method):
#     """Decorator to add traceback info to error logs by default."""
#     @wraps(log_method)
#     def wrapper(*args, **kwargs):
#         kwargs.setdefault("exc_info", True)
#         return log_method(*args, **kwargs)
#     return wrapper
#
#
# def setup_logger(
#     logger_name: str = __name__,
#     log_subdir: str = "log",
#     level: int = logging.DEBUG
# ) -> logging.Logger:
#     """
#     Console logs:
#       - Under Airflow (or CG_LOG_SIMPLE=1): NO timestamp (Airflow adds its own)
#       - Otherwise: include timestamp
#
#     File logs:
#       - Always include timestamp -> <project_root>/log/log.log
#     """
#     root_path = detect_project_root()
#     path_dir = root_path / log_subdir
#     path_dir.mkdir(parents=True, exist_ok=True)
#     log_file_path = path_dir / "log.log"
#
#     logger = logging.getLogger(logger_name)
#     logger.setLevel(level)
#
#     # Avoid duplicate handlers if called multiple times
#     if not logger.handlers:
#         simple_console = _in_airflow()
#
#         if simple_console:
#             console_fmt = "%(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
#             console_formatter = logging.Formatter(console_fmt)
#         else:
#             console_fmt = "%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
#             console_formatter = logging.Formatter(console_fmt, datefmt="%Y-%m-%dT%H:%M:%S%z")
#
#         stream_handler = logging.StreamHandler(sys.stdout)
#         stream_handler.setLevel(level)
#         stream_handler.setFormatter(console_formatter)
#         logger.addHandler(stream_handler)
#
#         file_fmt = "%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
#         file_formatter = logging.Formatter(file_fmt, datefmt="%Y-%m-%dT%H:%M:%S%z")
#         file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
#         file_handler.setLevel(level)
#         file_handler.setFormatter(file_formatter)
#         logger.addHandler(file_handler)
#
#         logger.propagate = False
#
#         # Ensure traceback included by default for errors/exceptions
#         logger.error = add_traceback(logger.error)
#         logger.exception = add_traceback(logger.exception)
#
#     return logger
#
#
# def log_exception(exc_type, exc_value, exc_traceback):
#     logger = logging.getLogger(__name__)
#     if issubclass(exc_type, KeyboardInterrupt):
#         sys.__excepthook__(exc_type, exc_value, exc_traceback)
#         return
#     logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
#
#
# # Default configuration
# logger = setup_logger()
# sys.excepthook = log_exception