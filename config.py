
import logging
import sys
import os
from functools import wraps
from joblib import Memory
from tenacity import retry, stop_after_attempt, wait_fixed

# Configure cache
memory = Memory("cache_dir", verbose=0)
memory.clear(warn=False)

# Configuration for logging
LOG_FILE_PATH = 'log/log.log'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def setup_logger(name=__name__):
    if not os.path.exists('log'):
        os.makedirs('log')
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create handlers
    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(LOG_FILE_PATH)

    # Set logging level for handlers
    stream_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.DEBUG)

    # Define formatters and add them to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    if not logger.handlers:
        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

    logger.error = add_traceback(logger.error)
    logger.exception = add_traceback(logger.exception)

    return logger


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


def log_exception(exc_type, exc_value, exc_traceback):
    logger = setup_logger()
    if issubclass(exc_type, KeyboardInterrupt):
        # Let KeyboardInterrupt exceptions go through
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


# Override default exception handling to log uncaught exceptions
sys.excepthook = log_exception

# Logger instance
logger = setup_logger()


# Retry logic decorator
def retry_decorator(func):
    """
    Decorator to add retry logic with logging for any function.
    """

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_fixed(60),  # Wait 60 seconds between retries
        before=lambda retry_state: logger.info(
            f"Retrying {retry_state.fn.__name__} (Attempt {retry_state.attempt_number})..."
        ),
        # after=lambda retry_state: logger.error(
        #     f"Attempt {retry_state.attempt_number} failed.",
        #     exc_info=retry_state.outcome.exception() if retry_state.outcome.failed else ""
        # )
    )
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
