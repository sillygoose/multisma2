"""Module handling the application and production log files/"""

import os
import sys
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler


_LOGGER = logging.getLogger('multisma2')


def check_config(log_options):
    """Check that the needed YAML options exist."""
    required_keys = ['file', 'level', 'format']
    for key in required_keys:
        if key not in log_options.keys():
            _LOGGER.error(f"Missing required 'log' option in YAML file: '{key}'")
            return None
    return dict(log_options)


def start(config):
    """Create the application log."""

    try:
        log_options = check_config(config.multisma2.log)
    except Exception as e:
        raise Exception(f"Error processing the 'log' options in the configuration file: {e}")

    log_options = dict(dict(config.multisma2.log))
    log_level = log_options.get('level', None)
    log_format = log_options.get('format', None)
    log_file = log_options.get('file', None)

    now = datetime.now()
    filename = os.path.expanduser(log_file + "_" + now.strftime("%Y-%m-%d") + ".log")

    # Create the directory if needed
    filename_parts = os.path.split(filename)
    if filename_parts[0] and not os.path.isdir(filename_parts[0]):
        os.mkdir(filename_parts[0])
    filename = os.path.abspath(filename)

    # Change log files at midnight
    handler = TimedRotatingFileHandler(filename, when='midnight', interval=1, backupCount=10)
    handler.suffix = "%Y-%m-%d"
    handler.setLevel(log_level)
    formatter = logging.Formatter(log_format)
    # formatter = logging.Formatter(config_log.format)
    handler.setFormatter(formatter)
    _LOGGER.addHandler(handler)

    # Add some console output for anyone watching
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    _LOGGER.addHandler(console_handler)
    _LOGGER.setLevel(logging.INFO)

    # First entry
    _LOGGER.info("Created application log %s", filename)
