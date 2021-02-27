"""Module handling the application and production log files/"""

import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler


logger = logging.getLogger('multisma2')


def check_config(config):
    """Check that the needed YAML options exist."""
    required_keys = ['file', 'level', 'format']
    for key in required_keys:
        if key not in config.keys():
            logger.error(f"Missing required 'log' option in YAML file: '{key}'")
            return False


#
# Public
#

def start(app_logger, config_log):
    """Create the application log."""
    if check_config(config_log) is False:
        return False

    filename = os.path.expanduser(config_log.file + ".log")
    APPLICATION_LOG_LEVEL = config_log.level

    # Create the directory if needed
    filename_parts = os.path.split(filename)
    if filename_parts[0] and not os.path.isdir(filename_parts[0]):
        os.mkdir(filename_parts[0])
    logname = os.path.abspath(filename)

    handler = TimedRotatingFileHandler(logname, when='midnight', interval=1, backupCount=10)
    handler.suffix = "%Y-%m-%d"
    handler.setLevel(APPLICATION_LOG_LEVEL)
    formatter = logging.Formatter(config_log.format)
    handler.setFormatter(formatter)
    app_logger.addHandler(handler)

    # Add some console output for anyone watching
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level=APPLICATION_LOG_LEVEL)
    console_handler.setFormatter(logging.Formatter(config_log.format))
    app_logger.setLevel(level=APPLICATION_LOG_LEVEL)
    app_logger.addHandler(console_handler)

    # First entry
    app_logger.info("Created application log at %s", filename)
    return True
