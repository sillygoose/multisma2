"""Module handling the application and production log files/"""

import os
import sys
import logging
from datetime import datetime

from configuration import (
    APPLICATION_LOG_LOGGER_NAME,
    APPLICATION_LOG_FILE,
    APPLICATION_LOG_FORMAT,
)

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


#
# Public
#

def start(app_logger):
    """Create the application log."""
    filename = os.path.expanduser(APPLICATION_LOG_FILE + ".log")

    try:
        from configuration import APPLICATION_LOG_LEVEL
    except ImportError:
        APPLICATION_LOG_LEVEL = 'INFO'

    # Create the directory if needed
    filename_parts = os.path.split(filename)
    if filename_parts[0] and not os.path.isdir(filename_parts[0]):
        os.mkdir(filename_parts[0])
    filename = os.path.abspath(filename)
    logging.basicConfig(
        filename=filename,
        filemode="w+",
        format=APPLICATION_LOG_FORMAT,
        level=APPLICATION_LOG_LEVEL,
    )

    # Add some console output for anyone watching
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level=APPLICATION_LOG_LEVEL)
    console_handler.setFormatter(logging.Formatter(APPLICATION_LOG_FORMAT))
    app_logger.addHandler(console_handler)
    app_logger.setLevel(level=APPLICATION_LOG_LEVEL)

    # First entry
    app_logger.info("Created application log %s", filename)
