"""Module handling the application and production log files/"""

import os
import sys
import logging
from datetime import datetime
from pprint import pprint

from configuration import (
    INVERTERS,
    ENABLE_PRODUCTION_LOGGING,
    PRODUCTION_LOG_FILE_PREFIX,
    APPLICATION_LOG_LOGGER_NAME,
    INVERTER_NAME_KEY,
    INVERTER_LOG_NAME_KEY,
    APPLICATION_LOG_FILE,
    APPLICATION_LOG_FORMAT,
)

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)

LOGGING_VAR = {}


#
# Public
#

def close():
    """Closes open log files."""
    # Call from main thread to make sure all logging threads are completed
    if "datalogging" in LOGGING_VAR:
        #handle = LOGGING_VAR["datalogging"]
        handle = LOGGING_VAR.pop("datalogging")
        logger.info("Closing production data log %s", LOGGING_VAR["filename"])
        handle.close()


def create_application_log(app_logger):
    """Create the application log."""
    # Create the application log
    now = datetime.now()
    filename = os.path.expanduser(
        APPLICATION_LOG_FILE + "_" + now.strftime("%Y-%m-%d") + ".log"
    )

    # Create the directory if needed
    filename_parts = os.path.split(filename)
    if filename_parts[0] and not os.path.isdir(filename_parts[0]):
        os.mkdir(filename_parts[0])
    filename = os.path.abspath(filename)
    logging.basicConfig(
        filename=filename,
        filemode="w+",
        format=APPLICATION_LOG_FORMAT,
        level=logging.INFO,
    )

    # Add some console output for anyone watching
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(APPLICATION_LOG_FORMAT))
    app_logger.addHandler(console_handler)
    app_logger.setLevel(logging.INFO)

    # First entry
    app_logger.info("Created application log %s", filename)


def create_production_log():
    """Create the production log file and fill in the header record."""
    # Build the production log file name and force line buffering
    now = datetime.now()
    filename = os.path.expanduser(
        PRODUCTION_LOG_FILE_PREFIX + "_" + now.strftime("%Y-%m-%d") + ".csv"
    )

    # Create the directory if needed
    filename_parts = os.path.split(filename)
    if filename_parts[0] and not os.path.isdir(filename_parts[0]):
        os.mkdir(filename_parts[0])
    LOGGING_VAR["filename"] = os.path.abspath(filename)
    LOGGING_VAR["datalogging"] = open(LOGGING_VAR["filename"], mode="a+", buffering=1)

    # Skip the header info if the file already exists
    if LOGGING_VAR["datalogging"].tell() > 0:
        # Existing file
        logger.info("Appending to existing production log %s", LOGGING_VAR["filename"])
    else:
        # Solar production log day of year and date fields once in the header
        LOGGING_VAR["datalogging"].write("DOY, " + now.strftime("%j") + "\n")
        LOGGING_VAR["datalogging"].write("Date, " + now.strftime("%Y-%m-%d") + "\n")

        # 1st row of AC Power (and total), DC Power, and inverter status
        LOGGING_VAR["datalogging"].write("%5s" % ("Local"))
        LOGGING_VAR["datalogging"].write(", %5s" % ("Solar"))
        for inv in INVERTERS:
            LOGGING_VAR["datalogging"].write(", %10s" % (inv[INVERTER_LOG_NAME_KEY]))
        LOGGING_VAR["datalogging"].write(", %10s" % ("Site"))
        for inv in INVERTERS:
            LOGGING_VAR["datalogging"].write(", %10s" % (inv[INVERTER_LOG_NAME_KEY]))
        for inv in INVERTERS:
            LOGGING_VAR["datalogging"].write(", %10s" % (inv[INVERTER_LOG_NAME_KEY]))
        LOGGING_VAR["datalogging"].write("\n")

        # 2nd row of AC Power (and total), DC Power, and inverter status
        LOGGING_VAR["datalogging"].write("%5s" % ("Time"))
        LOGGING_VAR["datalogging"].write(", %5s" % ("Time"))
        for inv in INVERTERS:
            LOGGING_VAR["datalogging"].write(", %10s" % ("AC Power"))
        LOGGING_VAR["datalogging"].write(", %10s" % ("AC Power"))
        for inv in INVERTERS:
            LOGGING_VAR["datalogging"].write(", %10s" % ("DC Power"))
        for inv in INVERTERS:
            LOGGING_VAR["datalogging"].write(", %10s" % ("Status"))
        LOGGING_VAR["datalogging"].write("\n")

        # New production log file
        logger.info("Creating production log %s", LOGGING_VAR["filename"])


def append(ac_production, local_time, solar_time):
    """Log each sensor, substitute 0 if not present."""
    # Exit if not actively logging
    #if not ENABLE_PRODUCTION_LOGGING or not daylight.is_daylight():
    if not ENABLE_PRODUCTION_LOGGING:
        return

    # Extract the data sets for output
    ac_power = {}
    dc_power = {}
    inv_state = {}
    for index, sensor in enumerate(ac_production):
        if sensor['topic'] == 'ac_measurements/power':
            ac_power = sensor
        if sensor['topic'] == 'dc_measurements/power':
            dc_power = sensor
        if sensor['topic'] == 'reason_for_derating':
            inv_state = sensor

    # Check if the first time through and the logs must be created
    if "datalogging" not in LOGGING_VAR:
        create_production_log()

    # Log the local and solar times
    LOGGING_VAR["datalogging"].write(local_time.strftime("%H:%M"))
    LOGGING_VAR["datalogging"].write(", " + solar_time.strftime("%H:%M"))

    # AC power, DC power, and inverter status
    for inverter in INVERTERS:
        LOGGING_VAR["datalogging"].write(", %10.0f" % (ac_power.get(inverter[INVERTER_NAME_KEY], '###')))
    LOGGING_VAR["datalogging"].write(", %10.0f" % (ac_power.get('total', '$$$')))
    for inverter in INVERTERS:
        strings = dc_power.get(inverter[INVERTER_NAME_KEY], None)
        LOGGING_VAR["datalogging"].write(", %10.0f" % (strings.get('total', '$$$')))
    for inverter in INVERTERS:
        LOGGING_VAR["datalogging"].write(", %10s" % (inv_state.get(inverter[INVERTER_NAME_KEY], '###')))
    LOGGING_VAR["datalogging"].write("\n")