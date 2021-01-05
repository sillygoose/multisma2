"""Code to interface with the SMA inverters and return the results."""
# Robust initialization and shutdown code courtesy of 
# https://github.com/wbenny/python-graceful-shutdown.git

import datetime
import logging
import sys
from typing import Dict, Any, NoReturn

import asyncio
import aiohttp
from delayedints import DelayedKeyboardInterrupt
from influxdb import InfluxDBClient

from pvsite import Site
from influx import InfluxDB
import version
import logfiles

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


class Multisma2:
    class NormalCompletion(Exception):
        pass
    class FailedInitialization(Exception):
        pass

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._session = None
        self._influx = InfluxDB()
        self._site = None

    def run(self):
        try:
            # Shield _start() from termination.
            try:
                with DelayedKeyboardInterrupt():
                    self._start()
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt during startup")
                raise

            # multisma2 is running, wait for completion.
            self._wait()
            raise Multisma2.NormalCompletion

        except (KeyboardInterrupt, Multisma2.NormalCompletion, Multisma2.FailedInitialization):
            # The _stop() is also shielded from termination.
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt during shutdown")

    async def _astart(self):
        # Create the application log and welcome messages
        logfiles.create_application_log(logger)
        logger.info(f"multisma2 inverter collection utility {version.get_version()}")

        # Create the client session and initialize the inverters
        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
        self._site = Site(self._session, self._influx)
        result = await self._site.initialize()
        if not result:
            raise Multisma2.FailedInitialization
        self._influx.start()

    async def _astop(self):
        self._influx.stop()
        await self._site.close()
        await self._session.close()
        logger.info("Closing multisma2 application")
        logfiles.close()

    async def _await(self):
        await self._site.run()

    def _start(self):
        self._loop.run_until_complete(self._astart())

    def _wait(self):
        self._loop.run_until_complete(self._await())

    def _stop(self):
        self._loop.run_until_complete(self._astop())


def main():
    """Set up and start multisma2."""
    multisma2 = Multisma2()
    multisma2.run()


if __name__ == "__main__":
    # make sure we can run multisma2
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 7:
        main()
    else:
        print("python 3.7 or better required")
