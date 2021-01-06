"""Code to interface with the SMA inverters and return the results."""
# Robust initialization and shutdown code courtesy of 
# https://github.com/wbenny/python-graceful-shutdown.git

import logging
import sys
import os
import signal
from typing import Dict, Any, NoReturn

import asyncio
import aiohttp

from delayedints import DelayedKeyboardInterrupt
from pvsite import PVSite
import version
import logfiles

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


class Multisma2:
    class NormalCompletion(Exception):
        pass
    class FailedInitialization(Exception):
        pass
    class TerminateSignal(Exception):
        pass

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._session = None
        self._site = None
        signal.signal(signal.SIGTERM, self.catch)
        signal.siginterrupt(signal.SIGTERM, False)

    def catch(self, signum, frame):
        logger.info("Received SIGTERM signal, forcing shutdown")
        raise Multisma2.TerminateSignal

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

        except (KeyboardInterrupt, Multisma2.NormalCompletion, Multisma2.FailedInitialization, Multisma2.TerminateSignal):
            # The _stop() is also shielded from termination.
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt during shutdown")

    async def _astart(self):
        # Create the application log and welcome messages
        logfiles.start(logger)
        logger.info(f"multisma2 inverter collection utility {version.get_version()}, PID is {os.getpid()}")

        # Create the client session and initialize the inverters
        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
        self._site = PVSite(self._session)
        result = await self._site.start()
        if not result: raise Multisma2.FailedInitialization

    async def _await(self):
        #while True:
        #    print("waiting....")
        #    await asyncio.sleep(3)
        await self._site.run()

    async def _astop(self):
        logger.info("Closing multisma2 application")
        await self._site.stop()
        await self._session.close()

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
