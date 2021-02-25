"""Code to interface with the SMA inverters and return the results."""
# Robust initialization and shutdown code courtesy of
# https://github.com/wbenny/python-graceful-shutdown.git

import logging
import sys
import os
import time
import signal
from config import config_from_yaml

import asyncio
import aiohttp

from delayedints import DelayedKeyboardInterrupt
from pvsite import PVSite
import version
import logfiles
from exceptions import TerminateSignal, NormalCompletion, AbnormalCompletion, FailedInitialization


logger = logging.getLogger('multisma2')


class Multisma2():

    def __init__(self):
        """Initialize the Multisma2 instance."""
        self._loop = asyncio.new_event_loop()
        self._session = None
        self._site = None
        signal.signal(signal.SIGTERM, self.catch)
        signal.siginterrupt(signal.SIGTERM, False)
        yaml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'multisma2.yaml')
        self._config = config_from_yaml(data=yaml_file, read_from_file=True)

    def catch(self, signum, frame):
        """Handler for SIGTERM signals."""
        logger.critical("Received SIGTERM signal, forcing shutdown")
        raise TerminateSignal

    def run(self):
        """Code to handle the start(), run(), and stop() interfaces."""
        try:
            self._config.multisma2
        except Exception:
            logger.critical("Unable to continue, 'multisma2' entry missing in YAML file'")
            return

        ERROR_DELAY = 10
        delay = 0
        try:
            try:
                with DelayedKeyboardInterrupt():
                    self._start()
            except KeyboardInterrupt:
                logger.critical("Received KeyboardInterrupt during startup")
                raise

            self._run()
            raise NormalCompletion

        except (KeyboardInterrupt, NormalCompletion, TerminateSignal):
            pass
        except AbnormalCompletion:
            logger.critical("Received AbnormalCompletion exception detected")
            delay = ERROR_DELAY
        except FailedInitialization:
            logger.critical("Received FailedInitialization exception detected")
            delay = ERROR_DELAY
        except Exception as e:
            #trace_back = sys.exc_info()[2]
            #line = trace_back.tb_lineno
            #print(e.__cause__)
            logger.error(f"Unexpected exception caught: {e}")
            delay = 0
        finally:
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                logger.critical("Received KeyboardInterrupt during shutdown")
            finally:
                if delay > 0:
                    logger.info(f"multisma2 is delaying restart for ")
                    time.sleep(delay)

    async def _astart(self):
        """Asynchronous initialization code."""
        config = self._config.multisma2
        if 'log' not in config.keys():
            logger.critical("Unable to continue, 'log' entry missing in 'multisma2' YAML file section'")
            raise FailedInitialization

        logfiles.start(logger, config.log)
        logger.info(f"multisma2 inverter collection utility {version.get_version()}, PID is {os.getpid()}")

        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
        self._site = PVSite(self._session, config)
        result = await self._site.start()
        if not result:
            raise FailedInitialization

    async def _arun(self):
        """Asynchronous run code."""
        await self._site.run()

    async def _astop(self):
        """Asynchronous closing code."""
        logger.info("Closing multisma2 application")
        await self._site.stop()
        await self._session.close()

    def _start(self):
        """Initialize everything prior to running."""
        self._loop.run_until_complete(self._astart())

    def _run(self):
        """Run the application."""
        self._loop.run_until_complete(self._arun())

    def _stop(self):
        """Cleanup after running."""
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
