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


def buildYAMLExceptionString(exception, file='sbhistory'):
    e = exception
    try:
        type = ''
        file = file
        line = 0
        column = 0
        info = ''

        if e.args[0]:
            type = e.args[0]
            type += ' '

        if e.args[1]:
            file = os.path.basename(e.args[1].name)
            line = e.args[1].line
            column = e.args[1].column

        if e.args[2]:
            info = os.path.basename(e.args[2])

        if e.args[3]:
            file = os.path.basename(e.args[3].name)
            line = e.args[3].line
            column = e.args[3].column

        errmsg = f"YAML file error {type}in {file}:{line}, column {column}: {info}"

    except Exception:
        errmsg = f"YAML file error and no idea how it is encoded."

    return errmsg


class Multisma2():

    def __init__(self):
        """Initialize the Multisma2 instance."""
        self._loop = asyncio.new_event_loop()
        self._session = None
        self._site = None
        signal.signal(signal.SIGTERM, self.catch)
        signal.siginterrupt(signal.SIGTERM, False)
        try:
            yaml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'multisma2.yaml')
            self._config = config_from_yaml(data=yaml_file, read_from_file=True)
        except Exception as e:
            error_message = buildYAMLExceptionString(exception=e, file='multisma2.yaml')
            print(error_message)
            raise FailedInitialization

    def catch(self, signum, frame):
        """Handler for SIGTERM signals."""
        logger.critical("Received SIGTERM signal, forcing shutdown")
        raise TerminateSignal

    def run(self):
        """Code to handle the start(), run(), and stop() interfaces."""
        try:
            self._config.multisma2
        except Exception:
            print("Unable to continue, 'multisma2' entry missing in YAML file")
            return

        # ERROR_DELAY might be non-zero when SMA errors are detected *for now not implemented)
        ERROR_DELAY = 0
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
                    print(f"multisma2 is delaying restart for {delay} seconds (Docker will restart multisma2, otherwise exits)")
                    time.sleep(delay)

    async def _astart(self):
        """Asynchronous initialization code."""
        config = self._config.multisma2
        if 'log' not in config.keys():
            print("Unable to continue, 'log' entry missing in 'multisma2' YAML file section'")
            raise FailedInitialization

        if not logfiles.start(logger, config.log):
            print("Unable to continue, 'log' entry missing in 'multisma2' YAML file section'")
            raise FailedInitialization
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
        if self._site:
            await self._site.stop()
        if self._session:
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
    try:
        multisma2 = Multisma2()
        multisma2.run()
    except FailedInitialization:
        pass
    except Exception as e:
        print(f"Unexpected exception: {e}")


if __name__ == "__main__":
    # make sure we can run multisma2
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 7:
        main()
    else:
        print("python 3.7 or better required")
