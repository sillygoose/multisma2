"""Code to interface with the SMA inverters and return the results."""
# Robust initialization and shutdown code courtesy of
# https://github.com/wbenny/python-graceful-shutdown.git

import logging
import sys
import os
import time
import signal
from readconfig import read_config

import asyncio
import aiohttp

from delayedints import DelayedKeyboardInterrupt
from pvsite import PVSite
import version
import logfiles
from exceptions import TerminateSignal, NormalCompletion, AbnormalCompletion, FailedInitialization


_LOGGER = logging.getLogger('multisma2')
_LOGGER = logging.getLogger('multisma2')


def buildYAMLExceptionString(exception, file='multisma2'):
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

    def __init__(self, config):
        """Initialize the Multisma2 instance."""
        self._config = config
        self._loop = asyncio.new_event_loop()
        self._session = None
        self._site = None
        signal.signal(signal.SIGTERM, self.catch)
        signal.siginterrupt(signal.SIGTERM, False)

    def catch(self, signum, frame):
        """Handler for SIGTERM signals."""
        _LOGGER.critical("Received SIGTERM signal, forcing shutdown")
        raise TerminateSignal

    def run(self):
        """Code to handle the start(), run(), and stop() interfaces."""
        # try:
        #    self._config.multisma2
        # except Exception:
        #    print("Unable to continue, 'multisma2' entry missing in YAML file")
        #    return

        # ERROR_DELAY might be non-zero when SMA errors are detected *for now not implemented)
        ERROR_DELAY = 0
        delay = 0
        try:
            try:
                with DelayedKeyboardInterrupt():
                    self._start()
            except KeyboardInterrupt:
                _LOGGER.critical("Received KeyboardInterrupt during startup")
                raise

            self._run()
            raise NormalCompletion

        except (KeyboardInterrupt, NormalCompletion, TerminateSignal):
            pass
        except AbnormalCompletion:
            # _LOGGER.critical("Received AbnormalCompletion exception detected")
            delay = ERROR_DELAY
        except FailedInitialization:
            # _LOGGER.critical("Received FailedInitialization exception detected")
            delay = ERROR_DELAY
        except Exception as e:
            _LOGGER.error(f"Unexpected exception caught: {e}")
            delay = 0
        finally:
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                _LOGGER.critical("Received KeyboardInterrupt during shutdown")
            finally:
                if delay > 0:
                    print(
                        f"multisma2 is delaying restart for {delay} seconds (Docker will restart multisma2, otherwise exits)")
                    time.sleep(delay)

    async def _astart(self):
        """Asynchronous initialization code."""
        config = self._config.multisma2
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
        _LOGGER.info("Closing multisma2 application")
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
        config = read_config()
        if config is None:
            return
    except Exception as e:
        print(f"{e}")
        return

    logfiles.start(config)
    _LOGGER.info(f"multisma2 inverter collection utility {version.get_version()}, PID is {os.getpid()}")

    try:
        multisma2 = Multisma2(config)
        multisma2.run()
    except FailedInitialization:
        pass
    except Exception as e:
        print(f"Unexpected exception: {e}")


if __name__ == "__main__":
    # make sure we can run multisma2
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 9:
        main()
    else:
        print("python 3.9 or better required")
