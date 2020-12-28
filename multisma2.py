"""Code to interface with the SMA inverters and return the results."""
# Robust initialization and shutdown code courtesy of 
# https://github.com/wbenny/python-graceful-shutdown.git

import datetime
import logging
import sys
from typing import Dict, Any

import asyncio
import aiohttp
from delayedints import DelayedKeyboardInterrupt

from pvsite import Site
import mqtt
import version
import logfiles

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


class Multisma2:
    class NormalCompletion(Exception):
        pass

    def __init__(self):
        self._session = None
        self._loop = asyncio.new_event_loop() # None
        self._site = None
        self._wait_event = None
        self._wait_task = None

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

        except (KeyboardInterrupt, Multisma2.NormalCompletion):
            # The _stop() is also shielded from termination.
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt during shutdown")

    async def _astart(self):
        # Create the client session and initialize the inverters
        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
        self._site = Site(self._session)
        await self._site.initialize()

        # Test out the MQTT broker connection, initialized if checks out
        mqtt.test_connection()

        # Create the application log and welcome messages
        logfiles.create_application_log(logger)
        logger.info(f"multisma2 inverter collection utility {version.get_version()}")
        logger.info(f"{('Waiting for daylight', 'Starting solar data collection now')[self._site.daylight()]}")

    async def _astop(self):
        logger.info("Closing multisma2 application, back on the other side of midnight")
        logfiles.close()
        await self._site.close()
        await self._session.close()

    async def _wait_for_end(self, event):
        end_time = datetime.datetime.combine(datetime.date.today(), datetime.time(23, 50))
        while True:
            if event.is_set():
                await event.wait()
                break   
            current_time = datetime.datetime.now()
            if current_time > end_time:
                break
            await asyncio.sleep(1)

    async def _await(self):
        self._wait_event = asyncio.Event()
        self._wait_task = asyncio.create_task(self._wait_for_end(self._wait_event))
        await self._wait_task

    def _start(self):
        self._loop.run_until_complete(self._astart())

    def _wait(self):
        self._loop.run_until_complete(self._await())

    def _stop(self):
        self._loop.run_until_complete(self._astop())

        # Because we want a clean exit, wait for completion
        # of the _wait_task (otherwise this task might get cancelled
        # in the _cancel_all_tasks() method - which wouldn't be a problem,
        # but it would be dirty).
        if self._wait_event:
            self._wait_event.set()

        if self._wait_task:
            if not self._wait_task.done():
                self._loop.run_until_complete(self._wait_task)

        def __loop_exception_handler(loop, context: Dict[str, Any]):
            if type(context['exception']) == ConnectionResetError:
                logger.warn("suppressing ConnectionResetError")
            elif type(context['exception']) == OSError:
                logger.warn("suppressing OSError")
            else:
                logger.warn(f"unhandled exception: {context}")

        self._loop.set_exception_handler(__loop_exception_handler)
        try:
            # Shutdown tasks and any active asynchronous generators.
            self._cancel_all_tasks()
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())

        finally:
            # ... and close the loop.
            self._loop.close()

    def _cancel_all_tasks(self):
        """
        Cancel all tasks in the loop.
        This method injects an asyncio.CancelledError exception
        into all tasks and lets them handle it.
        """
        # Code kindly borrowed from asyncio.run().
        to_cancel = asyncio.tasks.all_tasks(self._loop)
        if not to_cancel:
            return

        logger.error("At least one task is still running, error?")
        for task in to_cancel:
            task.cancel()

        self._loop.run_until_complete(
            asyncio.tasks.gather(*to_cancel, loop=self._loop, return_exceptions=True)
        )

        for task in to_cancel:
            if task.cancelled():
                continue

            if task.exception() is not None:
                self._loop.call_exception_handler({
                    'message': 'unhandled exception during Application.run() shutdown',
                    'exception': task.exception(),
                    'task': task,
                })


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
