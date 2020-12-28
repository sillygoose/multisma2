"""Code to interface with the SMA inverters and return the results."""

import datetime
import logging
import sys

import asyncio
import aiohttp
from asyncio.unix_events import _compute_returncode
from delayedkybrdint import DelayedKeyboardInterrupt

from pvsite import Site
import mqtt
import version
import logfiles

import signal
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Dict, Optional, Any

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


class NormalCompletion(Exception):
    pass


class Multisma2:
    def __init__(self, session):
        self._session = session
        self._loop = None
        self._site = None
        self._wait_event = None
        self._wait_task = None

    def run(self):
        self._loop = asyncio.new_event_loop()

        try:
            # Shield _start() from termination.
            try:
                with DelayedKeyboardInterrupt():
                    self._start()
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt during startup")
                raise

            # Application is running, wait for completion.
            self._wait()
            raise NormalCompletion

        except (KeyboardInterrupt, NormalCompletion):
            # The _stop() is also shielded from termination.
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                logger.info("Received KeyboardInterrupt during shutdown")

    async def _astart(self):
        #async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        #    self._site = Site(session)
        print("_astart()")

    async def _astop(self):
        print("_astop()")

    async def _waiter(self, event):
        #print("_waiter()")
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
        print("_await()")
        self._wait_event = asyncio.Event()
        #self._wait_task = asyncio.create_task(self._wait_event.wait())
        self._wait_task = asyncio.create_task(self._waiter(self._wait_event))
        #self._wait_task = asyncio.create_task(self._waiter())
        await self._wait_task

    def _start(self):
        print("_start()")
        self._loop.run_until_complete(self._astart())

    def _wait(self):
        print("_wait()")
        self._loop.run_until_complete(self._await())

    def _stop(self):
        print("_stop()")
        self._loop.run_until_complete(self._astop())

        # Because we want clean exit, we patiently wait for completion
        # of the _wait_task (otherwise this task might get cancelled
        # in the _cancel_all_tasks() method - which wouldn't be a problem,
        # but it would be dirty).
        #
        # The _wait_event & _wait_task might not exist if the application
        # has been terminated before calling _wait(), therefore we have to
        # carefully check for their presence.

        if self._wait_event:
            print("self._wait_event")
            self._wait_event.set()

        if self._wait_task:
            print(f"self._wait_task is {self._wait_task.done()}")
            #print("self._wait_task")
            self._loop.run_until_complete(self._wait_task)
            print(f"self._wait_task is now {self._wait_task.done()}")

        # Before the loop is finalized, we setup an exception handler that
        # suppresses several nasty exceptions.
        #
        # ConnectionResetError
        # --------------------
        # This exception is sometimes raised on Windows, possibly because of a bug in Python.
        #
        # ref: https://bugs.python.org/issue39010
        #
        # When this exception is raised, the context looks like this:
        # context = {
        #     'message': 'Error on reading from the event loop self pipe',
        #     'exception': ConnectionResetError(
        #         22, 'The I/O operation has been aborted because of either a thread exit or an application request',
        #         None, 995, None
        #       ),
        #     'loop': <ProactorEventLoop running=True closed=False debug=False>
        # }
        #
        # OSError
        # -------
        # This exception is sometimes raised on Windows - usually when application is
        # interrupted early after start.
        #
        # When this exception is raised, the context looks like this:
        # context = {
        #     'message': 'Cancelling an overlapped future failed',
        #     'exception': OSError(9, 'The handle is invalid', None, 6, None),
        #     'future': <_OverlappedFuture pending overlapped=<pending, 0x1d8937601f0>
        #                 cb=[BaseProactorEventLoop._loop_self_reading()]>,
        # }

        def __loop_exception_handler(loop, context: Dict[str, Any]):
            if type(context['exception']) == ConnectionResetError:
                print(f'!!! AsyncApplication._stop.__loop_exception_handler: suppressing ConnectionResetError')
            elif type(context['exception']) == OSError:
                print(f'!!! AsyncApplication._stop.__loop_exception_handler: suppressing OSError')
            else:
                print(f'!!! AsyncApplication._stop.__loop_exception_handler: unhandled exception: {context}')

        self._loop.set_exception_handler(__loop_exception_handler)

        try:
            # Cancel all remaining uncompleted tasks.
            # We should strive to not make any, but mistakes happen and laziness
            # is also a thing.
            #
            # Generally speaking, cancelling tasks shouldn't do any harm (unless
            # they do...).
            self._cancel_all_tasks()

            # Shutdown all active asynchronous generators.
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())

        finally:
            # ... and close the loop.
            #print(f'AsyncApplication._stop: closing event loop')
            self._loop.close()

    def _cancel_all_tasks(self):
        """
        Cancel all tasks in the loop.
        This method injects an asyncio.CancelledError exception
        into all tasks and lets them handle it.
        Note that after cancellation, the event loop is executed again and
        waits for all tasks to complete the cancellation.  This means that
        if some task contains code similar to this:
        >>> except asyncio.CancelledError:
        >>>     await asyncio.Event().wait()
        ... then the loop doesn't ever finish.
        """

        print("_cancel_all_tasks()")
        # Code kindly borrowed from asyncio.run().
        to_cancel = asyncio.tasks.all_tasks(self._loop)
        print(f'AsyncApplication._cancel_all_tasks: cancelling {len(to_cancel)} tasks ...')

        if not to_cancel:
            return

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
    """Set up and start multisma."""
    session = None # async aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
    multisma2 = Multisma2(session)
    multisma2.run()
    return

    #async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        #site = Site(session)
        #try:
            # Create the application log and welcome message
            #logfiles.create_application_log(logger)
            #logger.info(f"multisma2 inverter collection utility {version.get_version()}")
            #logger.info(f"{('Waiting for daylight', 'Starting solar data collection now')[site.daylight()]}")

            # Test out the MQTT broker connection, initialized if checks out
            #mqtt.test_connection()

            # Initialize the inverters
            #await site.initialize()

            #end_time = datetime.datetime.combine(datetime.date.today(), datetime.time(23, 50))

            #while True:
                #await asyncio.sleep(5)

                #current_time = datetime.datetime.now()
                #if current_time > end_time:
                    #break

        #finally:
            #logger.info("Closing multisma2 application, see you on the other side of midnight")
            #await site.close()
            #logfiles.close()


if __name__ == "__main__":
    # make sure we can run multisma2
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 7:
        main()
    else:
        print("python 3.7 or better required")
