"""Code to interface with the SMA inverters and return the results."""

import asyncio
import aiohttp
import datetime
import logging
import sys

from pvsite import Site
import mqtt
import version
import logfiles

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


if __name__ == "__main__":

    async def main():
        """Set up and start multisma."""
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            site = Site(session)
            try:
                # Create the application log and welcome message
                logfiles.create_application_log(logger)
                logger.info(f"multisma2 inverter collection utility {version.get_version()}")
                logger.info(f"{('Waiting for daylight', 'Starting solar data collection now')[site.daylight()]}")

                # Test out the MQTT broker connection, initialized if checks out
                mqtt.test_connection()

                # Initialize the inverters
                await site.initialize()

                end_time = datetime.datetime.combine(datetime.date.today(), datetime.time(23, 50))

                while True:
                    await asyncio.sleep(5)

                    current_time = datetime.datetime.now()
                    if current_time > end_time:
                        break

            finally:
                logger.info("Closing multisma2 application, see you on the other side of midnight")
                await site.close()
                logfiles.close()

    # make sure we can run multisma2
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 7:
        asyncio.run(main())
    print("python 3.7 or better required")
