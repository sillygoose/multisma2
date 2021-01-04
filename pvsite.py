"""Code to interface with the SMA inverters and return the results."""

import asyncio
from asyncio.streams import start_server
import datetime
import time
import logging
import os

from pprint import pprint
from dateutil import tz

from inverter import Inverter
import influx

from configuration import INVERTERS
from configuration import APPLICATION_LOG_LOGGER_NAME


logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)

request = {"destDev":[],"key":28704,"tStart":1596250800,"tEnd":1598943600}
RESULT = {
    "result": {
        "019D-B32CACFD": [
            {
                "t": 1596258000,
                "v": 1228745
            },
            {
                "t": 1596344400,
                "v": 1272144
            },
            {
                "t": 1596430800,
                "v": 1307251
            },
            {
                "t": 1596517200,
                "v": 1341159
            },
            {
                "t": 1596603600,
                "v": 1348391
            },
            {
                "t": 1596690000,
                "v": 1388665
            },
            {
                "t": 1596776400,
                "v": 1426463
            },
            {
                "t": 1596862800,
                "v": 1450830
            },
            {
                "t": 1596949200,
                "v": 1485184
            },
            {
                "t": 1597035600,
                "v": 1525833
            },
            {
                "t": 1597122000,
                "v": 1561069
            },
            {
                "t": 1597208400,
                "v": 1595344
            },
            {
                "t": 1597294800,
                "v": 1636528
            },
            {
                "t": 1597381200,
                "v": 1677497
            },
            {
                "t": 1597467600,
                "v": 1716759
            },
            {
                "t": 1597554000,
                "v": 1748018
            },
            {
                "t": 1597640400,
                "v": 1781189
            },
            {
                "t": 1597726800,
                "v": 1812265
            },
            {
                "t": 1597813200,
                "v": 1844584
            },
            {
                "t": 1597899600,
                "v": 1869876
            },
            {
                "t": 1597986000,
                "v": 1906040
            },
            {
                "t": 1598072400,
                "v": 1939529
            },
            {
                "t": 1598158800,
                "v": 1965801
            },
            {
                "t": 1598245200,
                "v": 1999765
            },
            {
                "t": 1598331600,
                "v": 2033739
            },
            {
                "t": 1598418000,
                "v": 2059763
            },
            {
                "t": 1598504400,
                "v": 2091712
            },
            {
                "t": 1598590800,
                "v": 2100454
            },
            {
                "t": 1598677200,
                "v": 2124620
            },
            {
                "t": 1598763600,
                "v": 2145093
            },
            {
                "t": 1598850000,
                "v": 2177071
            },
            {
                "t": 1598936400,
                "v": 2213071
            }
        ]
    }
}



class Site:
    """Class to describe a PV site with one or more inverters."""
    def __init__(self, session, influx):
        """Create a new Site object."""
        self._influx = influx
        self._inverters = []
        for inverter in INVERTERS:
            self._inverters.append(Inverter(inverter["name"], inverter["ip"], inverter["user"], inverter["password"], session))

    async def initialize(self):
        """Initialize the Site object."""
        await asyncio.gather(*(inverter.initialize() for inverter in self._inverters))

    async def close(self):
        """Shutdown the Site object."""
        await asyncio.gather(*(inverter.close() for inverter in self._inverters))

    async def run(self):
        while True:
            #start = 1596250800
            #stop = 1598943600
            month = 7
            year = 2020
            start = int((datetime.datetime.combine(datetime.date.today().replace(year=year, month=month, day=1), datetime.time(23, 0)) - datetime.timedelta(days=1)).timestamp())
            stop = int((datetime.datetime.combine(datetime.date.today().replace(year=year, month=month+1, day=1), datetime.time(3, 0))).timestamp())
            histories = await asyncio.gather(*(inverter.read_history(start, stop) for inverter in self._inverters))
            #pprint(histories)
            self._influx.write_history(histories)
            break
            #await asyncio.sleep(1)



