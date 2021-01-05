"""Code to interface with the SMA inverters and return the results."""

import asyncio
import datetime
#import time
import logging
#import os

from pprint import pprint
from dateutil import tz

from inverter import Inverter
#import influx

from configuration import INVERTERS
from configuration import APPLICATION_LOG_LOGGER_NAME


logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


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
        results = await asyncio.gather(*(inverter.initialize() for inverter in self._inverters))
        return False not in results

    async def close(self):
        """Shutdown the Site object."""
        await asyncio.gather(*(inverter.close() for inverter in self._inverters))

    async def run(self):
        while True:
            month = 7
            year = 2020
            start = int((datetime.datetime.combine(datetime.date.today().replace(year=year, month=month, day=1), datetime.time(23, 0)) - datetime.timedelta(days=1)).timestamp())
            #stop = int((datetime.datetime.combine(datetime.date.today().replace(year=year, month=month+1, day=1), datetime.time(3, 0))).timestamp())
            stop = int((datetime.datetime.combine(datetime.date.today(), datetime.time(3, 0))).timestamp())
            histories = await asyncio.gather(*(inverter.read_history(start, stop) for inverter in self._inverters))
            #pprint(histories)
            total = {}
            for inverter in histories:
                for i in range(1, len(inverter)):
                    t = inverter[i]['t']
                    v = inverter[i]['v']
                    if v is None:
                        if i > 1:
                            inverter[i]['v'] = inverter[i-1]['v']
                        else:
                            inverter[i]['v'] = inverter[i+1]['v']
                        v = inverter[i]['v']
                    if t in total:
                        total[t] += v
                    else:
                        total[t] = v

            site_total = []
            for t, v in total.items():
                site_total.append({'t': t, 'v': v})
            site_total.insert(0, {'inverter': 'site'})
            #pprint(site_total)
            histories.append(site_total)
            #pprint(histories)
            self._influx.write_history(histories)
            break
            #await asyncio.sleep(1)



