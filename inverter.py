"""Code to interface with the SMA inverters and return the results."""

# todo
#   - clean up futures
#   - fix get_state
#   - read_values
#

import asyncio
#import aiohttp
import datetime
from dateutil import tz
from pprint import pprint
import time
import logging
import os
import json

import logfiles
import sma

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)

class Inverter():
    """Class to encapsulate a single inverter."""
    def __init__(self, name, url, group, password, session):
        """Setup an Inverter class instance."""
        self._name = name
        self._url = url
        self._password = password
        self._group = group
        self._session = session
        self._sma = None
        self._metadata = None
        self._tags = None
        self._instantaneous = None
        self._values = None
        self._history = {}
        self._lock = asyncio.Lock()

    def __repr__(self):
        """Show some information for the inverter class."""
        return (
            "name = "
            + self._name
            + ", url = "
            + self._url
            + ", password = "
            + self._password
            + ", group = "
            + self._group
            + ", session = "
            + str(self._session)
            + ", sma = "
            + str(self._sma)
            + ", tags = "
            + str(type(self._tags))
            + ", values = "
            + str(type(self._values))
            + ", instantaneous = "
            + str(type(self._instantaneous))
        )

    async def initialize(self):
        """Setup inverter object for use."""
        # SMA class object for access to inverters
        self._sma = sma.SMA(session=self._session, url=self._url, password=self._password, group=self._group)
        await self._sma.new_session()
        if self._sma.sma_sid is None:
            logger.info(f"{self._name} - no session ID")
            return False

        # Grab the metadata dictionary
        metadata_url = self._url + '/data/ObjectMetadata_Istl.json'
        async with self._session.get(metadata_url) as resp:
            assert resp.status == 200
            self._metadata = json.loads(await resp.text())

        # Grab the inverter tag dictionary
        tag_url = self._url + '/data/l10n/en-US.json'
        async with self._session.get(tag_url) as resp:
            assert resp.status == 200
            self._tags = json.loads(await resp.text())

        # Read the initial set of history state data
        await self.read_history()
        await self.read_instantaneous()
        return True

    async def close(self):
        """###."""
        if self._sma:
            await self._sma.close_session()
            self._sma = None

    async def read_instantaneous(self):
        """Update the instantaneous inverter states."""
        async with self._lock:
            self._instantaneous = await self._sma.read_instantaneous()

    async def read_values(self, keys):
        """Read a specified set of inverter keys."""
        async with self._lock:
            self._values = await self._sma.read_values(keys)

    async def read_history_period(self, period):
        """###."""
        assert period == 'day' or period == 'month'
        PERIOD_LENGTH = {'day': 32, 'month': 366}
        start = datetime.datetime.combine(datetime.date.today(), datetime.time(22, 0)) - datetime.timedelta(days=PERIOD_LENGTH[period])
        end = datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0)) + datetime.timedelta(days=1)
        history = await self._sma.read_history(int(start.timestamp()), int(end.timestamp()))

        today_production = await self.get_state('6400_0046C300')
        value_list = today_production.pop(self._name)
        history.append({'t': end, 'v': value_list[0].get('val')})
        return history

    async def read_history(self):
        """###."""
        one_hour = 60 * 60 * 1
        three_hours = 60 * 60 * 3
        today_start = int(datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0)).timestamp())
        month_start = int(datetime.datetime.combine(datetime.date.today().replace(day=1), datetime.time(0, 0)).timestamp())
        year_start = int(datetime.datetime.combine(datetime.date.today().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
        today = await self._sma.read_history(today_start - one_hour, today_start + three_hours)
        month = await self._sma.read_history(month_start - one_hour, today_start)
        year = await self._sma.read_history(year_start - one_hour, today_start)
        self._history['today'] = today[0]
        self._history['month'] = month[0]
        self._history['year'] = year[0]
        self._history['lifetime'] = dict(t = 0, v = 0)

    async def display_state(self):
        """###."""
        for index, key in enumerate(STATES):
            print(f"{self._name}/{self.get_state(key)}")

    async def display_metadata(self, all=False):
        """###."""
        if all:
            for key, value in self._instantaneous.items():
                meta = self._metadata.get(key)
                type = meta.get('Typ')
                prio = meta.get('Prio')
                format = meta.get('DataFrmt')
                scale = meta.get('Scale')
                name = self.tag(meta.get('TagId', '###'))
                if type == 1:
                    for k1, v1 in value.items():
                        for e1 in v1:
                            t1 = e1.get('val')
                            for e2 in t1:
                                tag = e2.get('tag')
                                e1['val'] = self.tag(tag)

                print(f'{type} {prio} {format} {scale}  {key}   {name}')
                print(f'                    {value}')
        else:
            for index, key in enumerate(STATES):
                metadata = self._metadata.get(key, None)
                pprint(f"{self._name}/{key}/{metadata}")

    async def display_values(self):
        """###."""
        pprint(f"{self._name}/{self._values}")

    async def display_history(self):
        """###."""
        print(f"{self._name} today baseline {datetime.datetime.fromtimestamp(self._history['today'].get('t'))}   {self._history['today'].get('v')}")
        print(f"{self._name} month baseline {datetime.datetime.fromtimestamp(self._history['month'].get('t'))}   {self._history['month'].get('v')}")
        print(f"{self._name}  year baseline {datetime.datetime.fromtimestamp(self._history['year'].get('t'))}   {self._history['year'].get('v')}")

    async def _get_state_lock(self, key):
        """###."""
        async with self._lock:
            state_dict = self._instantaneous.get(key, None).copy()
        state_dict[self._name] = state_dict.pop('1', None)
        return state_dict

    async def get_state(self, key):
        """###."""
        return await self._get_state_lock(key)

    def get_unit(self, key):
        """###."""
        metadata = self._metadata.get(key, '???')
        if metadata:
            unit_tag = metadata.get('Unit', None)
            if unit_tag:
                return self.tag(unit_tag)
        return None

    def get_scale(self, key):
        """###."""
        metadata = self._metadata.get(key, None)
        if metadata:
            return metadata.get('Scale', None)
        return None

    def get_type(self, key):
        """###."""
        metadata = self._metadata.get(key, '???')
        return metadata.get('Typ', None)

    def name_for_key(self, key):
        """###."""
        return self._tags.get(str(key), '???')

    def tag(self, key):
        """Return tag dictionary value for the specified key."""
        return self._tags.get(str(key), '???')
