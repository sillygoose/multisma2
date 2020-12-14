"""Code to interface with the SMA inverters and return the results."""

# todo
#   - clean up futures
#

import asyncio
import datetime
import logging
import json
from pprint import pprint

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
            + ", instantaneous = "
            + str(type(self._instantaneous))
        )

    async def initialize(self):
        """Setup inverter for data collection."""
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
        """Log out of the interter."""
        if self._sma:
            await self._sma.close_session()
            self._sma = None

    async def read_instantaneous(self):
        """Update the instantaneous inverter states."""
        async with self._lock:
            self._instantaneous = await self._sma.read_instantaneous()

    AGGREGATE_KEYS = [
        '6380_40251E00',        # DC Power (current power)
    ]

    def clean(self, raw_results):
        """Clean the raw inverter data and return a dict with the key and result."""
        cleaned = {}
        for key, value in raw_results.items():
            aggregate = Inverter.AGGREGATE_KEYS.count(key)
            type = self.get_type(key)
            scale = self.get_scale(key)
            unit = self.get_unit(key)
            precision = self.get_precision(key)
            states = value.pop('1', None)
            results = {}
            if type == 0:
                sensors = {}
                total = 0
                subkeys = ['a', 'b', 'c']
                val = 0
                for index, state in enumerate(states):
                    val = state.get('val', None)
                    if val is None:
                        val = 0
                    if scale != 1:
                        val *= scale
                    total += val
                    sensors[subkeys[index]] = val

                if len(states) > 1:
                    if aggregate:
                        sensors['total'] = total
                    val = sensors
                cleaned[key] = { 'val': val, 'unit': unit, 'precision': precision }
            elif type == 1:
                for index, state in enumerate(states):
                    tag_list = state.get('val')
                    tag = self.lookup_tag(tag_list[0].get('tag'))
                cleaned[key] = { 'val': tag }
            else:
                logger.warning(f"unexpected sma type: {type}")

        cleaned['name'] = self._name
        return cleaned

    async def read_keys(self, keys):
        """Read a specified set of inverter keys."""
        results = []
        for index, key in enumerate(keys):
            results.append(self.read_key(key))
        return results

    async def read_key(self, key):
        """Read a specified inverter key."""
        raw_result = await self._sma.read_values([key])
        return self.clean({ key: raw_result.get(key) })

    async def read_history_period(self, period):
        """Collect the production history for the specified period."""
        assert period == 'day' or period == 'month'
        PERIOD_LENGTH = {'day': 32, 'month': 366}
        start = datetime.datetime.combine(datetime.date.today(), datetime.time(22, 0)) - datetime.timedelta(days=PERIOD_LENGTH[period])
        end = datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0)) + datetime.timedelta(days=1)
        history = await self._sma.read_history(int(start.timestamp()), int(end.timestamp()))

        TOTAL_PRODUCTION = '6400_0046C300'
        latest_production = await self.get_state(TOTAL_PRODUCTION)
        history.append({'t': int(end.timestamp()), 'v': latest_production.get(TOTAL_PRODUCTION)})
        history.insert(0, {'name': self._name})
        return history

    async def read_history(self):
        """Read the baseline inverter production for select periods."""
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
        self._history['lifetime'] = {'t': 0, 'v': 0}

    def display_metadata(self, key=None):
        """Display the inverter metadata."""
        if key is None:
            for key, value in self._instantaneous.items():
                meta = self._metadata.get(key)
                type = meta.get('Typ')
                prio = meta.get('Prio')
                format = meta.get('DataFrmt')
                scale = meta.get('Scale')
                name = self.lookup_tag(meta.get('TagId', '###'))
                if type == 1:
                    for k1, v1 in value.items():
                        for e1 in v1:
                            t1 = e1.get('val')
                            for e2 in t1:
                                tag = e2.get('tag')
                                e1['val'] = self.lookup_tag(tag)

                print(f'{type} {prio} {format} {scale}  {key}   {name}')
                print(f'                    {value}')
        else:
            metadata = self._metadata.get(key, None)
            pprint(f"{self._name}/{key}/{metadata}")

    async def display_history(self):
        """Display the baseline production for select periods."""
        print(f"{self._name}    today baseline {datetime.datetime.fromtimestamp(self._history['today'].get('t'))}   {self._history['today'].get('v')}")
        print(f"{self._name}    month baseline {datetime.datetime.fromtimestamp(self._history['month'].get('t'))}   {self._history['month'].get('v')}")
        print(f"{self._name}     year baseline {datetime.datetime.fromtimestamp(self._history['year'].get('t'))}   {self._history['year'].get('v')}")
        print(f"{self._name} lifetime baseline {datetime.datetime.fromtimestamp(self._history['lifetime'].get('t'))}   {self._history['lifetime'].get('v')}")

    async def get_state(self, key):
        """Return the state for a given key."""
        async with self._lock:
            state = self._instantaneous.get(key, None).copy()
        cleaned = self.clean({ key: state })
        return cleaned

    def name(self):
        """Return the inverter name."""
        return self._name

    def get_unit(self, key):
        """Return the unit used for a given key."""
        metadata = self._metadata.get(key, '???')
        if metadata:
            unit_tag = metadata.get('Unit', None)
            if unit_tag:
                return self.lookup_tag(unit_tag)
        return None

    def get_precision(self, key):
        """Return the precision for a given key."""
        precision = None
        metadata = self._metadata.get(key, None)
        if metadata:
            precision = metadata.get('DataFrmt', None)
            if precision > 3:
                precision = None
        return precision

    def get_scale(self, key):
        """Return the scale value for a given key."""
        metadata = self._metadata.get(key, None)
        if metadata:
            return metadata.get('Scale', None)
        return None

    def get_type(self, key):
        """Return the type of a given key."""
        metadata = self._metadata.get(key, '???')
        return metadata.get('Typ', None)

    def lookup_tag(self, key):
        """Return tag dictionary value for the specified key."""
        return self._tags.get(str(key), '???')
