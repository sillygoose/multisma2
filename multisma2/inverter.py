"""Code to interface with the SMA inverters and return the results."""

import asyncio
import datetime
import logging
import json
from pprint import pprint
from typing import Dict

import sma

from exceptions import SmaException


_LOGGER = logging.getLogger('multisma2')

# Inverter keys that contain aggregates
AGGREGATE_KEYS = [
    '6380_40251E00',  # DC Power (current power)
]


class Inverter:
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

    async def start(self) -> Dict:
        """Setup inverter for data collection."""

        try:
            self._sma = sma.SMA(session=self._session, url=self._url, password=self._password, group=self._group)
        except SmaException as e:
            _LOGGER.debug(f"Inverter error with '{self._url}': '{e.name}'")
            return {'keys': None, 'name': self._url, 'error': e.name}

        try:
            await self._sma.new_session()
            _LOGGER.debug(
                f"Connected to SMA inverter '{self._name}' at {self._url} with session ID '{self._sma.sma_sid}'")
        except SmaException as e:
            _LOGGER.debug(f"{self._name}, login failed: {e}")
            return {'keys': None, 'name': self._url, 'error': e.name}

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
        success = await self.read_inverter_production()
        if not success:
            return {'keys': None, 'name': self._url, 'error': 'read_inverter_production() failed'}
        success = await self.read_instantaneous(True)
        if not success:
            return {'keys': None, 'name': self._url, 'error': 'read_instantaneous() failed'}

        # Return a list of cached keys
        return {'keys': self._instantaneous.keys(), 'name': self._url, 'error': None}

    async def stop(self):
        """Log out of the interter."""
        if self._sma:
            await self._sma.close_session()
            self._sma = None

    def clean(self, raw_results):
        """Clean the raw inverter data and return a dict with the key and result."""
        cleaned = {}
        for key, value in raw_results.items():
            if not value:
                continue
            aggregate = AGGREGATE_KEYS.count(key)
            sma_type = self.get_type(key)
            scale = self.get_scale(key)
            states = value.pop('1', None)
            if sma_type == 0:
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
                        sensors[self.name()] = total
                    val = sensors
                cleaned[key] = {'val': val}
            elif sma_type == 1:
                for state in states:
                    tag_list = state.get('val')
                    cleaned[key] = {'val': tag_list[0].get('tag')}
                    break
            else:
                _LOGGER.warning(f"unexpected sma type: {sma_type}")

        cleaned['name'] = self._name
        return cleaned

    async def read_instantaneous(self, daylight):
        """Update the instantaneous inverter states."""
        if daylight is False:
            return {'name': self._name, 'sensors': self._instantaneous, 'error': 'None'}
        try:
            async with self._lock:
                self._instantaneous = await self._sma.read_instantaneous()
            return {'name': self._name, 'sensors': self._instantaneous, 'error': 'None'}
        except SmaException as e:
            _LOGGER.debug(f"{self._name} read_instantaneous() error: {e.name}")
            return {'name': self._name, 'sensors': None, 'error': e.name}

    async def read_keys(self, keys):
        """Read a specified set of inverter keys."""
        results = []
        for key in keys:
            results.append(self.read_key(key))
        return results

    async def read_key(self, key):
        """Read a specified inverter key."""
        try:
            raw_result = await self._sma.read_values([key])
        except SmaException as e:
            _LOGGER.debug(f"{self._name}: read_key({key}): {e.name}")
            return False
        if raw_result:
            return self.clean({key: raw_result.get(key)})
        return False

    async def read_history(self, start, stop):
        try:
            history = await self._sma.read_history(start, stop)
        except SmaException as e:
            _LOGGER.debug(f"{self._name}: read_history({start}, {stop}): {e.name}")
            return None
        if not history:
            _LOGGER.debug(f"{self._name}: read_history({start}, {stop}) returned 'None' (check your local time)")
            return None
        history.insert(0, {'inverter': self._name})
        return history

    async def read_inverter_production(self):
        """Read the baseline inverter production for select periods."""
        before_delta = datetime.timedelta(hours=1)
        after_delta = datetime.timedelta(hours=1)
        stop = datetime.datetime.now() + after_delta
        today = datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0)) - before_delta
        month = datetime.datetime.combine(datetime.date.today().replace(day=1), datetime.time(0, 0)) - before_delta
        year = datetime.datetime.combine(datetime.date.today().replace(
            month=1, day=1), datetime.time(0, 0)) - before_delta
        results = await asyncio.gather(
            self.read_history(int(today.timestamp()), int(stop.timestamp())),
            self.read_history(int(month.timestamp()), int(stop.timestamp())),
            self.read_history(int(year.timestamp()), int(stop.timestamp())),
        )
        if None in results:
            return False
        self._history['today'] = results[0][1]
        self._history['month'] = results[1][1]
        self._history['year'] = results[2][1]
        self._history['lifetime'] = {'t': 0, 'v': 0}
        # {'today': {'t': 1611032400, 'v': 3121525},
        #  'month': {'t': 1609477200, 'v': 3055878},
        #  'year': {'t': 1609477200, 'v': 3055878},
        #  'lifetime': {'t': 0, 'v': 0}}
        _LOGGER.debug(f"{self._name}/read_inverter_production({today}/{stop}): {self._history}")
        return True

    def display_metadata(self, key):
        """Display the inverter metadata for a key."""
        metadata = self._metadata.get(key, None)
        pprint(f"{self._name}/{key}/{metadata}")

    async def get_state(self, key):
        """Return the state for a given key."""
        assert self._instantaneous is not None
        async with self._lock:
            state = self._instantaneous.get(key, None).copy()
        cleaned = self.clean({key: state})
        return cleaned

    def name(self):
        """Return the inverter name."""
        return self._name

    async def keys_for_unit(self, unit_tag):
        keys = []
        for key, metadata in self._metadata.items():
            unit = metadata.get('Unit', None)
            if unit == unit_tag:
                keys.append(key)
        return keys

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

    async def start_production(self, period):
        """Return production value for the start of the specified period."""
        history = self._history.get(period)
        _LOGGER.debug(f"{self._name}/start_production({period}): {history['v']}")
        return {self.name(): history['v']}

    async def read_inverter_history(self, start, stop):
        """Read the baseline inverter production."""
        try:
            history = await self._sma.read_history(start, stop)
            _LOGGER.debug(f"{self._name}/read_inverter_history({start}, {stop}): {history}")
        except SmaException as e:
            _LOGGER.debug(f"{self._name}: read_inverter_history({start}, {stop}): {e.name}")
            return None
        history.insert(0, {'inverter': self._name})
        return history
