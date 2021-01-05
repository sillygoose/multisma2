"""Code to interface with the SMA inverters and return the results."""

import asyncio
import datetime
import logging
import json
from pprint import pprint

import sma

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


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

    async def start(self):
        """Setup inverter for data collection."""
        # SMA class object for access to inverters
        self._sma = sma.SMA(session=self._session, url=self._url, password=self._password, group=self._group)
        await self._sma.new_session()
        if self._sma.sma_sid is None:
            logger.info("%s - no session ID", self._name)
            return None

        # Grab the metadata dictionary
        metadata_url = self._url + "/data/ObjectMetadata_Istl.json"
        async with self._session.get(metadata_url) as resp:
            assert resp.status == 200
            self._metadata = json.loads(await resp.text())

        # Grab the inverter tag dictionary
        tag_url = self._url + "/data/l10n/en-US.json"
        async with self._session.get(tag_url) as resp:
            assert resp.status == 200
            self._tags = json.loads(await resp.text())

        # Read the initial set of history state data
        await self.read_history()
        await self.read_instantaneous()

        # Return a list of cached keys
        return self._instantaneous.keys()

    async def stop(self):
        """Log out of the interter."""
        if self._sma:
            await self._sma.close_session()
            self._sma = None

    async def read_instantaneous(self):
        """Update the instantaneous inverter states."""
        async with self._lock:
            self._instantaneous = await self._sma.read_instantaneous()

    AGGREGATE_KEYS = [
        "6380_40251E00",  # DC Power (current power)
    ]

    def clean(self, raw_results):
        """Clean the raw inverter data and return a dict with the key and result."""
        cleaned = {}
        for key, value in raw_results.items():
            aggregate = Inverter.AGGREGATE_KEYS.count(key)
            sma_type = self.get_type(key)
            scale = self.get_scale(key)
            unit = self.get_unit(key)
            precision = self.get_precision(key)
            states = value.pop("1", None)
            if sma_type == 0:
                sensors = {}
                total = 0
                subkeys = ["a", "b", "c"]
                val = 0
                for index, state in enumerate(states):
                    val = state.get("val", None)
                    if val is None:
                        val = 0
                    if scale != 1:
                        val *= scale
                    total += val
                    sensors[subkeys[index]] = val

                if len(states) > 1:
                    if aggregate:
                        sensors["total"] = total
                    val = sensors
                cleaned[key] = {"val": val, "unit": unit, "precision": precision}
            elif sma_type == 1:
                tag = 0
                for index, state in enumerate(states):
                    tag_list = state.get("val")
                    tag = self.lookup_tag(tag_list[0].get("tag"))
                cleaned[key] = {"val": tag}
            else:
                logger.warning("unexpected sma type: %d", sma_type)

        cleaned["name"] = self._name
        return cleaned

    async def read_keys(self, keys):
        """Read a specified set of inverter keys."""
        results = []
        for key in keys:
            results.append(self.read_key(key))
        return results

    async def read_key(self, key):
        """Read a specified inverter key."""
        raw_result = await self._sma.read_values([key])
        return self.clean({key: raw_result.get(key)})

    async def read_history_period(self, period):
        """Collect the production history for the specified period."""
        assert period in ("day", "month")
        PERIOD_LENGTH = {"day": 30, "month": 366}
        start = datetime.datetime.combine(datetime.date.today(), datetime.time(22, 0)) - datetime.timedelta(
            days=PERIOD_LENGTH[period]
        )
        end = datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0)) + datetime.timedelta(days=1)
        history = await self._sma.read_history(int(start.timestamp()), int(end.timestamp()))

        TOTAL_PRODUCTION = "6400_0046C300"
        latest_production = await self.get_state(TOTAL_PRODUCTION)
        results = latest_production.pop(TOTAL_PRODUCTION)
        history.append({"t": int(end.timestamp()), "v": results.get("val")})
        history.insert(0, {"name": self._name})
        return history

    async def read_history(self):
        """Read the baseline inverter production for select periods."""
        one_hour = 60 * 60 * 1
        three_hours = 60 * 60 * 3
        today_start = int(datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0)).timestamp())
        month_start = int(
            datetime.datetime.combine(datetime.date.today().replace(day=1), datetime.time(0, 0)).timestamp()
        )
        year_start = int(
            datetime.datetime.combine(datetime.date.today().replace(month=1, day=1), datetime.time(0, 0)).timestamp()
        )
        today = await self._sma.read_history(today_start - one_hour, today_start + three_hours)
        month = await self._sma.read_history(month_start - one_hour, today_start)
        year = await self._sma.read_history(year_start - one_hour, today_start)
        self._history["today"] = today[0]
        self._history["month"] = month[0]
        self._history["year"] = year[0]
        self._history["lifetime"] = {"t": 0, "v": 0}

    def display_metadata(self, key):
        """Display the inverter metadata for a key."""
        metadata = self._metadata.get(key, None)
        pprint(f"{self._name}/{key}/{metadata}")

    async def get_state(self, key):
        """Return the state for a given key."""
        async with self._lock:
            state = self._instantaneous.get(key, None).copy()
        cleaned = self.clean({key: state})
        return cleaned

    def name(self):
        """Return the inverter name."""
        return self._name

    def get_unit(self, key):
        """Return the unit used for a given key."""
        metadata = self._metadata.get(key, "???")
        if metadata:
            unit_tag = metadata.get("Unit", None)
            if unit_tag:
                return self.lookup_tag(unit_tag)
        return None

    def get_precision(self, key):
        """Return the precision for a given key."""
        precision = None
        metadata = self._metadata.get(key, None)
        if metadata:
            precision = metadata.get("DataFrmt", None)
            if precision > 3:
                precision = None
        return precision

    def get_scale(self, key):
        """Return the scale value for a given key."""
        metadata = self._metadata.get(key, None)
        if metadata:
            return metadata.get("Scale", None)
        return None

    def get_type(self, key):
        """Return the type of a given key."""
        metadata = self._metadata.get(key, "???")
        return metadata.get("Typ", None)

    def lookup_tag(self, key):
        """Return tag dictionary value for the specified key."""
        return self._tags.get(str(key), "???")

    async def start_production(self, period):
        """Return production value for the start of the specified period."""
        history = self._history.get(period)
        return {self.name(): history["v"]}
        
    async def read_inverter_history(self, start, stop):
        """Read the baseline inverter production."""
        history = await self._sma.read_history(start, stop)
        history.insert(0, {'inverter': self._name})
        return history