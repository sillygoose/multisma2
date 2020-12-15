"""Code to interface with the SMA inverters and return the results."""

import asyncio
import datetime
from dateutil import tz
from pprint import pprint
import time
import logging
import os

import astral
from astral import sun

from inverter import Inverter
import mqtt
import logfiles

from configuration import SITE_LATITUDE, SITE_LONGITUDE, SITE_NAME, SITE_REGION, TIMEZONE
from configuration import INVERTERS
from configuration import CO2_AVOIDANCE

from configuration import APPLICATION_LOG_LOGGER_NAME
logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


MQTT_TOPICS = {
        '6100_0046C200': 'production/current',
        '6400_0046C300': 'production/total',
        '6100_40263F00': 'ac_measurements/power',
        '6100_00465700': 'ac_measurements/frequency',
        '6180_08465A00': 'ac_measurements/excitation_type',
        '6100_00464800': 'ac_measurements/voltage/phase_l1',
        '6100_00464900': 'ac_measurements/voltage/phase_l2',
        '6100_00464B00': 'ac_measurements/voltage/phase_l1_l2',
        '6380_40251E00': 'dc_measurements/power',
        '6380_40451F00': 'dc_measurements/voltage',
        '6380_40452100': 'dc_measurements/current',
        '6180_08416500': 'status/reason_for_derating',
    }

# These are keys that we calculate a total across all inverters
AGGREGATE_KEYS = [
        '6100_40263F00',        # AC grid power (current)
        '6100_0046C200',        # PV generation power (current)
        '6400_0046C300',        # Meter count and PV gen. meter (total power)
        '6380_40251E00',        # DC power (1 per string)
    ]

AC_MEASUREMENTS = [
        '6100_40263F00',        # AC grid power (current)
        '6100_00465700',        # Grid frequency
        '6100_00464B00'         # Phase L1 against L2 voltage
    ]

SITE_SNAPSHOT = [
        '6100_40263F00',        # AC grid power (current)
        '6180_08416500',        # Reason for derating
        '6380_40251E00',        # DC power (current)
    ]

DC_MEASUREMENTS = [
        '6380_40251E00',        # DC Power (current)
        '6380_40451F00',        # DC Voltage
        '6380_40452100'         # DC Current
    ]

STATES = [
        '6100_40263F00',        # AC grid power (current)
        '6180_08416500',        # Reason for derating
        '6380_40251E00',        # DC power (current)
        '6800_08855C00',        # SMA Shadefix Activated (not cached)
    ]


class Site():
    """Class to describe a PV site with one or more inverters."""

    SOLAR_EVENTS = []

    def __init__(self, session):
        """Create a new Site object."""
        self._tasks = None
        self._inverters = []
        for inv, inverter in enumerate(INVERTERS, 1):
            object = Inverter(inverter['name'], inverter['ip'], inverter['user'], inverter['password'], session)
            self._inverters.append(object)

        self._siteinfo = astral.LocationInfo(SITE_NAME, SITE_REGION, TIMEZONE, SITE_LATITUDE, SITE_LONGITUDE)
        self._tzinfo = tz.gettz(TIMEZONE)

        local_noon = datetime.datetime.combine(datetime.date.today(), datetime.time(12, 0), tzinfo=self._tzinfo)
        solar_noon = astral.sun.noon(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)
        self._solar_time_diff = solar_noon - local_noon

        astral_now = astral.sun.now(tzinfo=self._tzinfo)
        self._dawn = dawn = astral.sun.dawn(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)
        sunrise = astral.sun.sunrise(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)
        sunset = astral.sun.sunset(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)
        self._dusk = dusk = astral.sun.dusk(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)

        Site.SOLAR_EVENTS.append(dict(dawn=dict(time=dawn, seen=astral_now > dawn, msg="Daylight begins, time to wake up and start monitoring")))
        Site.SOLAR_EVENTS.append(dict(sunrise=dict(time=sunrise, seen=astral_now > sunrise, msg="Sunrise is happening, time to collect some photons")))
        Site.SOLAR_EVENTS.append(dict(sunset=dict(time=sunset, seen=astral_now > sunset, msg="Sunset is here, things are slowing down")))
        Site.SOLAR_EVENTS.append(dict(dusk=dict(time=dusk, seen=astral_now > dusk, msg="End of daylight, start of lazy night monitoring")))

    async def initialize(self):
        """Initialize the Site object to begin collection."""
        cached_keys = await asyncio.gather(*(inverter.initialize() for inverter in self._inverters))
        self._cached_keys = cached_keys[0]

        queue5 = asyncio.Queue()
        queue30 = asyncio.Queue()
        queue60 = asyncio.Queue()
        self._tasks = [
            asyncio.create_task(self.task_5s(queue5)),
            asyncio.create_task(self.task_30s(queue30)),
            asyncio.create_task(self.task_60s(queue60)),
            asyncio.create_task(self.task_scheduler(queue5, queue30, queue60)),
        ]

    async def close(self):
        """Shutdown the Site."""
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await asyncio.gather(*(inverter.close() for inverter in self._inverters))

    async def read_instantaneous(self):
        """Update the instantaneous cache from the inverter."""
        await asyncio.gather(*(inverter.read_instantaneous() for inverter in self._inverters))

    async def read_history_period(self, period):
        """Collect the production history for a specified period."""
        history_list = await asyncio.gather(*(inverter.read_history_period(period) for inverter in self._inverters))

        aggregate = {}
        while history_list:
            inverter = history_list.pop()
            last_seen = 0
            inverter_name = inverter.pop(0)
            for index, item in enumerate(inverter, 1):
                if index == len(inverter):
                    break
                start_of_period = inverter[index-1].get('v')
                end_of_period = inverter[index].get('v')
                if not start_of_period:
                    start_of_period = last_seen
                if not end_of_period:
                    end_of_period = last_seen
                total = (end_of_period - start_of_period) / 1000
                date_time = item.get('t')
                aggregate[date_time] = aggregate.get(date_time, 0) + total
                last_seen = end_of_period

        if period in ['month']:
            reduced = {}
            for t, v in aggregate.items():
                dt = datetime.datetime.fromtimestamp(t)
                month = int(datetime.datetime.combine(datetime.date(dt.year, dt.month, 1), datetime.time(0, 0)).timestamp())
                reduced[month] = reduced.get(month, 0) + v
            aggregate = reduced

        aggregate['topic'] = 'history/' + period
        aggregate['precision'] = 3
        aggregate['unit'] = 'kWh'
        return [aggregate]

    async def production_stats(self):
        """Get the daily, monthly, yearly, and lifetime production values."""
        tprod_list = await self.total_production()
        tprod = tprod_list[0]
        stats = []
        for period in ['today', 'month', 'year', 'lifetime']:
            period_stats = {}
            total = 0
            for inverter in self._inverters:
                name = inverter.name()
                diff = tprod[name] - inverter._history[period].get('v')
                period_stats[name] = diff / 1000
                total += diff

            period_stats['total'] = total / 1000
            period_stats['unit'] = 'kWh'
            period_stats['topic'] = 'production/' + period
            stats.append(period_stats)

        return stats

    async def co2_avoided(self):
        """Calculate the CO2 avoided by solar production."""
        CO2_AVOIDANCE_KG = CO2_AVOIDANCE
        CO2_AVOIDANCE_TON = CO2_AVOIDANCE_KG / 1000
        CO2_SETTINGS = [
            { 'period': 'today',    'unit': 'kg',   'precision': 2, 'factor': CO2_AVOIDANCE_KG },
            { 'period': 'month',    'unit': 'kg',   'precision': 0, 'factor': CO2_AVOIDANCE_KG },
            { 'period': 'year',     'unit': 'tons', 'precision': 2, 'factor': CO2_AVOIDANCE_TON },
            { 'period': 'lifetime', 'unit': 'tons', 'precision': 2, 'factor': CO2_AVOIDANCE_TON },
        ]

        tp = await self.production_stats()
        co2avoided = []
        for index, total in enumerate(tp):
            total_topic = total['topic']
            for settings in CO2_SETTINGS:
                co2avoided_period = {}
                period = settings.get('period')
                if period in total_topic:
                    unit = settings.get('unit')
                    factor = settings.get('factor')
                    precision = settings.get('precision')
                    if precision:
                        co2avoided_period['total'] = round(total['total'] * factor, precision)
                    else:
                        co2avoided_period['total'] = int(total['total'] * factor)
                    co2avoided_period['topic'] = 'co2avoided/' + period
                    co2avoided_period['unit'] = unit
                    co2avoided_period['factor'] = factor
                    co2avoided.append(co2avoided_period)
                    break

        return co2avoided

    async def current_dc_values(self):
        """Get the current DC power of each inverter."""
        return await self.get_composite(DC_MEASUREMENTS)

    async def snapshot(self):
        """Get the values of interest from each inverter."""
        return await self.get_composite(SITE_SNAPSHOT)

    async def current_status(self):
        """Get the current status of each inverter."""
        return await self.get_composite(['6180_08416500'])

    async def current_production(self):
        """Get the current production of the inverters."""
        return await self.get_composite(['6100_40263F00'])

    async def total_production(self):
        """Get the total production of each inverter and the total of all inverters."""
        return await self.get_composite(['6400_0046C300'])

    async def read_keys(self, keys):
        """Read a list of keys from the cache or the inverter(s)."""
        return await self.get_composite(keys)

    async def get_composite(self, keys):
        """Get the key values of each inverter and optionally create a site total."""
        sensors = []
        for key in keys:
            if self.cached_key(key):
                results = await asyncio.gather(*(inverter.get_state(key) for inverter in self._inverters))
            else:
                results = await asyncio.gather(*(inverter.read_key(key) for inverter in self._inverters))

            composite = {}
            total = 0
            calculate_total = AGGREGATE_KEYS.count(key)
            for index, inverter in enumerate(results):
                result = inverter.get(key)
                val = result.get('val', None)
                unit = result.get('unit', None)
                precision = result.get('precision', None)
                if isinstance(val, dict):
                    if calculate_total:
                        subtotal = val.get('total')
                        total += subtotal
                else:
                    if calculate_total:
                        total += val

                if unit:
                    composite['unit'] = unit
                if precision is not None:
                    composite['precision'] = precision
                composite[inverter.get('name')] = val

            if calculate_total:
                composite['total'] = total

            composite['topic'] = MQTT_TOPICS.get(key, key)
            sensors.append(composite)

        return sensors

    # testing values
    SAMPLE_PERIOD = [
        { 'scale': 1 },     # night
        { 'scale': 1 },     # day
    ]

    async def task_scheduler(self, queue5, queue30, queue60):
        """Task to schedule actions at regular intervals."""
        SLEEP = 0.5
        last_tick = int(time.time())
        info = dict(time=last_tick, daylight=self.daylight(), dawn=self._dawn, delta=self._solar_time_diff, dusk=self._dusk)
        scaling = Site.SAMPLE_PERIOD[info['daylight']].get('scale')

        while True:
            tick = int(time.time())
            scaled_tick = tick / scaling
            if scaled_tick != last_tick:
                last_tick = scaled_tick
                info['time'] = tick

                if scaled_tick % 5 == 0:
                    queue5.put_nowait(info)
                if scaled_tick % 30 == 0:
                    queue30.put_nowait(info)
                if scaled_tick % 60 == 0:
                    queue60.put_nowait(info)

                if tick % 300 == 0:
                    daylight = self.daylight()
                    info['daylight'] = daylight
                    scaling = Site.SAMPLE_PERIOD[daylight].get('scale')
                    self.sun_events(daylight)

            await asyncio.sleep(SLEEP)

    async def task_5s(self, queue):
        """Work done every 5 seconds."""
        longest = 0.0
        while True:
            try:
                info = await queue.get()
                await self.read_instantaneous()
            finally:
                queue.task_done()

            mqtt.publish(await self.co2_avoided())
            #mqtt.publish(await self.snapshot())
            #mqtt.publish(await self.read_keys(STATES))
            #mqtt.publish(await self.current_production())
            #mqtt.publish(await self.current_dc_values())
            #mqtt.publish(await self.current_status())

    async def task_30s(self, queue):
        """Work done every 30 seconds."""
        while True:
            try:
                info = await queue.get()
            finally:
                queue.task_done()

            mqtt.publish(await self.production_stats())
            #mqtt.publish(await self.total_production())

    async def task_60s(self, queue):
        """Work done every 60 seconds."""
        while True:
            try:
                info = await queue.get()
            finally:
                queue.task_done()

            mqtt.publish(await self.co2_avoided())
            mqtt.publish(await self.read_history_period('day'))
            mqtt.publish(await self.read_history_period('month'))

            # Log production and status to the production log
            if info.get('daylight'):
                local_time = datetime.datetime.fromtimestamp(info.get('time'))
                solar_time = local_time + info.get('delta')
                logdata = await self.snapshot()
                logfiles.append(logdata, local_time, solar_time)

    DAYLIGHT_VAR = {
        'force_daylight_tested': False,
    }

    def daylight(self):
        """Check for daylight conditions, possibly overriding from the environment."""
        if not Site.DAYLIGHT_VAR['force_daylight_tested']:
            Site.DAYLIGHT_VAR['force_daylight_tested'] = True
            if 'FORCE_DAYLIGHT' in os.environ:
                option = os.environ.get('FORCE_DAYLIGHT')
                if 'daylight-info-shown' not in Site.DAYLIGHT_VAR:
                    Site.DAYLIGHT_VAR['daylight-info-shown'] = True
                    if option in ('True', "1"):
                        logger.info("Forcing daylight: %s", option)
                        Site.DAYLIGHT_VAR['force_daylight'] = True
                        return Site.DAYLIGHT_VAR['force_daylight']
                    elif option in ('False', "0"):
                        logger.info("Forcing daylight: %s", option)
                        Site.DAYLIGHT_VAR['force_daylight'] = False
                        return Site.DAYLIGHT_VAR['force_daylight']
                    else:
                        logger.info("Bad daylight forcing option: %s", option)
        elif 'force_daylight' in Site.DAYLIGHT_VAR:
            return Site.DAYLIGHT_VAR['force_daylight']

        astral_now = astral.sun.now(self._tzinfo)
        daylight = (astral_now > self._dawn) and (astral_now < self._dusk)
        return daylight

    def sun_events(self, daylight):
        """Determines if a solar event has occured."""
        if not daylight:
            return
        astral_now = astral.sun.now(tzinfo=self._tzinfo)
        for index, event_dict in enumerate(Site.SOLAR_EVENTS):
            for type, event in event_dict.items():
                if event['seen']:
                    continue
                if astral_now > event['time']:
                    event['seen'] = True
                    logger.info(event['msg'])

    def cached_key(self, key):
        """Determines if a key in the inverter cache."""
        cached = key in self._cached_keys
        return cached
