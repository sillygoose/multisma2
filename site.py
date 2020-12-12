"""Code to interface with the SMA inverters and return the results."""

# todo
#   - clean up futures
#   - fix get_state
#   - read_values
#

import asyncio
import aiohttp
import json
import datetime
from dateutil import tz
from pprint import pprint
import time
import logging
import os

import mqtt
import sma
import version
import logfiles

import astral
from astral import sun

from configuration import SITE_LATITUDE, SITE_LONGITUDE, TIMEZONE
from configuration import INVERTERS
from configuration import CO2_AVOIDANCE
from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)

class Timer:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start


MQTT_TOPICS = {
        '6100_0046C200': 'production_current',
        '6400_0046C300': 'production_total',
        '6100_40263F00': 'ac_measurements/power',
        '6100_00465700': 'ac_measurements/frequency',
        '6180_08465A00': 'ac_measurements/excitation_type',
        '6100_00464800': 'ac_measurements/voltage/phase_l1',
        '6100_00464900': 'ac_measurements/voltage/phase_l2',
        '6100_00464B00': 'ac_measurements/voltage/phase_l1_l2',
        '6380_40251E00': 'dc_measurements/power',
        '6380_40451F00': 'dc_measurements/voltage',
        '6380_40452100': 'dc_measurements/current',
        '6180_08416500': 'reason_for_derating',
    }

# These are keys that we calculate a total across all inverters
AGGREGATE_KEYS = [
        '6100_40263F00',        # AC grid power (current power)
        '6100_0046C200',        # PV generation power (current power)
        '6400_0046C300',        # Meter count and PV gen. meter (total power)
        '6380_40251E00',        # DC power (1 per string)
    ]

STATES = [
        '6180_08416500',        # Reason for derating
        '6100_0046C200',        # PV generation power (current power)
        '6400_0046C300',        # Meter count and PV gen. meter (total power)
    ]

AC_MEASUREMENTS = [
        '6100_40263F00',        # AC grid power (current power)
        '6100_00465700',        # Grid frequency
        '6100_00464B00'         # Phase L1 against L2 voltage
    ]

SITE_SNAPSHOT = [
        '6100_40263F00',        # AC grid power (current power)
        '6380_40251E00',        # DC power (current power)
        '6180_08416500',        # Reason for derating
    ]

DC_MEASUREMENTS = [
        '6380_40251E00',        # DC Power (current power)
#        '6380_40451F00',        # DC Voltage
#        '6380_40452100'         # DC Current
    ]


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


class Site():
    """Class to describe a PV site with one or more inverters."""

    SOLAR_EVENTS = []

    def __init__(self, session):
        """###."""
        self._tasks = None
        self._inverters = []
        for inv, inverter in enumerate(INVERTERS, 1):
            object = Inverter(inverter['Name'], inverter['IP_Addr'], inverter['User'], inverter['Password'], session)
            self._inverters.append(object)

        self._siteinfo = astral.LocationInfo("Parker Lane", "New York", TIMEZONE, SITE_LATITUDE, SITE_LONGITUDE)
        self._tzinfo = tz.gettz(TIMEZONE)

        local_noon = datetime.datetime.combine(datetime.date.today(), datetime.time(12, 0), tzinfo=self._tzinfo)
        solar_noon = astral.sun.noon(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)
        self._solar_time_diff = solar_noon - local_noon

        astral_now = astral.sun.now(tzinfo=self._tzinfo)
        self._dawn = dawn = astral.sun.dawn(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)
        sunrise = astral.sun.sunrise(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)
        sunset = astral.sun.sunset(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)
        self._dusk = dusk = astral.sun.dusk(observer=self._siteinfo.observer, date=datetime.datetime.today(), tzinfo=self._tzinfo)

        Site.SOLAR_EVENTS.append(dict(dawn = dict(time = dawn, seen = astral_now > dawn, msg = "Daylight begins, time to wake up and start monitoring")))
        Site.SOLAR_EVENTS.append(dict(sunrise = dict(time = sunrise, seen = astral_now > sunrise, msg = "Sunrise is happening, time to collect some photons")))
        Site.SOLAR_EVENTS.append(dict(sunset = dict(time = sunset, seen = astral_now > sunset, msg = "Sunset is here, things are slowing down")))
        Site.SOLAR_EVENTS.append(dict(dusk = dict(time = dusk, seen = astral_now > dusk, msg = "End of daylight, start of lazy night monitoring")))

    async def initialize(self):
        """###."""
        await asyncio.gather(*(inverter.initialize() for inverter in self._inverters))
        #worker_tasks = [attribute for attribute in dir(Site) if callable(getattr(Site, attribute)) and attribute.startswith('task_') is True]
        queue5 = asyncio.Queue()
        queue30 = asyncio.Queue()
        queue60 = asyncio.Queue()
        self._tasks = [
            asyncio.create_task(self.task_5s(queue5)),
            asyncio.create_task(self.task_30s(queue30)),
            asyncio.create_task(self.task_60s(queue60)),
            asyncio.create_task(self.task_manager(queue5, queue30, queue60)),
        ]

    async def close(self):
        """###."""
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await asyncio.gather(*(inverter.close() for inverter in self._inverters))

    async def read_instantaneous(self):
        """###."""
        await asyncio.gather(*(inverter.read_instantaneous() for inverter in self._inverters))

    async def read_values(self, keys):
        """###."""
        await asyncio.gather(*(inverter.read_values(keys) for inverter in self._inverters))

    async def read_history_period(self, period):
        """###."""
        history_list = await asyncio.gather(*(inverter.read_history_period(period) for inverter in self._inverters))

        aggregate = {}
        while history_list:
            inverter = history_list.pop()
            last_seen = 0
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
                name = inverter._name
                diff = tprod[name] - inverter._history[period].get('v')
                period_stats[name] = diff / 1000
                total += diff

            period_stats['total'] = total / 1000
            period_stats['topic'] = 'production_' + period
            stats.append(period_stats)
        return stats

    async def co2_avoided(self):
        """Calculate the CO2 avoided."""
        CO2_AVOIDANCE_KG = CO2_AVOIDANCE
        CO2_AVOIDANCE_TON = CO2_AVOIDANCE_KG / 1000
        tp = await self.production_stats()
        co2avoided = []
        for index, total in enumerate(tp):
            total_topic = total['topic']
            for period in ['today', 'month', 'year', 'lifetime']:
                co2avoided_period = {}
                if period in total_topic:
                    unit = 'kg'
                    factor = CO2_AVOIDANCE_KG
                    if period in ['year', 'lifetime']:
                        unit = 'tons'
                        factor = CO2_AVOIDANCE_TON
                    co2avoided_period['total'] = total['total'] * factor
                    co2avoided_period['topic'] = 'co2avoided_' + period
                    co2avoided_period['unit'] = unit
                    co2avoided_period['precision'] = 2
                    co2avoided_period['factor'] = factor
                    co2avoided.append(co2avoided_period)
                    break
        return co2avoided

    async def current_dc_values(self):
        """Get the current DC power of each inverter."""
        return await self.get_composite(DC_MEASUREMENTS)

    async def snapshot(self):
        """Get the values of interest fromf each inverter."""
        return await self.get_composite(SITE_SNAPSHOT )

    async def current_state(self):
        """Get the current status of each inverter."""
        return await self.get_composite(['6180_08416500'])

    async def current_production(self):
        """Get the current production of each inverter and the total of all inverters."""
        return await self.get_composite(['6100_40263F00'])

    async def total_production(self):
        """Get the total production of each inverter and the total of all inverters."""
        return await self.get_composite(['6400_0046C300'])

    async def get_composite(self, keys):
        """Get the key values of each inverter and optionally the total."""
        sensors = []
        for key in keys:
            composite = {}
            total = 0
            for inverter in self._inverters:
                calculate_total = AGGREGATE_KEYS.count(key)
                type = inverter.get_type(key)
                scale = inverter.get_scale(key)
                unit = inverter.get_unit(key)
                state = await inverter.get_state(key)
                for inv, values_list in state.items():
                    if len(values_list) == 1:
                        if type == 0:
                            for index, value in enumerate(values_list):
                                result = value.get('val', None)
                                if result == None:
                                    result = 0
                            if scale != 1:
                                result = result * scale
                            if calculate_total:
                                total += result
                        elif type == 1:
                            for index, value in enumerate(values_list):
                                tag_list = value.get('val')
                                result = inverter.tag(tag_list[0].get('tag'))
                        else:
                            logger.warning(f"unexpected sma type: {type}")
                    else:
                        # sensors that have multiple values (limited)
                        if type == 0:
                            subtotal = 0
                            result = {}
                            subkeys = ['a', 'b', 'c']
                            for index, value in enumerate(values_list):
                                val = value.get('val', None)
                                if val == None:
                                    val = 0
                                if scale != 1:
                                    val *= scale
                                if calculate_total:
                                    subtotal += val
                                result[subkeys[index]] = val

                            if calculate_total:
                                result['total'] = subtotal
                                total += subtotal
                        else:
                            logger.warning(f"unexpected sma type: {type}")

                    composite[inv] = result
                    if unit:
                        composite['unit'] = unit

            if calculate_total:
                composite['total'] = total

            composite['topic'] = MQTT_TOPICS.get(key, '???')
            sensors.append(composite)
        return sensors

    async def task_manager(self, queue5, queue30, queue60):
        """###."""
        SLEEP = 0.5
        last_tick = int(time.time())
        info = dict(time = last_tick, daylight = self.daylight(), dawn = self._dawn, delta = self._solar_time_diff, dusk = self._dusk)

        while True:
            tick = int(time.time())
            if tick != last_tick:
                last_tick = tick
                info['time'] = tick
                if info['daylight']:
                    if tick % 5 == 0:
                        queue5.put_nowait(info)
                    if tick % 30 == 0:
                        queue30.put_nowait(info)
                    if tick % 60 == 0:
                        queue60.put_nowait(info)
                elif tick % 120 == 0:
                    queue5.put_nowait(info)
                    queue30.put_nowait(info)
                    queue60.put_nowait(info)

                if tick % 300 == 0:
                    info['daylight'] = self.daylight()
                    self.sun_events(info['daylight'])

            await asyncio.sleep(SLEEP)

    async def task_5s(self, queue):
        """###."""
        longest = 0.0
        while True:
            try:
                info = await queue.get()
                with Timer() as t:
                    await self.read_instantaneous()

            finally:
                #if t.interval > longest:
                    #longest = t.interval
                    #logger.info(f"longest read_instantaneous() request took {longest:.3f} sec.")
                queue.task_done()

            # Broadcast
            mqtt.publish(await self.snapshot())
            #mqtt.publish(await self.current_production())
            #mqtt.publish(await self.current_dc_values())
            #mqtt.publish(await self.current_state())

    async def task_30s(self, queue):
        """###."""
        while True:
            info = await queue.get()
            queue.task_done()
            mqtt.publish(await self.production_stats())
            #mqtt.publish(await self.total_production())

    async def task_60s(self, queue):
        """###."""
        while True:
            info = await queue.get()
            queue.task_done()
            mqtt.publish(await self.co2_avoided())
            mqtt.publish(await self.read_history_period('day'))
            mqtt.publish(await self.read_history_period('month'))
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


if __name__ == "__main__":

    async def main():
        """###."""
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

                end_time =  datetime.datetime.combine(datetime.date.today(), datetime.time(23, 50))

                while True:
                    await asyncio.sleep(5)

                    current_time = datetime.datetime.now()
                    if current_time > end_time:
                        break

            finally:
                logger.info("Closing multisma2 application, see you on the other side of midnight")
                await site.close()
                logfiles.close()


#try:
    # Start collecting
    asyncio.run(main())

#except KeyboardInterrupt:
#    pass



#                await asyncio.gather(*(inverter.display_metadata(True) for inverter in site._inverters))
#                await asyncio.gather(*(inverter.display_metadata() for inverter in site._inverters))
#                await asyncio.gather(*(inverter.read_values(['6400_0046C300', '6100_0046C200']) for inverter in inverters))
#                await asyncio.gather(*(inverter.display_state() for inverter in site._inverters))
#            await asyncio.gather(*(inverter.display_history() for inverter in self._inverters))
#                await asyncio.gather(*(inverter.display_history() for inverter in site._inverters))

#                pprint(await current_production(site._inverters))
#                pprint(await current_production(inverters))
#                pprint(await total_production(inverters))
