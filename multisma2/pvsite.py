"""Code to interface with the SMA inverters and return the results."""

import asyncio
import datetime
import time
import logging
from dateutil import tz

from astral.sun import sun, elevation, azimuth
from astral import LocationInfo, now

import clearsky

from inverter import Inverter
from influx import InfluxDB
import mqtt

from exceptions import FailedInitialization


_LOGGER = logging.getLogger('multisma2')

# Default sampling values used when not defined in the configuration file
_DEFAULT_FAST = 30
_DEFAULT_MEDIUM = 60
_DEFAULT_SLOW = 120
_DEFAULT_NIGHT = 900

# Unlisted topics will use the key as the MQTT topic name
MQTT_TOPICS = {
    '6100_0046C200': 'production/current',
    '6400_0046C300': 'production/total_wh',
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
    '6180_08412800': 'status/general_operating_status',
    '6180_08416400': 'status/grid_relay',
    '6180_08414C00': 'status/condition',
    # This key is the same as 'production/total_wh' but is not aggregated
    '6400_00260100': 'production/totalwh2',
}

# These are keys that we calculate a total across all inverters (if multiple inverters)
AGGREGATE_KEYS = [
    '6100_40263F00',    # AC grid power (totals for site and each inverter)
    '6100_0046C200',    # PV generation power (instantaneous)
    '6400_0046C300',    # Meter count and PV gen. meter (total Wh meter)
    '6380_40251E00',    # DC power (totals for site and each inverter)
]

SITE_SNAPSHOT = [       # Instantaneous values
    '6100_40263F00',    # AC grid power (by inverter/site)
    '6380_40251E00',    # DC power (by inverter/site)
    '6380_40451F00',    # DC voltage (by inverter/string)
    '6380_40452100',    # DC current (by inverter/string)
]

SITE_STATUS = [
    '6180_08416500',    # Status: Reason for derating
    '6180_08412800',    # Status: General operating status
    '6180_08416400',    # Status: Grid relay
    '6180_08414C00',    # Status: Condition
]


class PVSite():
    """Class to describe a PV site with one or more inverters."""

    def __init__(self, session, config):
        """Create a new PVSite object."""
        self._session = session
        self._config = config
        self._inverters = []
        self._siteinfo = None
        self._tzinfo = None
        self._tasks = None
        self._task_gather = None
        self._total_production = None
        self._cached_keys = []
        self._daylight = None
        self._dawn = None
        self._dusk = None
        self._influxdb_client = InfluxDB(config)
        self._sampling_fast = _DEFAULT_FAST
        self._sampling_medium = _DEFAULT_MEDIUM
        self._sampling_slow = _DEFAULT_SLOW

    async def start(self) -> bool:
        """Initialize the PVSite object."""
        config = self._config

        site = config.site
        self._siteinfo = LocationInfo(site.name, site.region, site.tz, site.latitude, site.longitude)
        self._tzinfo = tz.gettz(config.site.tz)

        for inverter in config.inverters:
            try:
                i = inverter.get('inverter')
                invObject = Inverter(i.get('name'), i.get('url'), i.get('username'), i.get('password'), self._session)
                self._inverters.append(invObject)
            except Exception as e:
                _LOGGER.error(f"An error occurred while setting up the inverters: {e}")
                return False

        if 'influxdb2' in config.keys():
            try:
                result = self._influxdb_client.start()
                if result is False:
                    return False
            except FailedInitialization:
                return False
        else:
            _LOGGER.warning("No support for InfluxDB included in YAML file")

        if 'mqtt' in config.keys():
            if not mqtt.start(config=config.mqtt):
                return False
        else:
            _LOGGER.warning("No support for MQTT included in YAML file")

        if 'settings' in config.keys() and 'sampling' in config.settings.keys():
            self._sampling_fast = config.settings.sampling.get('fast', _DEFAULT_FAST)
            self._sampling_medium = config.settings.sampling.get('medium', _DEFAULT_MEDIUM)
            self._sampling_slow = config.settings.sampling.get('slow', _DEFAULT_SLOW)

        inverters = await asyncio.gather(*(inverter.start() for inverter in self._inverters))
        success = True
        for inverter in inverters:
            if inverter.get('keys', None) is None:
                _LOGGER.error(
                    f"Connection to inverter '{inverter.get('name')}' failed: {inverter.get('error', 'None')}")
                success = False
        if not success:
            return False

        self._cached_keys = inverters[0].get('keys')
        return True

    async def run(self):
        """Run the site and wait for an event to exit."""
        fast, medium, slow = self._sampling_fast, self._sampling_medium, self._sampling_slow
        _LOGGER.info(f"multisma2 sampling at {fast}/{medium}/{slow} second intervals")

        await asyncio.gather(
            self.solar_data_update(),
            self.read_instantaneous(daylight=True),
            self.update_total_production(daylight=True),
        )

        queues = {
            'fast': asyncio.Queue(),
            'medium': asyncio.Queue(),
            'slow': asyncio.Queue(),
        }
        self._task_gather = asyncio.gather(
            self.daylight(),
            self.midnight(),
            self.scheduler(queues),
            self.task_fast(queues.get('fast')),
            self.task_medium(queues.get('medium')),
            self.task_slow(queues.get('slow')),
            self.task_deletions(),
        )
        await self._task_gather

    async def stop(self):
        """Shutdown the site."""
        if self._task_gather:
            self._task_gather.cancel()

        await asyncio.gather(*(inverter.stop() for inverter in self._inverters))
        self._influxdb_client.stop()

    async def solar_data_update(self) -> None:
        """Update the sun data used to sequence operation."""
        astral_now = now(tzinfo=self._tzinfo)
        astral = sun(observer=self._siteinfo.observer, tzinfo=self._tzinfo)
        self._dawn = astral['dawn']
        self._dusk = astral['dusk']
        self._daylight = self._dawn < astral_now < self._dusk
        _LOGGER.info(
            f"Dawn occurs at {self._dawn.strftime('%H:%M')}, "
            f"noon is at {astral['noon'].strftime('%H:%M')}, "
            f"and dusk occurs at {self._dusk.strftime('%H:%M')} "
            f"on this {day_of_year()} day of {astral_now.year}"
        )

    async def daylight(self) -> None:
        """Task to determine when it is daylight and daylight changes."""
        while True:
            astral_now = now(tzinfo=self._tzinfo)
            previous = self._daylight
            if astral_now < self._dawn:
                self._daylight = False
                next_event = self._dawn - astral_now
                info = "Night: inverter data collection is inactive, cached updates being used"
            elif astral_now > self._dusk:
                self._daylight = False
                tomorrow = astral_now + datetime.timedelta(days=1)
                astral = sun(date=tomorrow.date(), observer=self._siteinfo.observer, tzinfo=self._tzinfo)
                next_event = astral['dawn'] - astral_now
                info = "Night: inverter data collection is inactive, cached updates being used"
            else:
                self._daylight = True
                next_event = self._dusk - astral_now
                info = f"Daylight: inverter data collection is active and sampling every at {self._sampling_fast} (fast), {self._sampling_medium} (medium), {self._sampling_slow} (slow) seconds"

            if previous != self._daylight:
                _LOGGER.info(f"{info}")

            FUDGE = 60
            await asyncio.sleep(next_event.total_seconds() + FUDGE)

    async def midnight(self) -> None:
        """Task to wake up after midnight and update the solar data for the new day."""
        while True:
            right_now = datetime.datetime.now()
            tomorrow = right_now + datetime.timedelta(days=1)
            midnight = datetime.datetime.combine(tomorrow, datetime.time(0, 0))
            await asyncio.sleep((midnight - right_now).total_seconds())

            await self.solar_data_update()

            retries = 0
            while True:
                if await self.read_instantaneous(True):
                    break
                if retries == 10:
                    _LOGGER.error(f"No response from inverter(s) after {retries} retries, giving up for now")
                    break
                _RETRY = 5
                retries += 1
                _LOGGER.debug(f"No response from inverter(s), will retry in {_RETRY} seconds")
                await asyncio.sleep(_RETRY)

            # fake daylight and update everything
            saved_daylight = self._daylight
            self._daylight = True
            await asyncio.gather(*(inverter.read_inverter_production() for inverter in self._inverters))
            self._influxdb_client.write_history(await self.get_yesterday_production(), 'production/midnight')

            await self.update_total_production(daylight=self._daylight)
            sensors = await asyncio.gather(
                self.production_totalwh(),
                self.production_history(),
            )
            for sensor in sensors:
                if sensor:
                    mqtt.publish(sensor)
                    self._influxdb_client.write_sma_sensors(sensor=sensor, timestamp=int(midnight.timestamp()))
            self._daylight = saved_daylight

    async def scheduler(self, queues):
        """Task to schedule actions at regular intervals."""
        SLEEP = 0.5
        last_tick = time.time_ns() // 1000000000
        while True:
            tick = time.time_ns() // 1000000000
            if tick != last_tick:
                last_tick = tick
                if tick % self._sampling_fast == 0:
                    await asyncio.gather(
                        self.read_instantaneous(self._daylight),
                        self.update_total_production(daylight=self._daylight),
                    )
                    queues.get('fast').put_nowait(tick)
                if tick % self._sampling_medium == 0:
                    queues.get('medium').put_nowait(tick)
                if tick % self._sampling_slow == 0:
                    queues.get('slow').put_nowait(tick)
            await asyncio.sleep(SLEEP)

    async def task_fast(self, queue):
        """Work done at a fast sample rate."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.production_snapshot(),
                self.status_snapshot(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)
                self._influxdb_client.write_sma_sensors(sensor=sensor, timestamp=timestamp)

    async def task_medium(self, queue):
        """Work done at a medium sample rate."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.production_totalwh(),
                self.production_history(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)
                self._influxdb_client.write_sma_sensors(sensor=sensor, timestamp=timestamp)

    async def task_slow(self, queue):
        """Work done at a slow sample rate."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.inverter_efficiency(),
                self.co2_avoided(),
                self.sun_irradiance(timestamp=timestamp),
                self.sun_position(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)
                self._influxdb_client.write_sma_sensors(sensor=sensor, timestamp=timestamp)

    async def task_deletions(self) -> None:
        """Task to remove older database entries."""
        delete_api = self._influxdb_client.delete_api()
        bucket = self._influxdb_client.bucket()
        org = self._influxdb_client.org()

        pruning_tasks = []
        config = self._config
        if 'influxdb2' in config.keys():
            if 'pruning' in config.influxdb2.keys():
                for pruning_task in config.influxdb2.pruning:
                    for task in pruning_task.values():
                        name = task.get('name', None)
                        keep_last = task.get('keep_last', 30)
                        predicate = task.get('predicate', None)
                        if name and predicate:
                            new_task = {'name': name, 'predicate': predicate, 'keep_last': keep_last}
                            pruning_tasks.append(new_task)
                            _LOGGER.debug(f"Added database pruning task: {new_task}")

        while True:
            right_now = datetime.datetime.now()
            midnight = datetime.datetime.combine(right_now + datetime.timedelta(days=1), datetime.time(2, 30))
            await asyncio.sleep((midnight - right_now).total_seconds())

            try:
                start = datetime.datetime(1970, 1, 1).isoformat() + 'Z'
                for task in pruning_tasks:
                    stop = datetime.datetime.combine(datetime.datetime.now(
                    ) - datetime.timedelta(days=keep_last), datetime.time(0, 0)).isoformat() + 'Z'
                    delete_api.delete(start, stop, predicate, bucket=bucket, org=org)
                    _LOGGER.debug(f"Pruned database '{bucket}': {predicate}, kept last {keep_last} days")
            except Exception as e:
                _LOGGER.debug(f"Unexpected exception in task_deletions(): {e}")

    async def read_instantaneous(self, daylight) -> bool:
        """Read the instantaneous sensors from the inverter."""
        results = await asyncio.gather(*(inverter.read_instantaneous(daylight) for inverter in self._inverters))
        inverter_list = []
        for result in results:
            if result.get('sensors', None) is None:
                inverter_list.append(f"{result.get('name')}({result.get('error')})")
        if len(inverter_list):
            _LOGGER.debug(f"read_instantaneous({daylight}), one more inverters returned no results: {inverter_list}")
            return False
        else:
            _LOGGER.debug(f"read_instantaneous({daylight}) was successful")
        return True

    async def get_yesterday_production(self):
        """Get the total production meter values for the previous day."""
        td_fudge = datetime.timedelta(minutes=10)
        right_now = datetime.datetime.now()
        yesterday = right_now - datetime.timedelta(days=1)
        start = datetime.datetime.combine(yesterday.date(), datetime.time(0, 0)) - td_fudge
        stop = datetime.datetime.combine(right_now.date(), datetime.time(0, 0)) - td_fudge
        production = await self.get_production_history(int(start.timestamp()), int(stop.timestamp()))
        _LOGGER.debug(f"get_yesterday_production(start={start}, stop={stop}): {production}")
        return production

    async def get_production_history(self, start, stop):
        """Get the production totals for a given period and create a site total."""
        production = await asyncio.gather(*(inverter.read_history(start, stop) for inverter in self._inverters))
        total = {}
        for inverter in production:
            for i in range(1, len(inverter)):
                t = inverter[i]['t']
                v = inverter[i]['v']
                if v is None:
                    continue
                if t in total:
                    total[t] += v
                else:
                    total[t] = v

        site_total = []
        for t, v in total.items():
            site_total.append({'t': t, 'v': v})
        site_total.insert(0, {'inverter': 'site'})
        production.append(site_total)
        return production

    async def update_total_production(self, daylight) -> None:
        """Get the daily, monthly, yearly, and lifetime total Wh production values."""
        if not daylight:
            return

        total_productions = await self.production_totalwh()
        # [{'sb71': 4376401, 'sb72': 4366596, 'sb51': 3121662, 'site': 11864659, 'topic': 'production/total_wh'}]
        # _LOGGER.debug(f"total_productions: {total_productions}")
        updated_total_production = []
        for total_production in total_productions:
            for period in ['today', 'month', 'year', 'lifetime']:
                period_stats = {}
                inverter_periods = await asyncio.gather(
                    *(inverter.start_production(period) for inverter in self._inverters)
                )

                total = 0
                for inverter in inverter_periods:
                    for inverter_name, history_value in inverter.items():
                        period_total = total_production[inverter_name] - history_value
                        total += period_total
                        period_stats[inverter_name] = period_total

                    period_stats['site'] = total
                    period_stats['period'] = period

                updated_total_production.append(period_stats)

        _LOGGER.debug(f"update_total_production(): {updated_total_production}")
        # [{'sb71': 157, 'site': 442, 'period': 'today', 'sb72': 176, 'sb51': 109},
        #  {'sb71': 97028, 'site': 260611, 'period': 'month', 'sb72': 97827, 'sb51': 65756},
        #  {'sb71': 97028, 'site': 260611, 'period': 'year', 'sb72': 97827, 'sb51': 65756},
        #  {'sb71': 4376363, 'site': 11864551, 'period': 'lifetime', 'sb72': 4366554, 'sb51': 3121634}]
        self._total_production = updated_total_production

    async def production_history(self):
        """Get the daily, monthly, yearly, and lifetime production values."""
        PRODUCTION_SETTINGS = {
            'today': {'unit': 'kWh', 'scale': 0.001, 'precision': 3},
            'month': {'unit': 'kWh', 'scale': 0.001, 'precision': 3},
            'year': {'unit': 'kWh', 'scale': 0.001, 'precision': 3},
            'lifetime': {'unit': 'kWh', 'scale': 0.001, 'precision': 3},
        }

        histories = []
        for period in ['today', 'month', 'year', 'lifetime']:
            settings = PRODUCTION_SETTINGS.get(period)
            tp = self.find_total_production(period)
            period = tp.pop('period')
            history = {}
            for key, value in tp.items():
                production = value * settings['scale']
                history[key] = round(production, settings['precision']) if settings['precision'] else int(production)

            history['topic'] = 'production/' + period
            histories.append(history)

        _LOGGER.debug(f"production_history(): {histories}")
        # [{'sb71': 0.21, 'site': 0.6, 'sb72': 0.24, 'sb51': 0.15, 'topic': 'production/today'},
        #  {'sb71': 97, 'site': 260, 'sb72': 97, 'sb51': 65, 'topic': 'production/month'},
        #  {'sb71': 97, 'site': 260, 'sb72': 97, 'sb51': 65, 'topic': 'production/year'},
        #  {'sb71': 4376, 'site': 11864, 'sb72': 4366, 'sb51': 3121, 'topic': 'production/lifetime'}]
        return histories

    async def sun_position(self):
        """Calculate where the sun is in the sky."""
        astral_now = now(tzinfo=self._tzinfo)
        sun_elevation = elevation(observer=self._siteinfo.observer, dateandtime=astral_now)
        sun_azimuth = azimuth(observer=self._siteinfo.observer, dateandtime=astral_now)
        results = [{'topic': 'sun/position', 'elevation': round(sun_elevation, 1), 'azimuth': round(sun_azimuth, 1)}]
        return results

    async def sun_irradiance(self, timestamp):
        """Calculate the estimated irradiation available."""
        site_properties = self._config.site
        solar_properties = self._config.solar_properties
        igc = clearsky.current_global_irradiance(
            site_properties=site_properties, solar_properties=solar_properties, timestamp=timestamp)
        results = [{'topic': 'sun/irradiance', 'modeled': round(igc, 1)}]
        return results

    async def co2_avoided(self):
        """Calculate the CO2 avoided by solar production."""
        CO2_AVOIDANCE_KG = self._config.site.co2_avoided
        CO2_SETTINGS = {
            'today': {'scale': 0.001, 'unit': 'kg', 'precision': 2, 'factor': CO2_AVOIDANCE_KG},
            'month': {'scale': 0.001, 'unit': 'kg', 'precision': 0, 'factor': CO2_AVOIDANCE_KG},
            'year': {'scale': 0.001, 'unit': 'kg', 'precision': 0, 'factor': CO2_AVOIDANCE_KG},
            'lifetime': {'scale': 0.001, 'unit': 'kg', 'precision': 0, 'factor': CO2_AVOIDANCE_KG},
        }

        co2avoided = []
        for period in ['today', 'month', 'year', 'lifetime']:
            settings = CO2_SETTINGS.get(period)
            tp = self.find_total_production(period)
            period = tp.pop('period')
            co2avoided_period = {}
            for key, value in tp.items():
                co2 = value * settings['scale'] * settings['factor']
                co2avoided_period[key] = round(co2, settings['precision']) if settings['precision'] else int(co2)

            co2avoided_period['topic'] = 'co2avoided/' + period
            co2avoided.append(co2avoided_period)

        return co2avoided

    async def inverter_efficiency(self):
        """Calculate the the inverter efficiencies."""
        dc_power = (await self.get_composite(["6380_40251E00"]))[0]
        ac_power = (await self.get_composite(["6100_40263F00"]))[0]
        efficiencies = {}
        for k, v in ac_power.items():
            if k in ['precision', 'topic']:
                continue
            dc = dc_power.get(k)
            denom = dc.get(k) if isinstance(dc, dict) else dc
            efficiencies[k] = 0.0 if denom == 0 else round((float(v) / denom) * 100, 2)
        efficiencies['topic'] = 'ac_measurements/efficiency'
        return [efficiencies]

    async def production_snapshot(self):
        """Get the production values of interest from each inverter."""
        return await self.get_composite(SITE_SNAPSHOT)

    async def status_snapshot(self):
        """Get the status values of interest from each inverter."""
        return await self.get_composite(SITE_STATUS)

    async def production_totalwh(self):
        """Get the total wH of each inverter and the total of all inverters."""
        return await self.get_composite(['6400_0046C300'])

    async def read_keys(self, keys):
        """Read a list of keys from the cache or the inverter(s)."""
        return await self.get_composite(keys)

    async def get_composite(self, keys):
        """Get the key values of each inverter and optionally create a site total."""
        sensors = []
        for key in keys:
            if self.cached_key(key):
                inverters = await asyncio.gather(*(inverter.get_state(key) for inverter in self._inverters))
            else:
                inverters = await asyncio.gather(*(inverter.read_key(key) for inverter in self._inverters))
                _LOGGER.warning(f"get_composite(): non-cached key '{key}'")

            composite = {}
            total = 0
            calculate_total = AGGREGATE_KEYS.count(key) and (len(inverters) > 1)
            for inverter in inverters:
                name = inverter.get('name')
                result = inverter.get(key)
                if not result:
                    continue
                val = result.get('val', None)
                precision = result.get('precision', None)
                if isinstance(val, dict):
                    if calculate_total:
                        subtotal = val.get(name)
                        total += subtotal
                else:
                    if calculate_total:
                        total += val

                if precision is not None:
                    composite['precision'] = precision
                composite[name] = val

            if calculate_total:
                composite['site'] = total
            composite['topic'] = MQTT_TOPICS.get(key, key)
            sensors.append(composite)

        return sensors

    def cached_key(self, key):
        """Determines if a key is in the inverter cache."""
        cached = key in self._cached_keys
        return cached

    def find_total_production(self, period):
        """Find the total production for a given period."""
        for d_period in self._total_production:
            if d_period.get('period') is period:
                return d_period.copy()
        return None

    def is_daylight(self) -> bool:
        """True if currently in daylight conditions."""
        return self._daylight


def day_of_year() -> str:
    """Return the DOY in a pretty form for logging."""
    doy = int(datetime.datetime.now().strftime('%j'))
    suffixes = ['st', 'nd', 'rd', 'th']
    return f"{doy}{suffixes[3 if doy >= 4 else doy-1]}"
