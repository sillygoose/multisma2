"""Code to interface with the SMA inverters and return the results."""

import os
import asyncio
import datetime
import time
import logging

# from pprint import pprint
from dateutil import tz

from astral.sun import sun, elevation, azimuth
from astral import LocationInfo, now

import clearsky
import version

from inverter import Inverter
from influx import InfluxDB
import mqtt


logger = logging.getLogger('multisma2')


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
    '6100_40263F00',    # AC grid power (current)
    '6100_0046C200',    # PV generation power (current)
    '6400_0046C300',    # Meter count and PV gen. meter (total Wh meter)
    '6380_40251E00',    # DC power (totals for site and each inverter)
]

SITE_SNAPSHOT = [
    '6100_40263F00',    # AC grid power (current)
    '6380_40251E00',    # DC power (current)
    '6180_08416500',    # Status: Reason for derating
    '6180_08412800',    # Status: General operating status
    '6180_08416400',    # Status: Grid relay
    '6180_08414C00',    # Status: Condition
]


class PVSite():
    """Class to describe a PV site with one or more inverters."""
    def __init__(self, session, config):
        """Create a new PVSite object."""
        self._config = config
        self._inverters = []
        self._session = session
        self._siteinfo = None
        self._tzinfo = None
        self._tasks = None
        self._total_production = None
        self._cached_keys = []
        self._scaling = 1
        self._daylight = None
        self._task_gather = None
        self._dawn = None
        self._dusk = None
        self._influx = InfluxDB()

    def check_config(self, config):
        """Check that the needed YAML options exist."""
        required_keys = ['site', 'solar_properties', 'inverters']
        for key in required_keys:
            if key not in config.keys():
                logger.error(f"Missing required 'multisma2' option in YAML file: '{key}'")
                return False

        required_keys = ['name', 'region', 'tz', 'latitude', 'longitude', 'elevation', 'co2_avoided']
        for key in required_keys:
            if key not in config.site.keys():
                logger.error(f"Missing required 'site' option in YAML file: '{key}'")
                return False

        required_keys = ['azimuth', 'tilt', 'area', 'efficiency', 'rho']
        for key in required_keys:
            if key not in config.solar_properties.keys():
                logger.error(f"Missing required 'solar_properties' option in YAML file: '{key}'")
                return False

    def check_inverter_config(self, config):
        """Check that the inverter keys are present."""
        key = 'inverter'
        if key not in config.keys():
            logger.error(f"Missing required 'inverters' option in YAML file: '{key}'")
            return False

        inverter_keys = config.get('inverter').keys()
        required_keys = ['name', 'url', 'user', 'password']
        for key in required_keys:
            if key not in inverter_keys:
                logger.error(f"Missing required 'inverter' option in YAML file: '{key}'")
                return False

    async def start(self):
        """Initialize the PVSite object."""
        config = self._config
        if self.check_config(config) is False:
            return False

        self._siteinfo = LocationInfo(config.site.name, config.site.region, config.site.tz, config.site.latitude, config.site.longitude)
        self._tzinfo = tz.gettz(config.site.tz)

        for inverter in config.inverters:
            if self.check_inverter_config(inverter) is False:
                return False
            inv = inverter.get('inverter', None)
            if inv is not None:
                self._inverters.append(Inverter(inv['name'], inv['url'], inv['user'], inv['password'], self._session))

        if 'influxdb2' in config.keys():
            if not self._influx.start(config=config.influxdb2):
                return False

        if 'mqtt' in config.keys():
            if not mqtt.start(config=config.mqtt):
                return False

        cached_keys = await asyncio.gather(*(inverter.start() for inverter in self._inverters))
        if None in cached_keys:
            return False
        self._cached_keys = cached_keys[0]
        return True

    async def run(self):
        """Run the site and wait for an event to exit."""
        await asyncio.gather(
            self.solar_data_update(),
            self.update_instantaneous(),
            self.update_total_production(),
        )

        queues = {
            '10s': asyncio.Queue(),
            '30s': asyncio.Queue(),
            '60s': asyncio.Queue(),
            '300s': asyncio.Queue(),
        }
        self._task_gather = asyncio.gather(
            self.daylight(),
            self.midnight(),
            self.scheduler(queues),
            self.task_10s(queues.get('10s')),
            self.task_30s(queues.get('30s')),
            self.task_60s(queues.get('60s')),
            self.task_300s(queues.get('300s')),
        )
        await self._task_gather

    async def stop(self):
        """Shutdown the site."""
        if self._task_gather:
            self._task_gather.cancel()

        await asyncio.gather(*(inverter.stop() for inverter in self._inverters))
        self._influx.stop()

    async def solar_data_update(self) -> None:
        """Update the sun data used to sequence operaton."""
        astral_now = now(tzinfo=self._tzinfo)
        astral = sun(observer=self._siteinfo.observer, tzinfo=self._tzinfo)
        self._dawn = astral['dawn']
        self._dusk = astral['dusk']
        self._daylight = self._dawn < astral_now < self._dusk
        logger.info(f"Dawn occurs at {self._dawn.strftime('%H:%M')}, "
                    f"noon is at {astral['noon'].strftime('%H:%M')}, "
                    f"and dusk occurs at {self._dusk.strftime('%H:%M')} "
                    f"on this {day_of_year()} day of {astral_now.year}")

    async def daylight(self) -> None:
        """Task to determine when it is daylight and daylight changes."""
        SAMPLE_PERIOD = [
            {'scale': 18},     # night (18 is 3 minute sample intervals)
            {'scale': 1},      # day (1 is 10 second sample intervals)
        ]
        while True:
            astral_now = now(tzinfo=self._tzinfo)
            self._daylight = False
            if astral_now < self._dawn:
                next_event = self._dawn - astral_now
            elif astral_now > self._dusk:
                tomorrow = astral_now + datetime.timedelta(days=1)
                astral = sun(date=tomorrow.date(), observer=self._siteinfo.observer, tzinfo=self._tzinfo)
                next_event = astral['dawn'] - astral_now
            else:
                self._daylight = True
                next_event = self._dusk - astral_now

            self._scaling = SAMPLE_PERIOD[self.is_daylight()].get('scale')

            FUDGE = 60
            await asyncio.sleep(next_event.total_seconds() + FUDGE)

    async def midnight(self) -> None:
        """Task to wake up after midnight and update the solar data for the new day."""
        while True:
            now = datetime.datetime.now()
            tomorrow = now + datetime.timedelta(days=1)
            midnight = datetime.datetime.combine(tomorrow, datetime.time(0, 5))
            await asyncio.sleep((midnight - now).total_seconds())

            # Update internal sun info and the daily production
            logger.info(f"multisma2 inverter collection utility {version.get_version()}, PID is {os.getpid()}")
            await self.solar_data_update()
            await asyncio.gather(*(inverter.read_inverter_production() for inverter in self._inverters))
            await self.update_total_production()
            self._influx.write_history(await self.get_yesterday_production(), 'production/midnight')

    async def scheduler(self, queues):
        """Task to schedule actions at regular intervals."""
        SLEEP = 0.5
        timestamp = time.time_ns() // 1000000000
        last_tick = timestamp / self._scaling
        while True:
            timestamp = time.time_ns() // 1000000000
            tick = timestamp / self._scaling
            if tick != last_tick:
                last_tick = tick
                if tick % 10 == 0:
                    await asyncio.gather(
                        self.update_instantaneous(),
                        self.update_total_production(),
                    )
                    queues.get('10s').put_nowait(timestamp)
                if tick % 30 == 0:
                    queues.get('30s').put_nowait(timestamp)
                if tick % 60 == 0:
                    queues.get('60s').put_nowait(timestamp)
                if tick % 300 == 0:
                    queues.get('300s').put_nowait(timestamp)
            await asyncio.sleep(SLEEP)

    async def task_10s(self, queue):
        """Work done every 10 seconds."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.snapshot(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)
                self._influx.write_sma_sensors(sensor=sensor, timestamp=timestamp)

    async def task_30s(self, queue):
        """Work done every 30 seconds."""
        while True:
            await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.inverter_efficiency(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)

    async def task_60s(self, queue):
        """Work done every 60 seconds."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.production_history(),
                self.co2_avoided(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)

            sensors = await asyncio.gather(
                self.sun_position(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)
                self._influx.write_sma_sensors(sensor=sensor, timestamp=timestamp)

    async def task_300s(self, queue):
        """Work done every 300 seconds (5 minutes)."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.total_production(),
                self.sun_irradiance(timestamp=timestamp),
            )
            for sensor in sensors:
                self._influx.write_sma_sensors(sensor=sensor, timestamp=timestamp)

    async def update_instantaneous(self):
        """Update the instantaneous cache from the inverter."""
        await asyncio.gather(*(inverter.read_instantaneous() for inverter in self._inverters))

    async def get_yesterday_production(self):
        """Get the total production meter values for the previous day."""
        now = datetime.datetime.now()
        yesterday = now - datetime.timedelta(days=1)
        start = datetime.datetime.combine(yesterday.date(), datetime.time(0, 0))
        stop = datetime.datetime.combine(now.date(), datetime.time(0, 0))
        production = await self.get_production_history(int(start.timestamp()), int(stop.timestamp()))
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

    async def update_total_production(self) -> None:
        """Get the daily, monthly, yearly, and lifetime production values."""
        total_productions = await self.total_production()
        # [{'sb71': 4376401, 'sb72': 4366596, 'sb51': 3121662, 'site': 11864659, 'topic': 'production/total_wh'}]
        # logger.debug(f"total_productions: {total_productions}")
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

        logger.debug(f"update_total_production()/updated_total_production: {updated_total_production}")
        # [{'sb71': 157, 'site': 442, 'period': 'today', 'sb72': 176, 'sb51': 109},
        #  {'sb71': 97028, 'site': 260611, 'period': 'month', 'sb72': 97827, 'sb51': 65756},
        #  {'sb71': 97028, 'site': 260611, 'period': 'year', 'sb72': 97827, 'sb51': 65756},
        #  {'sb71': 4376363, 'site': 11864551, 'period': 'lifetime', 'sb72': 4366554, 'sb51': 3121634}]
        self._total_production = updated_total_production

    async def production_history(self):
        """Get the daily, monthly, yearly, and lifetime production values."""
        PRODUCTION_SETTINGS = {
            'today': {'unit': 'kWh', 'scale': 0.001, 'precision': 2},
            'month': {'unit': 'kWh', 'scale': 0.001, 'precision': 0},
            'year': {'unit': 'kWh', 'scale': 0.001, 'precision': 0},
            'lifetime': {'unit': 'kWh', 'scale': 0.001, 'precision': 0},
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

        logger.debug(f"production_history()/histories: {histories}")
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
        """Calculate the sun is in the sky."""
        site_properties = self._config.site
        solar_properties = self._config.solar_properties
        current_igc = clearsky.current_global_irradiance(site_properties=site_properties, solar_properties=solar_properties, timestamp=timestamp)
        site_igc = current_igc * solar_properties.area * solar_properties.efficiency
        results = [{'topic': 'sun/irradiance', 'irradiance': round(current_igc, 1), 'solar_potential': round(site_igc, 1)}]
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
            co2avoided_period['factor'] = settings['factor']
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

    async def snapshot(self):
        """Get the values of interest from each inverter."""
        return await self.get_composite(SITE_SNAPSHOT)

    async def total_production(self):
        """Get the total production of each inverter and the total of all inverters."""
        return await self.get_composite(["6400_0046C300"])

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
        """Determines if a key in the inverter cache."""
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
