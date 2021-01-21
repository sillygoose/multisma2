"""Code to interface with the SMA inverters and return the results."""

import asyncio
import datetime
import time
import logging

from pprint import pprint
from dateutil import tz

from astral.sun import sun
from astral import LocationInfo, now

import clearsky

from inverter import Inverter
from influx import InfluxDB
import mqtt

from configuration import SITE_LATITUDE, SITE_LONGITUDE, SITE_NAME, SITE_REGION, TIMEZONE
from configuration import SITE_PANEL_AREA, SITE_PANEL_EFFICIENCY, SITE_AZIMUTH, SITE_TILT

from configuration import CO2_AVOIDANCE
from configuration import INVERTERS
from configuration import INFLUXDB_ENABLE, INFLUXDB_DATABASE, INFLUXDB_IPADDR, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD
from configuration import APPLICATION_LOG_LOGGER_NAME


logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)

influxdb = InfluxDB(INFLUXDB_ENABLE)


# Unlisted topics will use the key as the MQTT topic name
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
    '6180_08412800': 'status/general_operating_status',
    '6180_08416400': 'status/grid_relay',
    '6180_08414C00': 'status/condition',
    # This key is the same as 'production/total' but not aggregated
    '6400_00260100': 'total_production',
}

# These are keys that we calculate a total across all inverters
AGGREGATE_KEYS = [
    '6100_40263F00',    # AC grid power (current)
    '6100_0046C200',    # PV generation power (current)
    '6400_0046C300',    # Meter count and PV gen. meter (total power)
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
    def __init__(self, session):
        """Create a new PVSite object."""
        self._inverters = []
        self._session = session
        self._tasks = None
        self._total_production = None
        self._cached_keys = []
        self._scaling = 1
        self._daylight = None
        self._task_gather = None
        self._dawn = None
        self._dusk = None
        self._siteinfo = LocationInfo(SITE_NAME, SITE_REGION, TIMEZONE, SITE_LATITUDE, SITE_LONGITUDE)
        self._tzinfo = tz.gettz(TIMEZONE)

    async def start(self):
        """Initialize the PVSite object."""
        if not influxdb.start(host=INFLUXDB_IPADDR, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD): return False
        if not mqtt.start(): return False

        for inverter in INVERTERS:
            self._inverters.append(Inverter(inverter['name'], inverter['ip'], inverter['user'], inverter['password'], self._session))

        cached_keys = await asyncio.gather(*(inverter.start() for inverter in self._inverters))
        if None in cached_keys: return False
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
        influxdb.stop()
 
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
            {'scale': 30},     # night (30 is 5 minute samples)
            {'scale': 1},      # day (1 is 10 second samples)
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
            await asyncio.gather(
                self.solar_data_update(),
                self.update_total_production(),
            )
            influxdb.write_points(self.irradiance_today())
            influxdb.write_history(await self.get_yesterday_production(), 'production/today')

    def irradiance_today(self):
        # Create location object to store lat, lon, timezone
        site = clearsky.site_location(SITE_LATITUDE, SITE_LONGITUDE, tz=TIMEZONE)
        dawn = self._dawn
        dusk = self._dusk + datetime.timedelta(minutes=10)
        start = datetime.datetime(dawn.year, dawn.month, dawn.day, dawn.hour, int(int(dawn.minute/10)*10))
        stop = datetime.datetime(dusk.year, dusk.month, dusk.day, dusk.hour, int(int(dusk.minute/10)*10))

        # Get irradiance data for today and convert to InfluxDB line protocol
        irradiance = clearsky.get_irradiance(site=site, start=start.strftime("%Y-%m-%d %H:%M:00"), end=stop.strftime("%Y-%m-%d %H:%M:00"), tilt=SITE_TILT, azimuth=SITE_AZIMUTH, freq='10min')
        lp_points = []
        for point in irradiance:
            t = point['t']
            v = point['v'] * SITE_PANEL_AREA * SITE_PANEL_EFFICIENCY
            lp = f'prediction,inverter="site" production={round(v, 1)} {t}'
            lp_points.append(lp)
        return lp_points

    async def scheduler(self, queues):
        """Task to schedule actions at regular intervals."""
        SLEEP = 0.5
        last_tick = int(time.time()) / self._scaling
        while True:
            tick = int(time.time()) / self._scaling
            if tick != last_tick:
                last_tick = tick
                if tick % 10 == 0:
                    await asyncio.gather(
                        self.update_instantaneous(),
                        self.update_total_production(),
                    )
                    queues.get('10s').put_nowait(tick)
                if tick % 30 == 0:
                    queues.get('30s').put_nowait(tick)
                if tick % 60 == 0:
                    queues.get('60s').put_nowait(tick)
                if tick % 300 == 0:
                    queues.get('300s').put_nowait(tick)
            await asyncio.sleep(SLEEP)

    async def task_10s(self, queue):
        """Work done every 10 seconds."""
        while True:
            await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.snapshot(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)
                influxdb.write_sma_sensors(sensor)

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
            await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.production_history(),
                self.co2_avoided(),
            )
            for sensor in sensors:
                mqtt.publish(sensor)

    async def task_300s(self, queue):
        """Work done every 300 seconds (5 minutes)."""
        while True:
            await queue.get()
            queue.task_done()
            sensors = await asyncio.gather(
                self.total_production(),
            )
            for sensor in sensors:
                influxdb.write_sma_sensors(sensor)

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

    async def update_total_production(self):
        """Get the daily, monthly, yearly, and lifetime production values."""
        total_production_list = await self.total_production()
        raw_stats = []
        for total_production in total_production_list:
            unit = total_production.pop('unit')
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
                    period_stats['unit'] = unit

                raw_stats.append(period_stats)

        self._total_production = raw_stats
        return raw_stats

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
            tp.pop('unit')
            history = {}
            for key, value in tp.items():
                production = value * settings['scale']
                history[key] = round(production, settings['precision']) if settings['precision'] else int(production)

            history['topic'] = 'production/' + period
            history['unit'] = settings['unit']
            histories.append(history)

        return histories

    async def co2_avoided(self):
        """Calculate the CO2 avoided by solar production."""
        CO2_AVOIDANCE_KG = CO2_AVOIDANCE
        #CO2_AVOIDANCE_TON = CO2_AVOIDANCE_KG / 1000
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
            tp.pop('unit')
            co2avoided_period = {}
            for key, value in tp.items():
                co2 = value * settings['scale'] * settings['factor']
                co2avoided_period[key] = round(co2, settings['precision']) if settings['precision'] else int(co2)

            co2avoided_period['topic'] = 'co2avoided/' + period
            co2avoided_period['unit'] = settings['unit']
            co2avoided_period['factor'] = settings['factor']
            co2avoided.append(co2avoided_period)

        return co2avoided

    async def inverter_efficiency(self):
        """Calculate the the inverter efficiencies."""
        dc_power = (await self.get_composite(["6380_40251E00"]))[0]
        ac_power = (await self.get_composite(["6100_40263F00"]))[0]
        efficiencies = {}
        for k, v in ac_power.items():
            if k in ['unit', 'precision', 'topic']: continue
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
                results = await asyncio.gather(*(inverter.get_state(key) for inverter in self._inverters))
            else:
                results = await asyncio.gather(*(inverter.read_key(key) for inverter in self._inverters))

            composite = {}
            total = 0
            calculate_total = AGGREGATE_KEYS.count(key)
            for inverter in results:
                name = inverter.get('name')
                result = inverter.get(key)
                val = result.get('val', None)
                unit = result.get('unit', None)
                precision = result.get('precision', None)
                if isinstance(val, dict):
                    if calculate_total:
                        subtotal = val.get(name)
                        total += subtotal
                else:
                    if calculate_total:
                        total += val

                if unit: composite['unit'] = unit
                if precision is not None: composite['precision'] = precision
                composite[name] = val

            if calculate_total: composite['site'] = total
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
        """FCurrent if currently in daylight conditions."""
        return self._daylight


def day_of_year() -> str:
    """Return the DOY in a pretty form for logging."""
    doy = int(datetime.datetime.now().strftime('%j'))
    suffixes = ['st', 'nd', 'rd', 'th']
    return f"{doy}{suffixes[3 if doy >= 4 else doy-1]}"
