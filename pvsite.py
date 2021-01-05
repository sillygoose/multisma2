"""Code to interface with the SMA inverters and return the results."""

import asyncio
import datetime
import time
import logging

from pprint import pprint
from dateutil import tz

from inverter import Inverter
from influx import InfluxDB
import mqtt

from configuration import CO2_AVOIDANCE
from configuration import INVERTERS
from configuration import APPLICATION_LOG_LOGGER_NAME


logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


# Unlisted topics will use the key as the MQTT topic name
MQTT_TOPICS = {
    "6100_0046C200": "production/current",
    "6400_0046C300": "production/total",
    "6100_40263F00": "ac_measurements/power",
    "6100_00465700": "ac_measurements/frequency",
    "6180_08465A00": "ac_measurements/excitation_type",
    "6100_00464800": "ac_measurements/voltage/phase_l1",
    "6100_00464900": "ac_measurements/voltage/phase_l2",
    "6100_00464B00": "ac_measurements/voltage/phase_l1_l2",
    "6380_40251E00": "dc_measurements/power",
    "6380_40451F00": "dc_measurements/voltage",
    "6380_40452100": "dc_measurements/current",
    "6180_08416500": "status/reason_for_derating",
    "6180_08412800": "status/general_operating_status",
    "6180_08416400": "status/grid_relay",
    "6180_08414C00": "status/condition",
    # This key is the same as "production/total" but not aggregated
    "6400_00260100": "total_production",
}

# These are keys that we calculate a total across all inverters
AGGREGATE_KEYS = [
    "6100_40263F00",    # AC grid power (current)
    "6100_0046C200",    # PV generation power (current)
    "6400_0046C300",    # Meter count and PV gen. meter (total power)
    "6380_40251E00",    # DC power (1 per string)
]

SITE_SNAPSHOT = [
    "6100_40263F00",    # AC grid power (current)
    "6380_40251E00",    # DC power (current)
    "6180_08416500",    # Status: Reason for derating
    "6180_08412800",    # Status: General operating status
    "6180_08416400",    # Status: Grid relay
    "6180_08414C00",    # Status: Condition
    "6400_0046C300",    # AC Total yield (aggregated)
]


class PVSite:
    """Class to describe a PV site with one or more inverters."""
    def __init__(self, session):
        """Create a new PVSite object."""
        self._influx = InfluxDB()
        self._inverters = []
        self._total_production = None
        self._cached_keys = None
        for inverter in INVERTERS:
            self._inverters.append(Inverter(inverter["name"], inverter["ip"], inverter["user"], inverter["password"], session))

    async def start(self):
        """Initialize the PVSite object."""
        result = self._influx.start()
        if result:
            result = mqtt.start()
        if result:
            cached_keys = await asyncio.gather(*(inverter.start() for inverter in self._inverters))
            result = (None not in cached_keys) and result
            if result:
                self._cached_keys = cached_keys[0]
        return result

    async def stop(self):
        """Shutdown the PVSite object."""
        await asyncio.gather(*(inverter.stop() for inverter in self._inverters))
        self._influx.stop()

    async def read_instantaneous(self):
        """Update the instantaneous cache from the inverter."""
        await asyncio.gather(*(inverter.read_instantaneous() for inverter in self._inverters))

    async def read_total_production(self):
        """Get the daily, monthly, yearly, and lifetime production values."""
        total_production_list = await self.total_production()
        raw_stats = []
        for total_production in total_production_list:
            unit = total_production.pop("unit")
            for period in ["today", "month", "year", "lifetime"]:
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

                    period_stats["site"] = total
                    period_stats["period"] = period
                    period_stats["unit"] = unit

                raw_stats.append(period_stats)

        self._total_production = raw_stats
        return raw_stats

    def production_history(self):
        """Get the daily, monthly, yearly, and lifetime production values."""
        PRODUCTION_SETTINGS = {
            "today": {"unit": "kWh", "scale": 0.001, "precision": 2},
            "month": {"unit": "kWh", "scale": 0.001, "precision": 0},
            "year": {"unit": "MWh", "scale": 0.000001, "precision": 2},
            "lifetime": {"unit": "MWh", "scale": 0.000001, "precision": 2},
        }

        histories = []
        for period in ["today", "month", "year", "lifetime"]:
            settings = PRODUCTION_SETTINGS.get(period)
            tp = self.find_total_production(period)
            period = tp.pop("period")
            tp.pop("unit")
            history = {}
            for key, value in tp.items():
                production = value * settings["scale"]
                if settings["precision"]:
                    history[key] = round(production, settings["precision"])
                else:
                    history[key] = int(production)

            history["topic"] = "production/" + period
            history["unit"] = settings["unit"]
            histories.append(history)

        return histories

    def co2_avoided(self):
        """Calculate the CO2 avoided by solar production."""
        CO2_AVOIDANCE_KG = CO2_AVOIDANCE
        CO2_AVOIDANCE_TON = CO2_AVOIDANCE_KG / 1000
        CO2_SETTINGS = {
            "today": {"scale": 0.001, "unit": "kg", "precision": 2, "factor": CO2_AVOIDANCE_KG},
            "month": {"scale": 0.001, "unit": "kg", "precision": 0, "factor": CO2_AVOIDANCE_KG},
            "year": {"scale": 0.001, "unit": "tons", "precision": 2, "factor": CO2_AVOIDANCE_TON},
            "lifetime": {"scale": 0.001, "unit": "tons", "precision": 2, "factor": CO2_AVOIDANCE_TON},
        }

        co2avoided = []
        for period in ["today", "month", "year", "lifetime"]:
            settings = CO2_SETTINGS.get(period)
            tp = self.find_total_production(period)
            period = tp.pop("period")
            tp.pop("unit")
            co2avoided_period = {}
            for key, value in tp.items():
                co2 = value * settings["scale"] * settings["factor"]
                if settings["precision"]:
                    co2avoided_period[key] = round(co2, settings["precision"])
                else:
                    co2avoided_period[key] = int(co2)

            co2avoided_period["topic"] = "co2avoided/" + period
            co2avoided_period["unit"] = settings["unit"]
            co2avoided_period["factor"] = settings["factor"]
            co2avoided.append(co2avoided_period)

        return co2avoided

    async def inverter_efficiency(self):
        """Calculate the the inverter efficiencies."""
        dc_power_list = await self.get_composite(["6380_40251E00"])
        ac_power_list = await self.get_composite(["6100_40263F00"])
        efficiencies = {}
        ac_power = ac_power_list[0]
        dc_power = dc_power_list[0]
        ac_power.pop('precision')
        ac_power.pop('topic')
        ac_power.pop('unit')
        for k, v in ac_power.items():
            num = v
            dem = 0
            dc = dc_power.get(k)
            if isinstance(dc, dict):
                dem = dc.get('site')
            else:
                dem = dc_power.get(k)
            eff = round((float(num) / float(dem)) * 100, 2)
            efficiencies[k] = eff
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
                result = inverter.get(key)
                val = result.get("val", None)
                unit = result.get("unit", None)
                precision = result.get("precision", None)
                if isinstance(val, dict):
                    if calculate_total:
                        subtotal = val.get("site")
                        total += subtotal
                else:
                    if calculate_total:
                        total += val

                if unit:
                    composite["unit"] = unit
                if precision is not None:
                    composite["precision"] = precision
                composite[inverter.get("name")] = val

            if calculate_total:
                composite["site"] = total

            composite["topic"] = MQTT_TOPICS.get(key, key)
            sensors.append(composite)

        return sensors

    def cached_key(self, key):
        """Determines if a key in the inverter cache."""
        cached = key in self._cached_keys
        return cached

    def find_total_production(self, period):
        """."""
        for d_period in self._total_production:
            if d_period.get("period") is period:
                return d_period.copy()
        return None

    async def run(self):
        """Task to schedule actions at regular intervals."""
        SLEEP = 0.5
        last_tick = int(time.time())
        await self.read_instantaneous()
        await self.read_total_production()
        while True:
            tick = int(time.time())
            if tick != last_tick:
                last_tick = tick
                if tick % 5 == 0:
                    await self.read_instantaneous()

                    eff = await self.inverter_efficiency()
                    snapshot = await self.snapshot()
                    
                    self._influx.write_points(eff)
                    mqtt.publish(eff)
                    #pprint(snapshot)
                    self._influx.write_points(snapshot)
                    mqtt.publish(snapshot)
                    pass
                if tick % 15 == 0:
                    mqtt.publish(self.production_history())
                    pass
                if tick % 30 == 0:
                    mqtt.publish(self.co2_avoided())
                    pass
                if tick % 60 == 0:
                    pass
                if tick % 300 == 0:
                    pass
            await asyncio.sleep(SLEEP)
