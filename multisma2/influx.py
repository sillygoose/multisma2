# Interface to InfluxDB multisma2 database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

import time
import logging
# from pprint import pprint

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


logger = logging.getLogger('multisma2')

CACHE_ENABLED = False

LP_LOOKUP = {
    'ac_measurements/power': {'measurement': 'ac_measurements', 'tag': '_inverter', 'field': 'power'},
    'ac_measurements/voltage': {'measurement': 'ac_measurements', 'tag': '_inverter', 'field': 'voltage'},
    'ac_measurements/current': {'measurement': 'ac_measurements', 'tag': '_inverter', 'field': 'current'},
    'ac_measurements/efficiency': {'measurement': 'ac_measurements', 'tag': '_inverter', 'field': 'efficiency'},
    'dc_measurements/power': {'measurement': 'dc_measurements', 'tag': '_inverter', 'field': 'power'},
    'status/reason_for_derating': {'measurement': 'status', 'tag': '_inverter', 'field': 'derating'},
    'status/general_operating_status': {'measurement': 'status', 'tag': '_inverter', 'field': 'operating_status'},
    'status/grid_relay': {'measurement': 'status', 'tag': '_inverter', 'field': 'grid_relay'},
    'status/condition': {'measurement': 'status', 'tag': '_inverter', 'field': 'condition'},
    'production/total_wh': {'measurement': 'production', 'tag': '_inverter', 'field': 'total_wh'},
    'production/midnight': {'measurement': 'production', 'tag': '_inverter', 'field': 'midnight'},
    'sun/position': {'measurement': 'sun', 'tag': None, 'field': None},
    'sun/irradiance': {'measurement': 'sun', 'tag': None, 'field': None},
}


class InfluxDB():
    def __init__(self):
        self._client = None
        self._write_api = None
        self._enabled = False

    def __del__(self):
        if self._client:
            self._client.close()

    def start(self, config):
        if not config.multisma2.influxdb2.enable:
            return True
        self._bucket = config.multisma2.influxdb2.bucket
        self._client = InfluxDBClient(url=config.multisma2.influxdb2.url, token=config.multisma2.influxdb2.token, org=config.multisma2.influxdb2.org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS) if self._client else None
        result = self._client if self._client else False
        if result:
            logger.info(f"Connected to the InfluxDB database '{self._bucket}' at {config.multisma2.influxdb2.url}")
        else:
            logger.error(f"Failed to open the InfluxDB database '{self._bucket}' at {config.multisma2.influxdb2.url}")
        return result

    def stop(self):
        if self._write_api:
            self._write_api.close()
            self._write_api = None
        if self._client:
            bucket = self._bucket
            self._client.close()
            self._client = None
            logger.info(f"Closed the InfluxDB bucket '{bucket}'")

    cache = {}

    def write_points(self, points):
        if not self._write_api:
            return False
        try:
            self._write_api.write(bucket=self._bucket, record=points, write_precision=WritePrecision.S)
            result = True
        except Exception as e:
            logger.error(f"Database write() call failed in write_points(): {e}")
            result = False
        return result

    def write_history(self, site, topic):
        if not self._write_api:
            return False

        lookup = LP_LOOKUP.get(topic, None)
        if not lookup:
            logger.error(f"write_history(): unknown topic '{topic}'")
            return False

        measurement = lookup.get('measurement')
        tag = lookup.get('tag')
        field = lookup.get('field')
        lps = []
        for inverter in site:
            inverter_name = inverter.pop(0)
            name = inverter_name['inverter']
            for history in inverter:
                t = history['t']
                v = history['v']
                if v is None:
                    continue
                if isinstance(v, int):
                    lp = f'{measurement},{tag}={name} {field}={v}i {t}'
                    lps.append(lp)
                else:
                    logger.error(f"write_history(): unanticipated type '{type(v)}' in measurement '{measurement}/{field}'")
                    continue

        try:
            self._write_api.write(bucket=self._bucket, record=lps, write_precision=WritePrecision.S)
            result = True
        except Exception as e:
            logger.error(f"Database write() call failed in write_history(): {e}")
            result = False
        return result

    def write_sma_sensors(self, sensor, timestamp=None):
        if not self._client:
            return False

        ts = timestamp if timestamp is not None else int(time.time())
        lps = []
        for old_point in sensor:
            point = old_point.copy()
            topic = point.pop('topic', None)
            point.pop('precision', None)
            if topic:
                lookup = LP_LOOKUP.get(topic, None)
                if not lookup:
                    logger.error(f"write_sma_sensors(): unknown topic '{topic}'")
                    continue

                measurement = lookup.get('measurement')
                tag = lookup.get('tag')
                field = lookup.get('field')
                for k, v in point.items():
                    signature = f'{measurement}_{k}_{field}'
                    lp = f'{measurement}'
                    if tag:
                        lp += f',{tag}={k}'
                    lp += ' '
                    if not field:
                        field = k
                    if isinstance(v, int):
                        lp += f'{field}={v}i'
                    elif isinstance(v, float):
                        lp += f'{field}={v}'
                    elif isinstance(v, dict):
                        first = True
                        for k1, v1 in v.items():
                            if not first:
                                lp += ','
                            if isinstance(v1, int):
                                lp += f'{k1}={v1}i' if k1 != k else f'{field}={v1}i'
                            else:
                                logger.error(f"write_sma_sensors(): unanticipated dictionary type '{type(v1)}' in measurement '{measurement}/{field}'")
                            first = False
                    else:
                        logger.error(f"write_sma_sensors(): unanticipated type '{type(v)}' in measurement '{measurement}/{field}'")
                        continue

                    # Check if in the cache, if not or different update cache and write
                    if CACHE_ENABLED:
                        cached_result = InfluxDB.cache.get(signature, None)
                        if cached_result:
                            if lp == cached_result:
                                continue

                    InfluxDB.cache[signature] = lp
                    lp += f' {ts}'
                    lps.append(lp)

        try:
            self._write_api.write(bucket=self._bucket, record=lps, write_precision=WritePrecision.S)
            result = True
        except Exception as e:
            logger.error(f"Database write() call failed in write_sma_sensors(): {e}")
            result = False
        return result
