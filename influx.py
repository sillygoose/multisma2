# Interface to InfluxDB multisma2 database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

import time
import logging
from pprint import pprint

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError, InfluxDBClientError

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)

CACHED_ENABLED = False

LP_LOOKUP = {
    'ac_measurements/power': {'measurement': 'ac_measurements', 'field': 'power'},
    'ac_measurements/voltage': {'measurement': 'ac_measurements', 'field': 'voltage'},
    'ac_measurements/current': {'measurement': 'ac_measurements', 'field': 'current'},
    'ac_measurements/efficiency': {'measurement': 'ac_measurements', 'field': 'efficiency'},
    'dc_measurements/power': {'measurement': 'dc_measurements', 'field': 'power'},
    'status/reason_for_derating': {'measurement': 'status', 'field': 'derating'},
    'status/general_operating_status': {'measurement': 'status', 'field': 'operating_status'},
    'status/grid_relay': {'measurement': 'status', 'field': 'grid_relay'},
    'status/condition': {'measurement': 'status', 'field': 'condition'},
    'production/total': {'measurement': 'production', 'field': 'total'},
    'production/daily_total': {'measurement': 'production', 'field': 'daily_total'},
}

# Translate the tag strings to integers
TAG_TRANSLATIONS = {
    'Information not available':    0,
    'Ok':                           1,
    'MPP':                          2,
    'not active':                   3,
    'Open':                         4,
    'Closed':                       5,
    'Start':                        6,
    'Warning':                      7,
}


class InfluxDB():
    def __init__(self, enabled):
        self._client = None
        self._enabled = enabled

    def __del__(self):
        if self._client:
            self._client.close()

    def start(self, host, port, database, username, password):
        if not self._enabled:
            return True
        self._client = InfluxDBClient(host=host, port=port, database=database, username=username, password=password)
        result = self._client if self._client else False
        logger.info(f"{'Opened' if result else 'Failed to open'} the InfluxDB database '{database}'")
        return result

    def stop(self):
        if self._client:
            self._client.close()
            self._client = None
            logger.info(f"Closed the InfluxDB database")

    cache = {}

    def write_history(self, site):
        if not self._client:
            return False

        lookup = LP_LOOKUP.get('production/daily_total')
        measurement = lookup.get('measurement')
        field = lookup.get('field')
        lps = []
        for inverter in site:
            inverter_name = inverter.pop(0)
            name = inverter_name['inverter']
            for history in inverter:
                t = history['t']
                v = history['v']
                if isinstance(v, int):
                    lp = f'{measurement},inverter={name} {field}={v} {t}'
                    lps.append(lp)

        try:
            result = self._client.write_points(points=lps, time_precision='s', protocol='line')
            logger.info(f"Wrote {len(lps)} history points")
        except (InfluxDBClientError, InfluxDBServerError):
            logger.error(f"Database write_history() failed")
            result = False
        return result

    def write_points(self, sensors):
        if not self._client:
            return False

        ts = int(time.time())
        lps = []
        for old_point in sensors:
            point = old_point.copy()
            topic = point.pop('topic', None)
            point.pop('unit', None)
            point.pop('precision', None)
            if topic:
                lookup = LP_LOOKUP.get(topic, None)
                if not lookup:
                    logger.error(f"Unknown topic '{topic}'")
                    continue

                measurement = lookup.get('measurement')
                for k, v in point.items():
                    lp = f'{measurement}'
                    signature = f'{measurement}_{k}_{lookup.get("field")}'
                    if isinstance(v, str):
                        t_v = TAG_TRANSLATIONS.get(v, -1)
                        if t_v == -1:
                            logger.warn(f"Unanticipated tag string: '{v}' in field '{lookup.get('field')}'")
                        lp += f',inverter={k} {lookup.get("field")}="{t_v}"'
                    elif isinstance(v, int) or isinstance(v, float):
                        lp += f',inverter={k} {lookup.get("field")}={v}'
                    elif isinstance(v, dict): 
                        lp += f',inverter={k} '
                        first = True
                        for k1, v1 in v.items():
                            if first:
                                first = False
                                lp += f'{lookup.get("field")}_{k1}={v1}'
                            else:
                                lp += f',{lookup.get("field")}_{k1}={v1}'

                    # Check if in the cache, if not or different update cache and write
                    if CACHED_ENABLED:
                        cached_result = InfluxDB.cache.get(signature, None)
                        if cached_result:
                            if lp == cached_result:
                                continue

                    InfluxDB.cache[signature] = lp
                    lp += f' {ts}'
                    lps.append(lp)

        try:
            result = self._client.write_points(points=lps, time_precision='s', protocol='line')
        except (InfluxDBClientError, InfluxDBServerError):
            logger.error(f"Database write_history() failed")
            result = False
        return result
