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
from configuration import INFLUXDB_ENABLE, INFLUXDB_DATABASE, INFLUXDB_IPADDR, INFLUXDB_PORT

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


LP_LOOKUP = {
    'ac_measurements/power': {'measurement': 'ac_measurements', 'field': 'power'},
    'ac_measurements/voltage': {'measurement': 'ac_measurements', 'field': 'voltage'},
    'ac_measurements/current': {'measurement': 'ac_measurements', 'field': 'current'},
    'dc_measurements/power': {'measurement': 'dc_measurements', 'field': 'power'},
    'status/reason_for_derating': {'measurement': 'status', 'field': 'derating'},
    'status/general_operating_status': {'measurement': 'status', 'field': 'operating_status'},
    'status/grid_relay': {'measurement': 'status', 'field': 'grid_relay'},
    'status/condition': {'measurement': 'status', 'field': 'condition'},
    'production/total': {'measurement': 'production', 'field': 'total'},
}

class InfluxDB():
    def __init__(self):
        self._client = None

    def start(self):
        if INFLUXDB_ENABLE:
            self._client = InfluxDBClient(host=INFLUXDB_IPADDR, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)
            if self._client:
                logger.info(f"Opened the InfluxDB database '{INFLUXDB_DATABASE}' for output")
            else:
                logger.error(f"Failed to open the InfluxDB database '{INFLUXDB_DATABASE}'")

    def stop(self):
        if self._client:
            self._client.close()
            logger.info(f"Closed the InfluxDB database '{INFLUXDB_DATABASE}'")

    cache = {}

    def write_history(self, site):
        lps = []
        for inverter in site:
            inverter_name = inverter.pop(0)
            name = inverter_name['inverter']
            for history in inverter:
                t = history['t']
                v = history['v']
                if isinstance(v, int):
                    lp = f'production,inverter={name} total={v} {t}'
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
        for point in sensors:
            topic = point.pop('topic', None)
            point.pop('unit', None)
            point.pop('precision', None)
            if topic:
                lookup = LP_LOOKUP.get(topic, None)
                if lookup:
                    measurement = lookup.get('measurement')
                    for k, v in point.items():
                        lp = f'{measurement}'
                        signature = f'{measurement}_{k}_{lookup.get("field")}'
                        if isinstance(v, str): 
                            lp += f',inverter={k} {lookup.get("field")}="{v}"'
                        elif isinstance(v, int) or isinstance(v, float):
                            if k == 'total':
                                k = 'site'
                            lp += f',inverter={k} {lookup.get("field")}={v}'
                        elif isinstance(v, dict): 
                            lp += f',inverter={k} '
                            first = True
                            for k1, v1 in v.items():
                                if k1 == 'total':
                                    k1 = 'inverter'
                                if first:
                                    first = False
                                    lp += f'{lookup.get("field")}_{k1}={v1}'
                                else:
                                    lp += f',{lookup.get("field")}_{k1}={v1}'

                        # Check if in the cache, if not or different update cache and write
                        cached_result = InfluxDB.cache.get(signature, None)
                        if cached_result:
                            if lp == cached_result:
                                continue

                        InfluxDB.cache[signature] = lp
                        lp += f' {ts}'
                        lps.append(lp)

        try:
            result = self._client.write_points(points=lps, time_precision='s', protocol='line')
            logger.info(f"Wrote {len(lps)} sensor points")
        except (InfluxDBClientError, InfluxDBServerError):
            logger.error(f"Database write_history() failed")
            result = False
        return result
