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

LP_LOOKUP = {
    'ac_measurements/power': {'measurement': 'ac_measurements', 'tag': '_inverter', 'field': 'power'},
    'ac_measurements/voltage': {'measurement': 'ac_measurements', 'tag': '_inverter', 'field': 'voltage'},
    'ac_measurements/current': {'measurement': 'ac_measurements', 'tag': '_inverter', 'field': 'current'},
    'ac_measurements/efficiency': {'measurement': 'ac_measurements', 'tag': '_inverter', 'field': 'efficiency'},
    'dc_measurements/power': {'measurement': 'dc_measurements', 'tag': '_inverter', 'field': 'power', 'subtag': '_string'},
    'dc_measurements/voltage': {'measurement': 'dc_measurements', 'tag': '_inverter', 'field': 'voltage', 'subtag': '_string'},
    'dc_measurements/current': {'measurement': 'dc_measurements', 'tag': '_inverter', 'field': 'current', 'subtag': '_string'},
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

    def __del__(self):
        if self._client:
            self._client.close()

    def check_config(self, config):
        """Check that the needed YAML options exist."""
        required_keys = ['url', 'token', 'bucket', 'org']
        for key in required_keys:
            if key not in config.keys():
                logger.error(f"Missing required 'influxdb2' option in YAML file: '{key}'")
                return False
        return True

    def start(self, config):
        key = 'enable'
        if key not in config.keys():
            logger.error(f"Missing required 'influxdb2' option in YAML file: '{key}'")
            return False

        if not isinstance(config.enable, bool):
            logger.error(f"The influxdb 'enable' option is not a boolean '{config.enable}'")
            return False

        if not config.enable:
            return True

        if self.check_config(config) is False:
            return False

        try:
            self._bucket = config.bucket
            self._client = InfluxDBClient(url=config.url, token=config.token, org=config.org)
            if not self._client:
                logger.error(f"Failed to get InfluxDBClient object from {config.url} (check your url, token, and/or organization)")
                return False

            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
            if not self._write_api:
                logger.error(f"Failed to get client write_api() object from {config.url}")
                return False

            # Small test query to confirm the bucket exists
            query_api = self._client.query_api()
            if not query_api:
                logger.error(f"Failed to get client query_api() object from {config.url}")
            try:
                query_api.query(f'from(bucket: "{self._bucket}") |> range(start: -1m)')
                logger.info(f"Connected to the InfluxDB database at {config.url}, bucket '{self._bucket}'")
                return True
            except Exception:
                logger.error(f"Unable to access bucket '{self._bucket}' at {config.url}")
                return False

        except Exception:
            logger.error(f"Unexpected exception, unable to access bucket '{self._bucket}' at {config.url}")
            return False

        return False

    def stop(self):
        if self._write_api:
            self._write_api.close()
            self._write_api = None
        if self._client:
            bucket = self._bucket
            self._client.close()
            self._client = None
            logger.info(f"Closed the InfluxDB bucket '{bucket}'")

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
                tag = lookup.get('tag', None)
                for k, v in point.items():
                    field = lookup.get('field')
                    lp = f'{measurement}'
                    if tag:
                        lp += f',{tag}={k}'
                    #lp += ' '
                    if not field:
                        field = k
                    if isinstance(v, int):
                        lp += f' {field}={v}i {ts}'
                        lps.append(lp)
                    elif isinstance(v, float):
                        lp += f' {field}={v} {ts}'
                        lps.append(lp)
                    elif isinstance(v, dict):
                        # dc_measurements,_inverter=sb71,_string=a current=0.23 1556813561098
                        lp_prefix = f'{lp}'
                        subtag = lookup.get('subtag', None)
                        for k1, v1 in v.items():
                            lp = f'{lp_prefix}'
                            if subtag:
                                lp += f',{subtag}={k1}'
                            if isinstance(v1, int):
                                lp += f' {field}={v1}i {ts}'
                                lps.append(lp)
                            elif isinstance(v1, float):
                                lp += f' {field}={v1} {ts}'
                                lps.append(lp)
                            else:
                                logger.error(f"write_sma_sensors(): unanticipated dictionary type '{type(v1)}' in measurement '{measurement}/{field}'")
                    else:
                        logger.error(f"write_sma_sensors(): unanticipated type '{type(v)}' in measurement '{measurement}/{field}'")
                        continue

        try:
            self._write_api.write(bucket=self._bucket, record=lps, write_precision=WritePrecision.S)
            result = True
        except Exception as e:
            logger.error(f"Database write() call failed in write_sma_sensors(): {e}")
            result = False
        return result
