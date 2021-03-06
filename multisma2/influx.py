# Interface to InfluxDB multisma2 database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

import time
import os
import logging
from config import config_from_yaml
from pprint import pprint

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


logger = logging.getLogger('multisma2')

LP_LOOKUP = {
    'ac_measurements/power': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'power'},
    'ac_measurements/voltage': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'voltage'},
    'ac_measurements/current': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'current'},
    'ac_measurements/efficiency': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'efficiency'},
    'dc_measurements/power': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'power'},
    'dc_measurements/voltage': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'voltage'},
    'dc_measurements/current': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'current'},
    'status/reason_for_derating': {'measurement': 'status', 'tags': ['_inverter'], 'field': 'derating'},
    'status/general_operating_status': {'measurement': 'status', 'tags': ['_inverter'], 'field': 'operating_status'},
    'status/grid_relay': {'measurement': 'status', 'tags': ['_inverter'], 'field': 'grid_relay'},
    'status/condition': {'measurement': 'status', 'tags': ['_inverter'], 'field': 'condition'},
    'production/total_wh': {'measurement': 'production', 'tags': ['_inverter'], 'field': 'total_wh'},
    'production/midnight': {'measurement': 'production', 'tags': ['_inverter'], 'field': 'midnight'},
    'sun/position': {'measurement': 'sun', 'tags': None, 'field': None},
    'sun/irradiance': {'measurement': 'sun', 'tags': ['_type'], 'field': 'irradiance'},
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
        tags = lookup.get('tags', None)
        field = lookup.get('field', None)
        lps = []
        for inverter in site:
            inverter_name = inverter.pop(0)
            name = inverter_name['inverter']
            for history in inverter:
                t = history['t']
                v = history['v']
                if v is None:
                    continue
                lp = f'{measurement}'
                if tags and len(tags):
                    lp += f',{tags[0]}={name}'
                if isinstance(v, int):
                    lp += f' {field}={v}i {t}'
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
                tags = lookup.get('tags', None)
                for k, v in point.items():
                    field = lookup.get('field')
                    # sample: dc_measurements
                    lp = f'{measurement}'
                    if tags and len(tags):
                        # sample: dc_measurements,_inverter=sb71
                        lp += f',{tags[0]}={k}'
                    if not field:
                        field = k
                    if isinstance(v, int):
                        # sample: ac_measurements,_inverter=sb71 power=0.23 1556813561098
                        lp += f' {field}={v}i {ts}'
                        lps.append(lp)
                    elif isinstance(v, float):
                        # sample: ac_measurements,_inverter=sb71 power=0.23 1556813561098
                        lp += f' {field}={v} {ts}'
                        lps.append(lp)
                    elif isinstance(v, dict):
                        lp_prefix = f'{lp}'
                        for k1, v1 in v.items():
                            # sample: dc_measurements,_inverter=sb71
                            lp = f'{lp_prefix}'
                            if tags and len(tags) > 1:
                                # sample: dc_measurements,_inverter=sb71,_string=a
                                lp += f',{tags[1]}={k1}'
                            if isinstance(v1, int):
                                # sample: dc_measurements,_inverter=sb71,_string=a power=1000 1556813561098
                                lp += f' {field}={v1}i {ts}'
                                lps.append(lp)
                            elif isinstance(v1, float):
                                # sample: dc_measurements,_inverter=sb71,_string=a current=0.23 1556813561098
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


#
# Debug code to test instandalone mode
#

testdata = [
    {'sb51': 0, 'sb71': 0, 'sb72': 0, 'site': 0, 'topic': 'ac_measurements/power'},
    {'sb51': {'a': 0, 'b': 0, 'c': 0, 'sb51': 0}, 'sb71': {'a': 0, 'b': 0, 'c': 0, 'sb71': 0}, 'sb72': {'a': 0, 'b': 0, 'c': 0, 'sb72': 0}, 'site': 0, 'topic': 'dc_measurements/power'},
    {'sb51': {'a': 10.0, 'b': 20.0, 'c': 30.0}, 'sb71': {'a': 40.0, 'b': 50.0, 'c': 60.0}, 'sb72': {'a': 70.0, 'b': 80.0, 'c': 90.0}, 'topic': 'dc_measurements/voltage'},
    {'sb51': 16777213, 'sb71': 16777213, 'sb72': 16777213, 'topic': 'status/general_operating_status'}
]

if __name__ == "__main__":
    yaml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'multisma2.yaml')
    config = config_from_yaml(data=yaml_file, read_from_file=True)
    influxdb = InfluxDB()
    result = influxdb.start(config=config.multisma2.influxdb2)
    if not result:
        print("Something failed during initialization")
    else:
        influxdb.write_sma_sensors(testdata)
        influxdb.stop()
        print("Done")
