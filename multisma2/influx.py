# Interface to InfluxDB multisma2 database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

import os
import time
import datetime
import logging
from config import config_from_yaml

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from readconfig import retrieve_options

from exceptions import FailedInitialization
from exceptions import InfluxDBWriteError, InfluxDBBucketError

from urllib3.exceptions import NewConnectionError


_LOGGER = logging.getLogger('multisma2')

_INFLUXDB2_OPTIONS = {
    'url': {'type': str, 'required': True},
    'token': {'type': str, 'required': True},
    'bucket': {'type': str, 'required': True},
    'org': {'type': str, 'required': True},
}

_DEBUG_ENV_VAR = 'MULTISMA2_DEBUG'
_DEBUG_OPTIONS = {
    'create_bucket': {'type': bool, 'required': False},
    'delete_bucket': {'type': bool, 'required': False},
}

LP_LOOKUP = {
    'ac_measurements/power': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'power', 'output': True},
    'ac_measurements/voltage': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'voltage', 'output': True},
    'ac_measurements/current': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'current', 'output': True},
    'ac_measurements/efficiency': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'efficiency', 'output': False},
    'dc_measurements/power': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'power', 'output': True},
    'dc_measurements/voltage': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'voltage', 'output': False},
    'dc_measurements/current': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'current', 'output': False},
    'status/reason_for_derating': {'measurement': 'status', 'tags': ['_inverter'], 'field': 'derating', 'output': True},
    'status/general_operating_status': {'measurement': 'status', 'tags': ['_inverter'], 'field': 'operating_status', 'output': True},
    'status/grid_relay': {'measurement': 'status', 'tags': ['_inverter'], 'field': 'grid_relay', 'output': True},
    'status/condition': {'measurement': 'status', 'tags': ['_inverter'], 'field': 'condition', 'output': True},
    'production/total_wh': {'measurement': 'production', 'tags': ['_inverter'], 'field': 'total_wh', 'output': True},
    'production/midnight': {'measurement': 'production', 'tags': ['_inverter'], 'field': 'midnight', 'output': True},
    'production/today': {'measurement': 'production', 'tags': ['_inverter'], 'field': 'today', 'output': True},
    'production/month': {'measurement': 'production', 'tags': ['_inverter'], 'field': 'month', 'output': True},
    'production/year': {'measurement': 'production', 'tags': ['_inverter'], 'field': 'year', 'output': True},
    'production/lifetime': {'measurement': 'production', 'tags': ['_inverter'], 'field': 'lifetime', 'output': False},
    'co2avoided/today': {'measurement': 'co2avoided', 'tags': ['_inverter'], 'field': 'today', 'output': False},
    'co2avoided/month': {'measurement': 'co2avoided', 'tags': ['_inverter'], 'field': 'month', 'output': False},
    'co2avoided/year': {'measurement': 'co2avoided', 'tags': ['_inverter'], 'field': 'year', 'output': False},
    'co2avoided/lifetime': {'measurement': 'co2avoided', 'tags': ['_inverter'], 'field': 'lifetime', 'output': False},
    'sun/position': {'measurement': 'sun', 'tags': None, 'field': None, 'output': True},
    'sun/irradiance': {'measurement': 'sun', 'tags': ['_type'], 'field': 'irradiance', 'output': True},
}


class InfluxDB:
    def __init__(self, config):
        self._config = config
        self._client = None
        self._write_api = None
        self._query_api = None
        self._delete_api = None
        self._tasks_api = None
        self._organizations_api = None
        self._token = None
        self._org = None
        self._url = None
        self._bucket = None

    def start(self) -> bool:
        """Initialize the InfluxDB client."""
        try:
            influxdb_options = retrieve_options(self._config, 'influxdb2', _INFLUXDB2_OPTIONS)
            debug_options = retrieve_options(self._config, 'debug', _DEBUG_OPTIONS)
        except FailedInitialization as e:
            _LOGGER.error(f"{e}")
            return False

        if not influxdb_options.get('enable', None):
            _LOGGER.warning("InfluxDB support is disabled in the YAML configuration file")
            return True

        result = False
        try:
            self._bucket = influxdb_options.get('bucket', None)
            self._url = influxdb_options.get('url', None)
            self._token = influxdb_options.get('token', None)
            self._org = influxdb_options.get('org', None)
            self._client = InfluxDBClient(url=self._url, token=self._token, org=self._org, enable_gzip=True)
            if not self._client:
                raise FailedInitialization(
                    f"failed to get InfluxDBClient from '{self._url}' (check url, token, and/or organization)")
            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
            self._query_api = self._client.query_api()
            self._delete_api = self._client.delete_api()
            self._tasks_api = self._client.tasks_api()
            self._organizations_api = self._client.organizations_api()

            multisma2_debug = os.getenv(_DEBUG_ENV_VAR, 'False').lower() in ('true', '1', 't')
            try:
                if multisma2_debug and debug_options.get('delete_bucket', False):
                    self.delete_bucket()
                    _LOGGER.info(f"Deleted bucket '{self._bucket}' at '{self._url}'")
            except InfluxDBBucketError as e:
                raise FailedInitialization(f"{e}")

            try:
                if not self.connect_bucket(multisma2_debug and debug_options.get('create_bucket', False)):
                    raise FailedInitialization(f"Unable to access (or create) bucket '{self._bucket}' at '{self._url}'")
            except InfluxDBBucketError as e:
                raise FailedInitialization(f"{e}")

            _LOGGER.info(f"Connected to InfluxDB: '{self._url}', bucket '{self._bucket}'")
            result = True

        except FailedInitialization as e:
            _LOGGER.error(f"{e}")
        except NewConnectionError:
            _LOGGER.error(f"InfluxDB client unable to connect to host at {self._url}")
        except ApiException as e:
            _LOGGER.error(f"InfluxDB client unable to access bucket '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception: {e}")
        finally:
            self._client = None
            return result

    def stop(self):
        if self._write_api:
            self._write_api.close()
            self._write_api = None
        if self._client:
            self._client.close()
            self._client = None

    def bucket(self):
        return self._bucket

    def org(self):
        return self._org

    def write_api(self):
        return self._write_api

    def query_api(self):
        return self._query_api

    def delete_api(self):
        return self._delete_api

    def tasks_api(self):
        return self._tasks_api

    def organizations_api(self):
        return self._organizations_api

    def write_points(self, points):
        if not self._write_api:
            return False
        try:
            self._write_api.write(bucket=self._bucket, record=points, write_precision=WritePrecision.S)
            return True
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_points(): {e}")

    def write_history(self, site, topic):
        if not self._write_api:
            return False

        lookup = LP_LOOKUP.get(topic, None)
        if not lookup:
            _LOGGER.error(f"write_history(): unknown topic '{topic}'")
            return False

        measurement = lookup.get('measurement')
        tags = lookup.get('tags', None)
        field = lookup.get('field', None)
        lps = []
        for inverter in site:
            inverter_name = inverter.pop(0)
            name = inverter_name.get('inverter', 'sunnyboy')
            for history in inverter:
                t = history['t']
                v = history['v']
                if v is None:
                    continue
                lp = f"{measurement}"
                if tags and len(tags):
                    lp += f",{tags[0]}={name}"
                if isinstance(v, int):
                    lp += f" {field}={v}i {t}"
                    lps.append(lp)
                else:
                    _LOGGER.error(
                        f"write_history(): unanticipated type '{type(v)}' in measurement '{measurement}/{field}'")
                    continue

        try:
            self._write_api.write(bucket=self._bucket, record=lps, write_precision=WritePrecision.S)
            _LOGGER.debug(f"write_history({site}, {topic}): {lps}")
            return True
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_history(): {e}")

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
                    _LOGGER.error(f"write_sma_sensors(): unknown topic '{topic}'")
                    continue

                if not lookup.get('output', False):
                    continue

                if topic == 'production/today':
                    day = datetime.datetime.fromtimestamp(ts).date()
                    dt = datetime.datetime.combine(day, datetime.time(0, 0))
                    ts = int(dt.timestamp())
                elif topic == 'production/month':
                    month = datetime.date.fromtimestamp(ts).replace(day=1)
                    dt = datetime.datetime.combine(month, datetime.time(0, 0))
                    ts = int(dt.timestamp())
                elif topic == 'production/year':
                    year = datetime.date.fromtimestamp(ts).replace(month=1, day=1)
                    dt = datetime.datetime.combine(year, datetime.time(0, 0))
                    ts = int(dt.timestamp())

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
                                _LOGGER.error(
                                    f"write_sma_sensors(): unanticipated dictionary type '{type(v1)}' in measurement '{measurement}/{field}'")
                    else:
                        _LOGGER.error(
                            f"write_sma_sensors(): unanticipated type '{type(v)}' in measurement '{measurement}/{field}'")
                        continue

        try:
            self._write_api.write(bucket=self._bucket, record=lps, write_precision=WritePrecision.S)
            return True
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_sma_sensors(): {e}")

    def delete_bucket(self):
        if not self._client:
            return False
        try:
            buckets_api = self._client.buckets_api()
            found_bucket = buckets_api.find_bucket_by_name(self._bucket)
            if found_bucket:
                buckets_api.delete_bucket(found_bucket)
                bucket = buckets_api.find_bucket_by_name(self._bucket)
                if not bucket:
                    return True
            return False
        except ApiException as e:
            raise InfluxDBBucketError(
                f"InfluxDB client unable to delete bucket '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBBucketError(f"Unexpected exception in delete_bucket(): {e}")

    def connect_bucket(self, create_bucket=False):
        if not self._client:
            return False
        try:
            buckets_api = self._client.buckets_api()
            bucket = buckets_api.find_bucket_by_name(self._bucket)
            if bucket:
                return True
            if create_bucket:
                bucket = buckets_api.create_bucket(
                    bucket_name=self._bucket, org_id=self._org, retention_rules=None, org=None)
                if bucket:
                    _LOGGER.info(f"Created bucket '{self._bucket}' at {self._url}")
                    return True
            return False
        except ApiException as e:
            raise InfluxDBBucketError(
                f"InfluxDB client unable to create bucket '{self._bucket}' at {self._url}: {e.reason}")
        except NewConnectionError:
            raise
        except Exception as e:
            raise InfluxDBBucketError(f"Unexpected exception in connect_bucket(): {e}")


#
# Debug code to test instandalone mode
#

testdata = [
    {'sb51': 0, 'sb71': 0, 'sb72': 0, 'site': 0, 'topic': 'ac_measurements/power'},
    {'sb51': {'a': 0, 'b': 0, 'c': 0, 'sb51': 0}, 'sb71': {'a': 0, 'b': 0, 'c': 0, 'sb71': 0},
        'sb72': {'a': 0, 'b': 0, 'c': 0, 'sb72': 0}, 'site': 0, 'topic': 'dc_measurements/power'},
    {'sb51': {'a': 10.0, 'b': 20.0, 'c': 30.0}, 'sb71': {'a': 40.0, 'b': 50.0, 'c': 60.0},
        'sb72': {'a': 70.0, 'b': 80.0, 'c': 90.0}, 'topic': 'dc_measurements/voltage'},
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
