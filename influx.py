# Interface to InfluxDB multisma2 database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

import time
import logging
from pprint import pprint

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)

CACHE_ENABLED = False

LP_LOOKUP = {
    'ac_measurements/power': {'measurement': 'ac_measurements', 'tag': 'inverter', 'field': 'power'},
    'ac_measurements/voltage': {'measurement': 'ac_measurements', 'tag': 'inverter', 'field': 'voltage'},
    'ac_measurements/current': {'measurement': 'ac_measurements', 'tag': 'inverter', 'field': 'current'},
    'ac_measurements/efficiency': {'measurement': 'ac_measurements', 'tag': 'inverter', 'field': 'efficiency'},
    'dc_measurements/power': {'measurement': 'dc_measurements', 'tag': 'inverter', 'field': 'power'},
    'status/reason_for_derating': {'measurement': 'status', 'tag': 'inverter', 'field': 'derating'},
    'status/general_operating_status': {'measurement': 'status', 'tag': 'inverter', 'field': 'operating_status'},
    'status/grid_relay': {'measurement': 'status', 'tag': 'inverter', 'field': 'grid_relay'},
    'status/condition': {'measurement': 'status', 'tag': 'inverter', 'field': 'condition'},
    'production/total': {'measurement': 'production', 'tag': 'inverter', 'field': 'total'},
    'production/today': {'measurement': 'production', 'tag': 'inverter', 'field': 'today'},
    'sun/position': {'measurement': 'sun', 'tag': None, 'field': None},
}


class InfluxDB():
    def __init__(self, enabled):
        self._client = None
        self._write_api = None
        self._enabled = enabled

    def __del__(self):
        if self._client:
            self._client.close()

    def start(self, url, bucket, org, token):
        if not self._enabled:
            return True
        self._bucket = bucket
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS) if self._client else None
        result = self._client if self._client else False
        logger.info(f"{'Opened' if result else 'Failed to open'} the InfluxDB database '{self._bucket}' at {url}")
        return result

    def stop(self):
        if self._write_api:
            self._write_api.close()
            self._write_api = None
        if self._client:
            self._client.close()
            self._client = None
            logger.info(f"Closed the InfluxDB database")

    cache = {}

    def write_points(self, points):
        if not self._write_api:
            return False
        try:
            self._write_api.write(bucket=self._bucket, record=points, write_precision=WritePrecision.S)
            result = True
        except Exception as e:
            logger.error(f"Database write_points() call failed in write_points(): {e}")
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
        field = lookup.get('field')
        lps = []
        for inverter in site:
            inverter_name = inverter.pop(0)
            name = inverter_name['inverter']
            for history in inverter:
                t = history['t']
                v = history['v']
                if isinstance(v, int):
                    lp = f'{measurement},inverter={name} {field}={v}i {t}'
                    lps.append(lp)
                else:
                    logger.error(f"write_history(): unanticipated type '{type(v)}' in measurement '{measurement}/{field}'")
                    continue

        try:
            self._write_api.write(bucket=self._bucket, record=lps, write_precision=WritePrecision.S)
            result = True
        except Exception as e:
            logger.error(f"Database write_points() call failed in write_history(): {e}")
            result = False
        return result

    def write_sma_sensors(self, sensors):
        if not self._client:
            return False

        ts = int(time.time())
        lps = []
        for old_point in sensors:
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
                for k, v in point.items():
                    field = lookup.get('field')
                    signature = f'{measurement}_{k}_{field}'
                    lp = f'{measurement}'
                    if tag:
                        lp += f',{tag}={k}'
                    lp += f' '
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
                                lp += f','
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
            #pprint(lps)
            self._write_api.write(bucket=self._bucket, record=lps, write_precision=WritePrecision.S)
            result = True
        except Exception as e:
            logger.error(f"Database write_points() call failed in write_sma_sensors(): {e}")
            result = False
        return result

xxx = [
        'production,inverter=site irradiance=0.0 1613044800',
        'production,inverter=site irradiance=24.1 1613045400',
        'production,inverter=site irradiance=305.5 1613046000',
        'production,inverter=site irradiance=1044.8 1613046600',
        'production,inverter=site irradiance=2091.6 1613047200',
        'production,inverter=site irradiance=3268.4 1613047800',
        'production,inverter=site irradiance=4481.9 1613048400',
        'production,inverter=site irradiance=5690.4 1613049000',
        'production,inverter=site irradiance=6875.7 1613049600',
        'production,inverter=site irradiance=8029.1 1613050200',
        'production,inverter=site irradiance=9146.6 1613050800',
        'production,inverter=site irradiance=10225.6 1613051400',
        'production,inverter=site irradiance=11264.7 1613052000',
        'production,inverter=site irradiance=12262.3 1613052600',
        'production,inverter=site irradiance=13217.5 1613053200',
        'production,inverter=site irradiance=14128.9 1613053800',
        'production,inverter=site irradiance=14995.6 1613054400',
        'production,inverter=site irradiance=15816.4 1613055000',
        'production,inverter=site irradiance=16590.1 1613055600',
        'production,inverter=site irradiance=17315.8 1613056200',
        'production,inverter=site irradiance=17992.4 1613056800',
        'production,inverter=site irradiance=18618.8 1613057400',
        'production,inverter=site irradiance=19194.2 1613058000',
        'production,inverter=site irradiance=19717.7 1613058600',
        'production,inverter=site irradiance=20188.4 1613059200',
        'production,inverter=site irradiance=20605.6 1613059800',
        'production,inverter=site irradiance=20968.6 1613060400',
        'production,inverter=site irradiance=21276.9 1613061000',
        'production,inverter=site irradiance=21529.9 1613061600',
        'production,inverter=site irradiance=21727.3 1613062200',
        'production,inverter=site irradiance=21868.6 1613062800',
        'production,inverter=site irradiance=21953.8 1613063400',
        'production,inverter=site irradiance=21982.6 1613064000',
        'production,inverter=site irradiance=21954.9 1613064600',
        'production,inverter=site irradiance=21870.9 1613065200',
        'production,inverter=site irradiance=21730.7 1613065800',
        'production,inverter=site irradiance=21534.5 1613066400',
        'production,inverter=site irradiance=21282.7 1613067000',
        'production,inverter=site irradiance=20975.6 1613067600',
        'production,inverter=site irradiance=20613.7 1613068200',
        'production,inverter=site irradiance=20197.7 1613068800',
        'production,inverter=site irradiance=19728.2 1613069400',
        'production,inverter=site irradiance=19205.9 1613070000',
        'production,inverter=site irradiance=18631.8 1613070600',
        'production,inverter=site irradiance=18006.5 1613071200',
        'production,inverter=site irradiance=17331.2 1613071800',
        'production,inverter=site irradiance=16606.9 1613072400',
        'production,inverter=site irradiance=15834.4 1613073000',
        'production,inverter=site irradiance=15015.1 1613073600',
        'production,inverter=site irradiance=14149.9 1613074200',
        'production,inverter=site irradiance=13240.0 1613074800',
        'production,inverter=site irradiance=12286.5 1613075400',
        'production,inverter=site irradiance=11290.7 1613076000',
        'production,inverter=site irradiance=10253.6 1613076600',
        'production,inverter=site irradiance=9176.8 1613077200',
        'production,inverter=site irradiance=8061.8 1613077800',
        'production,inverter=site irradiance=6911.1 1613078400',
        'production,inverter=site irradiance=5728.9 1613079000',
        'production,inverter=site irradiance=4523.4 1613079600',
        'production,inverter=site irradiance=3312.6 1613080200',
        'production,inverter=site irradiance=2136.3 1613080800',
        'production,inverter=site irradiance=1084.4 1613081400',
        'production,inverter=site irradiance=329.2 1613082000',
        'production,inverter=site irradiance=28.7 1613082600',
        'production,inverter=site irradiance=0.0 1613085000',
    ]

if __name__ == "__main__":
    influxdb = InfluxDB(True)
    if influxdb.start(url='http://192.168.1.80:8088', bucket='test', org='Parker Lane', token='P1UzFa14Qm7oUAdPe3EzYr3jUd15703mzRfay1AmqW5zgOiWloLeIxhWZIF5u-3Jy5T85LdXPQdQimYFqlyddQ=='):
        influxdb.write_points(xxx)
        influxdb.stop()

