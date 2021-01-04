##
import time
import logging
from pprint import pprint

from influxdb import InfluxDBClient

from configuration import APPLICATION_LOG_LOGGER_NAME
from configuration import INFLUXDB_ENABLE, INFLUXDB_DATABASE, INFLUXDB_IPADDR, INFLUXDB_PORT

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


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

    def write(self, points):
        if not self._client:
            return

        ts = time.time_ns()
        lps = []
        for point in points:
            point.pop('precision', None)
            point.pop('unit', None)
            topic = point.pop('topic')
            topic = topic.replace('/', '_')
            lp = f"{topic} "
            first = True
            for k, v in point.items():
                if isinstance(v, dict):
                    for sk, sv in v.items():
                        if first:
                            lp += f"{k}_{sk}={sv}i"
                            first = False
                        else:
                            lp += f",{k}_{sk}={sv}i"
                else:
                    prefix = ''
                    suffix = ''
                    if isinstance(v, str): 
                        prefix = '"'
                        suffix = '"'
                    elif isinstance(v, int):
                        suffix = 'i'
                    if first:
                        lp += f"{k}={prefix}{v}{suffix}"
                        first = False
                    else:
                        lp += f",{k}={prefix}{v}{suffix}"

            # Check if in cache, if not or different update cache and output
            cached_result = InfluxDB.cache.get(topic, None)
            if cached_result:
                cached_lp = InfluxDB.cache.get(topic)
                if lp == cached_lp:
                    continue

            InfluxDB.cache[topic] = lp
            lp += f" {ts}"
            lps.append(lp)

        if len(lps):
            #pprint(lps)
            result = self._client.write_points(point=lps, time_precision='s', protocol='line')
            if not result:
                logger.error(f"Database write_points() failed")
