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

    def write(self, points):
        if not self._client:
            return
        #pprint(points)
        ts = time.time_ns()
        lps = []
        for point in points:
            precision = point.pop('precision', None)
            unit = point.pop('unit', None)
            topic = point.pop('topic')
            topic = topic.replace('/', '_')
            #pprint(point)
            #"airSensors,sensor_id=TLM0#{v[:id]} temperature=#{v[:t]},humidity=#{v[:h]},co=#{v[:c]}"
            #migration,id=91752A,s2_cell_id=164b35c lat=8.3495,lon=39.01233 1554123600000000000
            #migration,id=91752A,s2_cell_id=164b3dc lat=8.56067,lon=39.08883 1554102000000000000
            #migration,id=91752A,s2_cell_id=17b4854 lat=7.86233,lon=38.81167 1547557200000000000
            lp = f"{topic} "
            first = True
            for k, v in point.items():
                #print(f"{k}  {v}")
                if isinstance(v, dict):
                    for sk, sv in v.items():
                        #print(f"{sk}  {sv}")
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
            lp += f" {ts}"
            lps.append(lp)
        #pprint(lps)
        result = self._client.write_points(lps, protocol='line')
        #print(result)
        if not result:
            logger.error(f"Database write failed")
