"""Module to estimate the clearsky irradiance for a specific site."""

import os
import logging
import datetime
from dateutil import tz
import math

from pprint import pprint

from pysolar.solar import get_altitude, get_azimuth
# from pysolar.solar import radiation
# from pysolar.solar import *

from pysolar.radiation import get_radiation_direct
from astral import LocationInfo
from astral.sun import sun, elevation, azimuth

from config import config_from_yaml


logger = logging.getLogger('multisma2')

class Site():
    """Class to describe a PV site."""
    def __init__(self, site):
        """Create a new Site object."""
        self._name = site.name
        self._region = site.region
        self._latitude = site.latitude
        self._longitude = site.longitude
        self._elevation = site.elevation
        self._tz = tz.gettz(site.tz)
        self._siteinfo = LocationInfo(site.name, site.region, site.tz, site.latitude, site.longitude)

    def siteinfo(self):
        return self._siteinfo

    def latitude(self):
        return self._latitude

    def longitude(self):
        return self._longitude

    def tz(self):
        return self._tz


def global_irradiance(site, date):
    """Calculate the clear-sky POA (plane of array) irradiance."""
    irradiance = []

    astral = sun(observer=site.siteinfo().observer, date=date, tzinfo=site.siteinfo().tzinfo)
    dawn = astral['dawn']
    dusk = astral['dusk'] + datetime.timedelta(minutes=10)

    t = datetime.datetime(year=dawn.year, month=dawn.month, day=dawn.day, hour=dawn.hour, minute=int(int(dawn.minute/10)*10), tzinfo=site.tz())
    stop = datetime.datetime(year=dusk.year, month=dusk.month, day=dusk.day, hour=dusk.hour, minute=int(int(dusk.minute/10)*10), tzinfo=site.tz())
    print(f"Start is {t}, end is {stop}")
    while t < stop:
        elevation = get_altitude(site.latitude(), site.longitude(), t)
        clearsky = get_radiation_direct(t, elevation)
        irradiance.append({'t': int(t.timestamp()), 'v': clearsky})
        t += datetime.timedelta(minutes=10)

    return irradiance

if __name__ == "__main__":
    yaml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'multisma2.yaml')
    config = config_from_yaml(data=yaml_file, read_from_file=True)

    site = Site(config.multisma2.site)

    tzinfo = tz.gettz(config.multisma2.site.tz)
    #test_date = datetime.datetime(year=2021, month=6, day=21, hour=13, minute=8, tzinfo=tzinfo)
    #global_irradiance = global_irradiance(site=site, date=test_date)
    #elevation = get_altitude(site.latitude(), site.longitude(), test_date)
    #azimuth = get_azimuth(site.latitude(), site.longitude(), test_date)
    #print(f"elevation is {elevation}, azimuth is {azimuth}")
    #clearsky = get_radiation_direct(test_date, elevation)
    #print(f"estimated solar radiation is {clearsky}")

    tilt = config.multisma2.solar_properties.tilt
    sigma = math.radians(tilt)
    orientation = 180 - config.multisma2.solar_properties.azimuth
    phi_c = math.radians(orientation)

    daily_radiation = []
    date = datetime.date(year=2021, month=1, day=1)
    end_date = datetime.date(year=2022, month=1, day=1)
    while date < end_date:
        n = date.timetuple().tm_yday
        astral = sun(observer=site.siteinfo().observer, date=date, tzinfo=tzinfo)

        altitude = get_altitude(site.latitude(), site.longitude(), astral['noon'])
        beta = math.radians(altitude)
        #azimuth = math.radians(get_azimuth(site.latitude(), site.longitude(), astral['noon']))

        cos_theta = math.cos(beta) * math.cos(phi_c) * math.sin(sigma) + math.sin(beta) * math.cos(sigma)
        ib = get_radiation_direct(when=astral['noon'], altitude_deg=altitude)
        ibc = ib * cos_theta

        C = 0.095 + 0.04 * math.sin(math.radians((n - 100) / 365))
        idc = C * ib * (1 + math.cos(sigma) / 2)

        rho = 0.2
        irc = rho * ib * (math.sin(beta) + C) * ((1 - math.cos(sigma)) / 2)

        igc = ibc + idc + irc
        daily_radiation.append({'t': astral['noon'].date(), 'ib': ib, 'ibc': ibc, 'idc': idc, 'irc': irc, 'igc': igc})
        #print(f"dawn is at {astral['dawn']}, dusk is at {astral['dusk']}")
        #print(f"radiation at noon on {astral['noon'].date()} is {get_radiation_direct(astral['noon'], get_altitude(site.latitude(), site.longitude(), astral['noon'])):.0f} w/m2")
        date += datetime.timedelta(days=1)


    pass

#    solar_properties = config.multisma2.solar_properties
#    irradiance = get_irradiance(date=date, site=site, azimuth=solar_properties.azimuth, tilt=solar_properties.tilt)
#    pprint(irradiance)
