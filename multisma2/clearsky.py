"""Module to estimate the clearsky irradiance for a given day and site."""
# From Chapter 7 of 'Renewable and Efficient Electric Power Systems' by Masters

import logging
import datetime
from dateutil import tz
import math

from pysolar.solar import get_altitude, get_azimuth
from pysolar.radiation import get_radiation_direct


logger = logging.getLogger('sbhistory')


def global_irradiance(site, dawn, dusk, n, sigma, phi_c, rho):
    """Calculate the clear-sky POA (plane of array) irradiance."""
    irradiance = []
    tzinfo = tz.gettz(site.tz)

    C = 0.095 + 0.04 * math.sin(math.radians((n - 100) / 365))
    sin_sigma = math.sin(sigma)
    cos_sigma = math.cos(sigma)

    dusk += datetime.timedelta(minutes=10)
    t = datetime.datetime(year=dawn.year, month=dawn.month, day=dawn.day, hour=dawn.hour, minute=int(int(dawn.minute/10)*10), tzinfo=tzinfo)
    stop = datetime.datetime(year=dusk.year, month=dusk.month, day=dusk.day, hour=dusk.hour, minute=int(int(dusk.minute/10)*10), tzinfo=tzinfo)
    while t < stop:
        altitude = get_altitude(latitude_deg=site.latitude, longitude_deg=site.longitude, when=t)
        beta = math.radians(altitude)
        azimuth = math.radians(get_azimuth(site.latitude, site.longitude, t))
        phi_s = math.radians(azimuth)

        cos_theta = math.cos(beta) * math.cos(phi_s - phi_c) * sin_sigma + math.sin(beta) * cos_sigma
        ib = get_radiation_direct(when=t, altitude_deg=altitude)
        ibc = ib * cos_theta

        idc = C * ib * (1 + cos_sigma) / 2

        irc = rho * ib * (math.sin(beta) + C) * ((1 - cos_sigma) / 2)

        igc = ibc + idc + irc
        irradiance.append({'t': int(t.timestamp()), 'v': igc})
        t += datetime.timedelta(minutes=10)

    return irradiance
