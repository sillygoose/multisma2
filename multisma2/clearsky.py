"""Module to estimate the clearsky irradiance for a given day and site."""
# Equations from G. Masters, “Renewable and Efficient Electric Power Systems,” Wiley-IEEE Press, 2004.
# Section 7.9 TOTAL CLEAR SKY INSOLATION ON A COLLECTING SURFACE

import logging
import datetime
from dateutil import tz
import math

import os
from config import config_from_yaml

from pysolar.solar import get_altitude, get_azimuth
from pysolar.radiation import get_radiation_direct


logger = logging.getLogger('sbhistory')


def current_global_irradiance(site_properties, solar_properties, timestamp):
    """Calculate the clear-sky POA (plane of array) irradiance for a specific time (seconds timestamp)."""
    dt = datetime.datetime.fromtimestamp(timestamp=timestamp, tz=tz.gettz(site_properties.tz))
    n = dt.timetuple().tm_yday

    sigma = math.radians(solar_properties.tilt)
    rho = solar_properties.get('rho', 0.0)

    C = 0.095 + 0.04 * math.sin(math.radians((n - 100) / 365))
    sin_sigma = math.sin(sigma)
    cos_sigma = math.cos(sigma)

    altitude = get_altitude(latitude_deg=site_properties.latitude, longitude_deg=site_properties.longitude, when=dt)
    beta = math.radians(altitude)
    sin_beta = math.sin(beta)
    cos_beta = math.cos(beta)

    azimuth = get_azimuth(latitude_deg=site_properties.latitude, longitude_deg=site_properties.longitude, when=dt)
    phi_s = math.radians(180 - azimuth)
    phi_c = math.radians(180 - solar_properties.azimuth)
    phi = phi_s - phi_c
    cos_phi = math.cos(phi)

    # Workaround for a quirk of pvsolar since the airmass for the sun ele===altitude of zero
    # is infinite and very small numbers close to zero result in NaNs being returned rather
    # than zero
    if altitude < 0.0:
        altitude = -1.0

    cos_theta = cos_beta * cos_phi * sin_sigma + sin_beta * cos_sigma
    ib = get_radiation_direct(when=dt, altitude_deg=altitude)
    ibc = ib * cos_theta
    idc = C * ib * (1 + cos_sigma) / 2
    irc = rho * ib * (sin_beta + C) * ((1 - cos_sigma) / 2)
    igc = ibc + idc + irc

    # If we still get a bad result just return 0
    if math.isnan(igc):
        igc = 0.0
    return igc


if __name__ == "__main__":
    yaml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'multisma2.yaml')
    config = config_from_yaml(data=yaml_file, read_from_file=True)
    site_properties = config.multisma2.site
    solar_properties = config.multisma2.solar_properties
    timestamp = 1614000000
    igc = current_global_irradiance(site_properties, solar_properties, timestamp)
    print(f"{datetime.datetime.fromtimestamp(timestamp)}   {igc:.0f}")
