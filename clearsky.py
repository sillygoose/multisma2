"""Module to estimate the clearsky irradiance for a site."""

import pandas as pd
from pvlib import location, irradiance

import logging
from pprint import pprint

from configuration import APPLICATION_LOG_LOGGER_NAME

logger = logging.getLogger(APPLICATION_LOG_LOGGER_NAME)


def site_location(latitude, longitude, tz):
    site = location.Location(latitude, longitude, tz)
    return site

def get_irradiance(site, start, end, tilt, azimuth, freq):
    """Calculate the clear-sky POA (plane of array) irradiance."""
    # Creates one day's worth of intervals
    times = pd.date_range(start=start, end=end, freq=freq, tz=site.tz)

    # Generate clearsky data using the Ineichen model, which is the default
    # The get_clearsky method returns a dataframe with values for GHI, DNI, and DHI
    clearsky = site.get_clearsky(times)
    
    # Get solar azimuth and zenith to pass to the transposition function
    solar_position = site.get_solarposition(times=times)
    
    # Use the get_total_irradiance function to transpose the GHI to POA
    POA_irradiance = irradiance.get_total_irradiance(
        surface_tilt=tilt,
        surface_azimuth=azimuth,
        dni=clearsky['dni'],
        ghi=clearsky['ghi'],
        dhi=clearsky['dhi'],
        solar_zenith=solar_position['apparent_zenith'],
        solar_azimuth=solar_position['azimuth'])

    # Return a DataFrame with only POA result and convert to a dictionary of time and values
    today_irradiance = pd.DataFrame({'POA': POA_irradiance['poa_global']})
    points = []
    for time, row in today_irradiance.iterrows():
        dt = pd.to_datetime(time)
        v = row['POA']
        points.append({'t': int(dt.timestamp()), 'v': v})

    # Return value: [{'t': timestamp, 'v': irradiance_value}]
    return points
