#!/usr/bin/env python

"""multisma2 setup."""
from pathlib import Path
from setuptools import setup

VERSION = "0.9.1"
URL = "https://github.com/sillygoose/multisma2"

setup(
    name="multisma2",
    version=VERSION,
    description="SMA Sunny Boy inverter data collection utility",
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    url=URL,
    download_url="{}/tarball/{}".format(URL, VERSION),
    author="Rick Naro",
    author_email="sillygoose@me.com",
    license="MIT",
    setup_requires=[
        "flake8",
    ],
    install_requires=[
        "aiohttp",
        "asyncio",
        "async_timeout",
        "jmespath",
        "influxdb-client",
        "paho-mqtt",
        "astral",
        "python-dateutil",
        "python-configuration",
    ],
    zip_safe=True,
)
