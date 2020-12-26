# multisma2
Improved Python application for real-time monitoring multiple SMA Sunny Boy inverters.

Now features a wider range of outputs, basically anything you see in your browser when connected to an SMA inverter with WebConnect can be pulled and sent as an MQTT message to your favorite home automation application or database.  Current outputs include

- current production (AC and DC values)
- total production (day, month, year, and lifetime)
- inverter status
- co2 avoided
- last 30 days production
- last 12 months production
- daily .CSV file with production and inverter status 
- any SMA sensors or setting for which you know the 'key'

## Rationale for multisma2
multisma2 is driven by my desire to see what is happening in my ground mount solar array which uses three Sunny Boy inverters tied to eight strings of nine panels each (total 24.84 kWp).  SMA offers Sunny Portal which is a non-real time window of the AC production.  It also uses an unknown and less than robust averaging algorithm which guarantees that I never see my peak production.  There is more data available if you log into each inverter with a browser but that quickly gets tired with three inverters to check, multisma2 fixes this by working with one or many inverters and combines the data intelligently for easy display or storage.

I wanted a real-time dashboard in Home Assistant that displays both the site totals and the individual inverters so multisma2 is the result, building on the pysma project to log into each inverter and pull **ALL** the data in the *Instantaneous values* menu every 5 seconds from each inverter.  This is cached and you display selected outputs at various intervals (5s, 15s, 30s, and 60s) depending on your needs.  For example, I report the AC production, DC production (by string), and inverter status every five seconds.  Slower changing outputs such as total production occurs every 30 seconds, and reporting of the CO2 avoided occurs every 60 seconds.

multisma2 is pretty complete for my purposes but there could be small improvements and bug fixes. Of course comments and feedback are welcome or you have a question on Sunny Boy inverters (at least the ones I have access to) feel free to ask.

## Using multisma2
A lot of this is new to me (a few months ago I had never seen Python) but hopefully it is pretty simple to setup multisma2 to connect to your SMA inverters and MQTT broker. 
### Requirements
- Python 3.7 or later
- Python packages
    - paho-mqtt
    - aiohttp
    - astral
    - python-dateutil
    - jmespath
- SMA Sunny Boy inverter(s) supporting WebConnect
- Docker or cron skills

### Installation
1.  First up is to clone this repository:

    `git clone https://github.com/sillygoose/multisma2.git`

2.  Copy `configuration.edit` to `configuration.py`

3.  Edit `configuration.py` to match your PV site, you will need the IP addresses for each inverter and the login credentials.  If you are using MQTT then you need the IP address of your MQTT broker and the optional login credentials.

    There are some other fields to configure for the log files, time zone, site location, etc, these should be easy to figure out.

4.  Test that multisma2 connects to your inverters and MQTT broker:

    `python3 multisma2`

5.  Docker setup
Once you have a working `configuration.py` file you can build a Docker container that is setup to run multisma2 once a day using a cron table entry:

    `docker build -t multisma2 .`

### Some Interesting Facts
It maybe helpful to understand these quirks about multisma2:

1.  multisma2 runs until just before midnight and then exits

2.  If you want simulate daylight for testing you can use the FORCE_DAYLIGHT environment variable:
    ```
    export FORCE_DAYLIGHT=0     # force nighttime
    export FORCE_DAYLIGHT=1     # force daylight
    unset FORCE_DAYLIGHT        # normal operation
    ```        
2.  cron is used to run a new copy of multisma2 after midnight, this allows for new timestamped log files to be created (multisma2 does not attempt to manage multiple log files, app logs are overwritten and production logs are appended to).
3.  multisma2 runs at full speed during daylight hours, which for now is defined from dawn to dusk.  At night it slows down outputs when they are zero but still reports to keep a display (Home Assistant in my case) updated.

| Interval | Outputs |
| --- | --------- |
| 5s | AC production, DC production, inverter status |
| 15s | Total production (today, month, year, lifetime) |
| 30s | CO2 avoided |
| 60s | Daily and monthly history, logging to production log (if enabled) |

At night these updates based on the settings in `pvsite.py`: 
```
      SAMPLE_PERIOD = [
        {"scale": 10},  # night
        {"scale": 1},   # day
    ]
```

