# multisma2
Improved Python application for monitoring multiple SMA Sunnyboy inverters.

Now features a wider range of outputs, basically anything you see in your browser when connected to an inverter can be pulled and sent as an MQTT message to your favorite home automation or database.  Current outputs include

- current production (AC and DC values)
- total production (day, month, year, and lifetime)
- inverter status
- co2 avoided
- last 30 days production
- last 12 months production
- daily .CSV file with production and inverter status 

multisma2 is driven by my desire to see what is happening in my ground mount solar array which uses three Sunnyboy inverters tied to eight strings of nine panels each (total 24.84 kWp).  SMA offers Sunny Portal which is a non-real time window with the site statistics (but only if you want AC production).  It also has a less than robust averaging algorithm which guarantees that I never see my peak production which is why I have an option to log output to a .CSV file every minute during daylight hours.  There is more data available if you log into each inverter but that quickly gets tired with the lack of scaling, multisma2 fixes this by working with one or many inverters.

I wanted a real-time dashboard in Home Assistant that displays both the site totals and individual inverters so multisma2 is the result, building on the pysma project to log into each inverter and pull **ALL** the data in the *Instantaneous values* menu every 5 seconds from each inverter.  This is a great period for real-time updates to the AC and DC power output but too fast for slower moving data so you can choose to have the data transmitted at slower speeds based on your requirements.

Look for regular updates as I clean up the code and finalize the MQTT message formats.  Of ccomments and feedback are welcome or you have a question on Sunnyboy inverters (at least the ones I have access to).
