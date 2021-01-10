FROM debian:latest

# tzdata setup
ENV TZ America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# for pysma
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y git
RUN apt-get install -y nano
RUN apt-get install -y cron
RUN apt-get install -y tzdata
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

# add crontab file in the cron directory
#ADD crontab /etc/cron.d/multisma2-cron
#RUN chmod 0644 /etc/cron.d/multisma2-cron

# install other packages
RUN pip3 install aiohttp astral python-dateutil
RUN pip3 install paho-mqtt jmespath influxdb

# clone the repo into the docker container
WORKDIR /solar
RUN git clone https://github.com/sillygoose/multisma2.git
RUN git checkout rewtite

# add the site-specific configuration file
WORKDIR /solar/multisma2
ADD configuration.py .

# add the entrypoint script
#RUN echo "#!/bin/bash\necho \"Docker container has been started\"\ncron -f\n" > /entrypoint.sh
#RUN chmod +x /entrypoint.sh

# directory to start from
WORKDIR /solar

# run the cron command then a shell on container startup
#ENTRYPOINT ["/entrypoint.sh"]
CMD ["python3","multisma2/multism2.py"]
