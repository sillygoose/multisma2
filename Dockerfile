FROM python:3.9.1-buster

# tzdata setup
ENV TZ America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# for multisma2
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y git
RUN apt-get install -y nano
RUN apt-get install -y cron
RUN apt-get install -y tzdata
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

# install other packages
RUN pip3 install aiohttp astral python-dateutil
RUN pip3 install paho-mqtt jmespath influxdb

# clone the repo into the docker container
WORKDIR /multisma2
RUN git clone https://github.com/sillygoose/multisma2.git

# add the site-specific configuration file
WORKDIR /multisma2/multisma2
ADD configuration.py .

# directory to start from
WORKDIR /multisma2

# run the multisma2 python3 app
CMD ["python3","multisma2/multism2.py"]
