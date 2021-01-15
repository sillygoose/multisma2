FROM python:3.9.1-buster

# tzdata setup
ENV TZ America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# for pysma
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y git
RUN apt-get install -y nano
RUN apt-get install -y tzdata
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

# install other packages
RUN pip3 install aiohttp astral python-dateutil
RUN pip3 install paho-mqtt jmespath influxdb

# clone the repo into the docker container
WORKDIR /solar
RUN git clone https://github.com/sillygoose/multisma2.git

# add the site-specific configuration file
WORKDIR /solar/multisma2
ADD configuration.py .

# directory to start from
WORKDIR /solar

# run multisma2 python code
CMD ["python3", "multisma2/multisma2.py"]
