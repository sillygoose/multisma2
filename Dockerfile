FROM ubuntu:latest

# tzdata setup
ENV TZ America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# for multisma2
RUN apt-get update
RUN apt-get install -y git nano tzdata
RUN apt-get install -y python3 python3-pip python3-scipy
RUN apt-get install -y pkg-config libhdf5-dev

# install required python packages
RUN pip3 install pvlib tables
RUN pip3 install aiohttp astral python-dateutil
RUN pip3 install paho-mqtt jmespath influxdb

# clone the repo into the docker container
WORKDIR /sillygoose
RUN git clone https://github.com/sillygoose/multisma2.git

# add the site-specific configuration file
WORKDIR /sillygoose/multisma2
ADD configuration.py .

# run the python code
WORKDIR /sillygoose
CMD ["python3", "multisma2/multisma2.py"]
