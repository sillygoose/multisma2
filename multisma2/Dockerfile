FROM alpine:latest

# tzdata setup
ENV TZ America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# for multisma2
RUN apt-get update
RUN apt-get install -y git nano tzdata
RUN apt-get install -y python3 python3-pip

# install required python packages
RUN pip3 install aiohttp asyncio astral python-dateutil python-configuration
RUN pip3 install paho-mqtt async_timeout jmespath influxdb-client

# clone the repo into the docker container
WORKDIR /sillygoose
RUN git clone https://github.com/sillygoose/multisma2.git

# add the site-specific configuration file
WORKDIR /sillygoose/multisma2
ADD multisma2.py .

# run multisma2
WORKDIR /sillygoose
CMD ["python3", "multisma2/multisma2.py"]
