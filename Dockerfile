# Ubuntu Hirsute gets Python 3.10.4
FROM ubuntu:jammy

# tzdata setup
ENV TZ America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# python and helpers
RUN apt-get update
RUN apt-get install -y git nano tzdata iputils-ping
RUN apt-get install -y python3 python3-pip

# clone the repo into the docker container
WORKDIR /sillygoose
RUN git clone https://github.com/sillygoose/multisma2.git

# install required python packages
WORKDIR /sillygoose/multisma2
RUN pip3 install -e .

# add the site-specific configuration/secrets file
WORKDIR /sillygoose/multisma2/multisma2
ADD multisma2_secrets.yaml .

# run multisma2
WORKDIR /sillygoose
CMD ["python3", "multisma2/multisma2/multisma2.py"]
