FROM ubuntu

RUN apt update -y
RUN apt upgrade -y
RUN apt install -y python3 python3-pip

COPY requirements.txt app/
RUN python3 -m pip install -r app/requirements.txt

COPY src app/