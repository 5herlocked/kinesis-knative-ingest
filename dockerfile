FROM python:3.7.13-buster

COPY requirements.txt app/
RUN python3 -m pip install -r app/requirements.txt

COPY src app/