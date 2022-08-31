FROM python:3.7.13-alpine

COPY requirements.txt app/
RUN python3 -m pip install -r app/requirements.txt

COPY src app/