FROM python:3.7

ADD requirements.txt .

ADD lib lib

ADD ingestion ingestion
ADD normalization normalization
ADD kpis kpis
ADD forecast forecast

ENV PYTHONPATH "${PYTHONPATH}:/:/lib"

RUN apt-get update && apt-get install -y \
    freetds-bin \
    freetds-common \
    freetds-dev

RUN pip install -r lib/python/requirements.txt
RUN pip install -r requirements.txt

