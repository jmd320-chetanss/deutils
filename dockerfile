FROM python:3.13-bookworm

RUN apt update \
    && apt install --yes --no-install-recommends \
    git \
    vim

RUN pip install --no-cache-dir \
    pyspark \
    wordninja
