FROM python:3.14-rc-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

RUN mkdir -p /data

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV DATA_DIR=/data
