FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /code
COPY requirements.txt /code/
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc g++ libpq-dev  && pip install -r requirements.txt  && apt-get purge -y build-essential gcc g++ && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
COPY . /code/
