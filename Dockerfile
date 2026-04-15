# Base Python image # base image (abstract)

# Must be at the very top for --platform to work
ARG build_for=linux/amd64
ARG dbt_core_ref=dbt-core@v1.5.0
ARG dbt_bigquery_ref=dbt-bigquery@v1.5.0
ARG dbt_third_party

FROM --platform=$build_for python:3.11-slim-bullseye as base

# If you need to use build_for or dbt_core_ref inside the stages below, 
# you must re-declare them like this:
ARG dbt_core_ref
ARG dbt_bigquery_ref

# Where dbt will run from inside the container # Set docker basics
WORKDIR /usr/app/
ENTRYPOINT ["dbt"]

# Tell dbt where to find profiles.yml
ENV DBT_PROFILES_DIR=/usr/app

# System setup
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

# dbt-core
FROM base as dbt-core
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_core_ref}#egg=dbt-core&subdirectory=core"


# dbt-bigquery
FROM base as dbt-bigquery
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_bigquery_ref}#egg=dbt-bigquery"

# dbt-all
FROM base as dbt-all
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    python-dev \
    libsasl2-dev \
    gcc \
    unixodbc-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*
  RUN python -m pip install --no-cache "git+https://github.com/dbt-labs/${dbt_bigquery_ref}#egg=dbt-bigquery"