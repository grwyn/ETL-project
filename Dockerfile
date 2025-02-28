FROM quay.io/astronomer/astro-runtime:12.7.1

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    build-essential \
    libffi-dev \
    libssl-dev \
    libblas-dev \
    liblapack-dev \
    libatlas-base-dev

RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir soda-core-bigquery &&\
    pip install --no-cache-dir soda-core-scientific && deactivate

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate