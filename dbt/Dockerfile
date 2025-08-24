FROM python:3.11-slim

RUN pip install dbt-core dbt-snowflake

COPY ./dbt_project /dbt

COPY ./profiles.yml /dbt/profiles.yml

WORKDIR /dbt

