FROM python:3.12

WORKDIR /project

COPY pyproject.toml README.md /project/
COPY src/ /project/src/

RUN pip install -e ".[dagster]"
