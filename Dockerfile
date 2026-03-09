FROM python:3.12

WORKDIR /project

COPY pyproject.toml README.md /project/
COPY src/airglow/ /project/airglow/

RUN pip install -e ".[dagster]"
