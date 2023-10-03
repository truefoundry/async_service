FROM --platform=linux/amd64 python:3.9-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN python -m pip install -U pip setuptools wheel poetry
WORKDIR /code
COPY poetry.lock pyproject.toml /code/
RUN poetry config virtualenvs.create false \
  && poetry install --all-extras --only main --no-interaction --no-ansi --no-root
COPY . /code/
RUN poetry install --all-extras --only main --no-interaction --no-ansi
