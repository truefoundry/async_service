FROM registry.access.redhat.com/ubi9/ubi-minimal:9.4-1227.1726694542
SHELL ["/bin/bash", "-c"]
USER root
RUN microdnf install -y \
  unzip \
  tar \
  xz \
  unzip \
  git \
  jq \
  ca-certificates \
  python3.11-devel python3.11-pip \
  && ln -s /usr/bin/python3.11 /usr/bin/python3 \
  && ln -s /usr/bin/python3 /usr/bin/python \
  && ln -s /usr/bin/pip3.11 /usr/bin/pip
RUN python -m pip install -U pip setuptools wheel poetry
WORKDIR /code
COPY poetry.lock pyproject.toml /code/
RUN poetry config virtualenvs.create false \
  && poetry install --all-extras --only main --no-interaction --no-ansi --no-root
COPY . /code/
RUN poetry install --all-extras --only main --no-interaction --no-ansi
