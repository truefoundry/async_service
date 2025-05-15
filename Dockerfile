FROM registry.access.redhat.com/ubi9/ubi-minimal:9.5-1742914212
SHELL ["/bin/bash", "-c"]
USER root
# Install snappy for kafka-python
RUN curl -fSL "https://rpmfind.net/linux/centos-stream/9-stream/BaseOS/$(uname -m)/os/Packages/snappy-1.1.8-8.el9.$(uname -m).rpm" -o /tmp/snappy.rpm && \
    rpm -i /tmp/snappy.rpm && \
    rm -f /tmp/snappy.rpm
RUN microdnf install -y \
  unzip \
  tar \
  xz \
  unzip \
  git \
  jq \
  gzip \
  lz4 \
  zstd \
  ca-certificates \
  python3.11-devel python3.11-pip \
  && ln -s /usr/bin/python3.11 /usr/bin/python3 \
  && ln -s /usr/bin/python3 /usr/bin/python \
  && ln -s /usr/bin/pip3.11 /usr/bin/pip \
  && microdnf clean all \
  && rm -rf /var/cache/dnf
RUN python -m pip install -U pip setuptools wheel poetry
WORKDIR /code
COPY poetry.lock pyproject.toml /code/
RUN poetry config virtualenvs.create false \
  && poetry install --all-extras --only main --no-interaction --no-ansi --no-root
COPY . /code/
RUN poetry install --all-extras --only main --no-interaction --no-ansi
