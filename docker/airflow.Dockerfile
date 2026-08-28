FROM apache/airflow:2.10.4-python3.11

# PySpark needs a JDK on the image — installed via apt (Debian's own
# mirrors), not a tarball pulled from a third-party host, per the Docker
# build network-call gotcha in CLAUDE.md.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# This dev machine's Avast HTTPS-scanning MITMs pip's connection to
# pypi.org from *inside* any container's network path (apt to Debian's
# mirrors is unaffected — only pip hits this) with a cert signed by
# Avast's own locally-generated root CA, which isn't in the container's
# trust store. docker/ca-certs/ is that CA exported from this machine's
# Windows cert store (see CLAUDE.md "Docker build network calls" for the
# export command) — installing it via update-ca-certificates makes pip's
# TLS verification pass *for real*, against the CA actually terminating
# the connection, same category of fix as this repo family's
# `truststore.inject_into_ssl()` host-side pattern, not a bypass. The
# directory is empty (just .gitkeep) on any machine without this problem,
# so update-ca-certificates is a no-op there and the build still succeeds
# unmodified.
COPY docker/ca-certs/ /usr/local/share/ca-certificates/extra/
RUN update-ca-certificates
ENV PIP_CERT=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

USER airflow
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# Makes `from extract...` / `from transform...` / `from query...` /
# `from tableau...` importable from DAG code without installing this
# project as a package.
ENV PYTHONPATH=/opt/airflow/project

# The base image's own WORKDIR is /opt/airflow, not the bind-mounted
# project dir (docker-compose.yml's `.:/opt/airflow/project` volume) — a
# relative path (this project's `data/`, `tableau/*.hyper` defaults, meant
# to resolve against the repo root the way they do for host-venv runs)
# would otherwise land in the container's ephemeral filesystem and vanish
# on the next `docker compose run --rm`, never reaching the host. Airflow
# itself doesn't care about cwd (its own paths come from AIRFLOW_HOME),
# so this is safe to change.
WORKDIR /opt/airflow/project
