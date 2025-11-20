FROM python:3.9-slim-bullseye
LABEL maintainer="lakehouse"

ENV DEBIAN_FRONTEND=noninteractive
ENV TERM linux

# Airflow Envs
ARG AIRFLOW_VERSION=2.6.1
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# Cài đặt các gói hệ thống
RUN set -ex \
    && buildDeps="freetds-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev libpq-dev git build-essential default-libmysqlclient-dev" \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin apt-utils curl rsync netcat locales libsasl2-modules openjdk-11-jdk libmariadb3 \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

COPY requirements.txt /requirements.txt
RUN set -ex \
    && pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir "google-re2==1.1.20240701" \
    && pip install --no-cache-dir -r /requirements.txt \
    && apt-get purge --auto-remove -yqq freetds-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev libpq-dev git build-essential default-libmysqlclient-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_USER_HOME}
EXPOSE 8088 5555 8793
USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]

USER root
RUN apt-get update && apt-get install -y docker.io && \
    usermod -aG docker airflow && \
    chmod 666 /var/run/docker.sock || true
USER airflow