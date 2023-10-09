FROM apache/airflow:2.7.1-python3.11

USER root

RUN apt-get update && \
    apt-get -y --no-install-recommends install gcc g++ && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY providers.txt /providers.txt
RUN pip install --user -r /providers.txt

COPY requirements.txt /requirements-force.txt
RUN pip install --user -r /requirements-force.txt

COPY webserver_config.py /opt/airflow/webserver_config.py

ENTRYPOINT ["bash", "/entrypoint"]

EXPOSE 8080 5555 8793
