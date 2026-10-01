# Runs the generator inside the compose network, on purpose.
#
# Redpanda advertises its listener as `redpanda:9092`. A client on the host can
# open a TCP connection to localhost:9092 but then reads metadata pointing at
# `redpanda:9092`, a name that does not resolve outside the network -- so
# kafka-python fails with a 60s metadata timeout. Running the producer in here
# sidesteps the whole problem and is closer to how the pipeline actually runs.
FROM python:3.11-slim

WORKDIR /opt/app

RUN pip install --no-cache-dir kafka-python==2.0.2

COPY generator /opt/app/generator

ENV PYTHONUNBUFFERED=1 \
    KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    KAFKA_TOPIC=events

ENTRYPOINT ["python", "-m", "generator"]
CMD ["--help"]