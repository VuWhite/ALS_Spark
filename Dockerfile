FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY main.py /data/main.py

RUN chmod +x /data/main.py

USER 1001

CMD ["python", "/data/main.py"]
