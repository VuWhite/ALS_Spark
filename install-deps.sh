#!/bin/bash
# Install required Python dependencies for ALS Spark project

echo "Installing dependencies in spark-master..."
docker compose exec -T --user root spark-master pip3 install requests numpy pyspark

echo "Installing dependencies in spark-worker-1..."
docker compose exec -T --user root spark-worker-1 pip3 install requests numpy pyspark

echo "Installing dependencies in spark-worker-2..."
docker compose exec -T --user root spark-worker-2 pip3 install requests numpy pyspark

echo "Installing dependencies in spark-worker-3..."
docker compose exec -T --user root spark-worker-3 pip3 install requests numpy pyspark

echo "Installing dependencies in spark-worker-4..."
docker compose exec -T --user root spark-worker-4 pip3 install requests numpy pyspark

echo "Installing dependencies in spark-worker-5..."
docker compose exec -T --user root spark-worker-5 pip3 install requests numpy pyspark

echo "Done! All dependencies installed."
