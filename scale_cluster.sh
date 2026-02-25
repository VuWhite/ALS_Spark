#!/bin/bash

set -e

echo "=========================================="
echo "  Dynamic Cluster Scaling Script"
echo "  Scale from 1 -> 2 -> 5 nodes"
echo "=========================================="

start_1_node() {
    echo ""
    echo "=========================================="
    echo "STEP 1: Starting cluster with 1 worker"
    echo "=========================================="
    
    docker compose up -d spark-master spark-worker-1
    
    echo "Waiting for master..."
    sleep 10
    
    for i in {1..30}; do
        if curl -s http://localhost:8080 > /dev/null 2>&1; then
            echo "Master is ready!"
            break
        fi
        echo "Waiting... ($i/30)"
        sleep 2
    done
    
    echo ""
    echo "1-Node Cluster Status:"
    docker compose ps
    echo ""
    echo "Memory per executor: 1GB"
    echo "Total cluster memory: ~2GB (1 master + 1 worker)"
}

scale_to_2_nodes() {
    echo ""
    echo "=========================================="
    echo "STEP 2: Scaling to 2 workers"
    echo "=========================================="
    
    docker compose up -d spark-worker-2
    
    sleep 10
    
    echo ""
    echo "2-Node Cluster Status:"
    docker compose ps
    echo ""
    echo "Memory per executor: 1GB"
    echo "Total cluster memory: ~3GB (1 master + 2 workers)"
}

scale_to_5_nodes() {
    echo ""
    echo "=========================================="
    echo "STEP 3: Scaling to 5 workers"
    echo "=========================================="
    
    docker compose up -d spark-worker-3 spark-worker-4 spark-worker-5
    
    sleep 10
    
    echo ""
    echo "5-Node Cluster Status:"
    docker compose ps
    echo ""
    echo "Memory per executor: 1GB"
    echo "Total cluster memory: ~6GB (1 master + 5 workers)"
}

run_benchmark() {
    local num_nodes=$1
    local partitions=$2
    
    echo ""
    echo "=========================================="
    echo "Running ALS with $num_nodes node(s)"
    echo "Partitions: $partitions"
    echo "=========================================="
    
    docker compose exec -T spark-master python /data/main.py || echo "Benchmark completed (or failed)"
}

echo "Starting full cluster scaling experiment..."
echo ""

start_1_node

echo ""
read -p "Press Enter to scale to 2 nodes..."
scale_to_2_nodes

echo ""
read -p "Press Enter to scale to 5 nodes..."
scale_to_5_nodes

echo ""
echo "=========================================="
echo "Full cluster is now running!"
echo "=========================================="
echo "Spark Master UI: http://localhost:8080"
echo ""
echo "To run experiments:"
echo "  docker compose exec spark-master python /data/main.py"
echo ""
