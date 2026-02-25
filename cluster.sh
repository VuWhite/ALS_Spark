#!/bin/bash

set -e

echo "=========================================="
echo "  Spark Cluster Management Script"
echo "=========================================="

COMPOSE_FILE="docker-compose.yml"

show_usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  start <n>     Start cluster with n worker nodes (1-5)"
    echo "  stop          Stop all containers"
    echo "  status        Show cluster status"
    echo "  scale <n>     Scale to n worker nodes dynamically"
    echo "  logs          Show container logs"
    echo "  clean         Remove all containers and volumes"
    echo ""
    echo "Examples:"
    echo "  $0 start 1    Start with 1 worker"
    echo "  $0 start 2    Start with 2 workers"
    echo "  $0 start 5    Start with 5 workers (full cluster)"
    echo "  $0 scale 3    Scale to 3 workers"
    echo "  $0 run1       Run 1-node experiment"
    echo "  $0 run2       Run 2-node experiment"
    echo "  $0 run5       Run 5-node experiment"
}

wait_for_master() {
    echo "Waiting for Spark Master to be ready..."
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:8080 > /dev/null 2>&1; then
            echo "Spark Master is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        echo "Attempt $attempt/$max_attempts - waiting..."
        sleep 2
    done
    
    echo "Timeout waiting for Spark Master"
    return 1
}

start_cluster() {
    local num_workers=$1
    
    if [ "$num_workers" -lt 1 ] || [ "$num_workers" -gt 5 ]; then
        echo "Error: Number of workers must be between 1 and 5"
        exit 1
    fi
    
    echo "Starting Spark cluster with $num_workers worker(s)..."
    
    if [ "$num_workers" -eq 1 ]; then
        docker compose up -d spark-master spark-worker-1
    elif [ "$num_workers" -eq 2 ]; then
        docker compose up -d spark-master spark-worker-1 spark-worker-2
    else
        docker compose up -d
    fi
    
    wait_for_master
    
    echo ""
    echo "=========================================="
    echo "  Cluster Status"
    echo "=========================================="
    docker compose ps
    
    echo ""
    echo "Spark Master UI: http://localhost:8080"
    echo "Spark Master:     spark://localhost:7077"
}

stop_cluster() {
    echo "Stopping Spark cluster..."
    docker compose down
    echo "Cluster stopped."
}

show_status() {
    echo "=========================================="
    echo "  Cluster Status"
    echo "=========================================="
    docker compose ps
    
    echo ""
    echo "Worker containers:"
    docker ps --filter "name=spark-worker" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
}

scale_cluster() {
    local num_workers=$1
    
    if [ "$num_workers" -lt 1 ] || [ "$num_workers" -gt 5 ]; then
        echo "Error: Number of workers must be between 1 and 5"
        exit 1
    fi
    
    echo "Scaling cluster to $num_workers worker(s)..."
    
    case $num_workers in
        1)
            docker compose up -d --scale spark-worker=1 spark-worker-1
            docker compose stop spark-worker-2 spark-worker-3 spark-worker-4 spark-worker-5 2>/dev/null || true
            ;;
        2)
            docker compose up -d --scale spark-worker=2
            docker compose stop spark-worker-3 spark-worker-4 spark-worker-5 2>/dev/null || true
            ;;
        3)
            docker compose up -d --scale spark-worker=3
            docker compose stop spark-worker-4 spark-worker-5 2>/dev/null || true
            ;;
        4)
            docker compose up -d --scale spark-worker=4
            docker compose stop spark-worker-5 2>/dev/null || true
            ;;
        5)
            docker compose up -d --scale spark-worker=5
            ;;
    esac
    
    sleep 5
    show_status
}

show_logs() {
    docker compose logs -f
}

clean_cluster() {
    echo "WARNING: This will remove all containers and volumes!"
    read -p "Are you sure? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        docker compose down -v
        echo "Cluster cleaned."
    else
        echo "Cancelled."
    fi
}

run_experiment() {
    local num_workers=$1
    local partitions=$2
    
    start_cluster $num_workers
    
    echo ""
    echo "Starting ALS experiment..."
    docker compose exec -T spark-master python3 /data/main.py
    
    if [ $? -eq 0 ]; then
        echo "Experiment completed successfully!"
    else
        echo "Experiment failed!"
    fi
}

run_1node() {
    echo "Starting 1-node experiment..."
    
    docker compose up -d spark-master spark-worker-1
    
    wait_for_master
    
    echo ""
    echo "Running 1-node ALS experiment..."
    docker compose exec -T spark-master python3 /data/main.py
    
    if [ $? -eq 0 ]; then
        echo "1-node experiment completed successfully!"
    else
        echo "1-node experiment failed!"
    fi
}

run_2node() {
    echo "Starting 2-node experiment..."
    
    docker compose up -d spark-master spark-worker-1 spark-worker-2
    
    wait_for_master
    
    echo ""
    echo "Running 2-node ALS experiment..."
    docker compose exec -T spark-master python3 /data/main.py
    
    if [ $? -eq 0 ]; then
        echo "2-node experiment completed successfully!"
    else
        echo "2-node experiment failed!"
    fi
}

run_5node() {
    echo "Starting 5-node experiment..."
    
    docker compose up -d
    
    wait_for_master
    
    echo ""
    echo "Running 5-node ALS experiment..."
    docker compose exec -T spark-master python3 /data/main.py
    
    if [ $? -eq 0 ]; then
        echo "5-node experiment completed successfully!"
    else
        echo "5-node experiment failed!"
    fi
}

CMD=${1:-}
shift || true

case $CMD in
    start)
        NUM_WORKERS=${1:-1}
        start_cluster $NUM_WORKERS
        ;;
    stop)
        stop_cluster
        ;;
    status)
        show_status
        ;;
    scale)
        NUM_WORKERS=${1:-1}
        scale_cluster $NUM_WORKERS
        ;;
    logs)
        show_logs
        ;;
    clean)
        clean_cluster
        ;;
    experiment)
        NUM_WORKERS=${1:-1}
        PARTITIONS=${2:-4}
        run_experiment $NUM_WORKERS $PARTITIONS
        ;;
    run1)
        run_1node
        ;;
    run2)
        run_2node
        ;;
    run5)
        run_5node
        ;;
    *)
        show_usage
        ;;
esac
