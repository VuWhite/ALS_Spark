# ALS Recommendation Engine with PySpark

This project demonstrates building an Alternating Least Squares (ALS) recommendation engine using PySpark on a multi-container Docker cluster. It uses the MovieLens 32M dataset to show Spark's ability to handle data larger than the allocated executor memory.

## Project Structure

```
ALS_Spark/
├── docker-compose.yml      # Spark cluster configuration
├── main.py                 # ALS implementation and benchmarking
├── cluster.sh              # Cluster management script
├── scale_cluster.sh        # Dynamic scaling script (1->2->5)
├── Dockerfile              # Custom Spark image
├── README.md               # This file
└── data/                   # MovieLens dataset (auto-downloaded)
```

## Prerequisites

- Docker and Docker Compose
- WSL2 (if running on Windows)
- At least 8GB RAM available for Docker

## Quick Start

### 1. Start the Cluster

```bash
# Start with 1 worker node
./cluster.sh start 1

# Or start with 5 workers (full cluster)
./cluster.sh start 5
```

### 2. Run the ALS Experiment

```bash
# Run 1-node experiment
./cluster.sh run1

# Run 2-node experiment
./cluster.sh run2

# Run 5-node experiment
./cluster.sh run5

# Or manually inside the container
docker compose exec spark-master python3 /data/main.py
```

## Memory Configuration

The cluster is configured with 6GB per worker node:

- **Worker Memory**: 6GB per worker
- **Worker Cores**: 1 core per worker
- **Master Memory**: 3GB
- **Executor Cores**: 1 core (matches worker)
- **Total with 5 workers**: 33GB (3GB master + 30GB for 5 workers)

## How to Capture Spark UI Screenshots

### Accessing the Spark UI

1. **Spark Master UI (Port 8080)**
   - URL: `http://localhost:8080`
   - Shows cluster overview, worker status, running applications

2. **Spark Application UI (Port 4040)**
   - Available when a job is running
   - Shows detailed job, stage, task, and storage information

### Capturing Evidence Under Load

To capture the best screenshots for your discussion:

#### Step 1: Start the 5-Node Cluster

```bash
./cluster.sh start 5
```

Wait for all workers to register with the master (check http://localhost:8080).

#### Step 2: Run the Experiment

In one terminal, run:
```bash
docker compose exec spark-master python /data/main.py
```

#### Step 3: Capture Screenshots

While the job is running, open browser tabs and capture:

1. **Executors Tab** (http://localhost:8080)
   - Shows all 5 worker containers
   - Displays storage memory usage per executor
   - Shows active/dead executors

2. **Application UI** (http://localhost:4040)
   
   **Storage Tab:**
   - Shows RDD storage across executors
   - Look for memory usage vs disk (spilling evidence)
   
   **Stages Tab:**
   - Captures during ALS iterations
   - Shows complex shuffle dependencies
   - Demonstrates the alternating minimization pattern
   
   **DAG Visualization:**
   - Shows the directed acyclic graph of transformations
   - Particularly visible during the ALS.fit() phase

### What to Look For

1. **Memory Pressure Evidence**:
   - "Storage" tab showing Memory: X% / 100%
   - Disk usage increasing for cached data
   - Log messages about spilling to disk

2. **Data Distribution**:
   - "Executors" tab showing records processed per executor
   - Partition statistics from console output

3. **ALS-Specific Visualizations**:
   - Multiple stages with shuffle operations
   - Dependencies between user and item factor computation

## Experiment Results

The script runs experiments with:

| Nodes | Partitions | Expected Behavior |
|-------|------------|-------------------|
| 1     | 4          | Heavy spilling, slow |
| 2     | 8          | Moderate parallelism |
| 5     | 20         | Good distribution |

## Key Features

### 1. Memory Pressure Demonstration
- Calculates dataset size vs executor memory
- Prints memory pressure analysis to console
- Shows when Spark must spill to disk

### 2. Partition Statistics
- Uses `rdd.glom().map(len).collect()` to get per-partition counts
- Shows distribution across executors

### 3. ALS Configuration
- `rank=10`: Number of latent factors
- `maxIter=10`: Maximum iterations
- `regParam=0.1`: Regularization parameter
- `coldStartStrategy="drop"`: Handles cold start

### 4. Benchmarking
- Logs execution time for:
  - Data loading
  - Training
  - Total pipeline
- Outputs RMSE evaluation metric

## Troubleshooting

### Workers Not Connecting
```bash
# Check worker logs
docker compose logs spark-worker-1

# Restart cluster
./cluster.sh stop
./cluster.sh start 5
```

### Out of Memory Errors
- Reduce number of partitions
- Increase executor memory in docker-compose.yml
- Reduce ALS rank

### Spark UI Not Accessible
```bash
# Check if containers are running
docker compose ps

# Check port mapping
docker port spark-master
```

## Dynamic Scaling

To scale the cluster dynamically:

```bash
# Scale to 1 node
./cluster.sh scale 1

# Scale to 2 nodes  
./cluster.sh scale 2

# Scale to 5 nodes
./cluster.sh scale 5
```

Or use the automated scaling script:
```bash
./scale_cluster.sh
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| SPARK_MASTER | spark://spark-master:7077 | Spark master URL |
| EXECUTOR_MEMORY | 6g | Memory per executor |

## Technical Notes

- MovieLens 32M: ~32 million ratings, ~1GB raw CSV
- ALS expands data in memory significantly (factor matrices)
- With 6GB executor and ~3GB ALS memory requirement per partition, Spark can:
  - Cache data across executors
  - Handle larger partitions in memory
  - Use shuffle operations for factorization
