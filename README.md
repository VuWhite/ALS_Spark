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
├── install-deps.sh         # Python dependencies installer
└── data/                   # MovieLens dataset (auto-downloaded)
```

## Complete Setup from Scratch

### 1. Prerequisites Installation

#### For All Platforms:
- **Docker**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher
- **Git**: For cloning the repository
- **Minimum 8GB RAM** available for Docker containers

#### Docker Installation Options

Choose between Docker Desktop (GUI + CLI) or Docker Engine (CLI-only):

| Feature | Docker Desktop | Docker Engine |
|---------|---------------|---------------|
| **GUI Interface** | ✅ Yes | ❌ No |
| **Memory Usage** | Higher (~2GB) | Lower (~500MB) |
| **Setup Complexity** | Easier | More manual |
| **Platform Support** | macOS, Windows, Linux | Linux, macOS (via brew) |
| **Best For** | Beginners, GUI users | Servers, CLI enthusiasts |

#### Option A: Docker Desktop (Recommended for Beginners)

Docker Desktop includes both the Docker Engine and a graphical interface for managing containers.

**macOS:**
```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Docker Desktop
brew install --cask docker

# Install Git
brew install git

# Start Docker Desktop from Applications folder
```

**Windows (with WSL2):**
1. Install WSL2: `wsl --install`
2. Download and install Docker Desktop for Windows from https://www.docker.com/products/docker-desktop/
3. Enable WSL2 integration in Docker Desktop settings
4. Open WSL2 terminal and install Git:
   ```bash
   sudo apt update && sudo apt install -y git
   ```

#### Option B: Docker Engine (Lightweight, CLI-only)

**Ubuntu/Debian (Docker Engine):**
```bash
# Update package index
sudo apt update

# Install prerequisites
sudo apt install -y ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up the repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group (requires logout/login)
sudo usermod -aG docker $USER

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Verify installation
docker --version
docker compose version
```

**CentOS/RHEL/Fedora (Docker Engine):**
```bash
# Install prerequisites
sudo yum install -y yum-utils

# Add Docker repository
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo

# Install Docker Engine
sudo yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Start and enable Docker
sudo systemctl start docker
sudo systemctl enable docker

# Add user to docker group
sudo usermod -aG docker $USER

# Verify installation
docker --version
docker compose version
```

**macOS (Docker Engine via Homebrew):**
```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Docker Engine (without Desktop)
brew install docker docker-compose

# Start Docker service
brew services start docker

# Install Git
brew install git
```

**Windows (Docker Engine via WSL2):**
```bash
# Install WSL2
wsl --install

# Open WSL2 terminal and install Docker Engine
sudo apt update
sudo apt install -y docker.io docker-compose git

# Start Docker service
sudo service docker start

# Add user to docker group
sudo usermod -aG docker $USER
# Log out and log back in for group changes to take effect
```

### 2. Clone the Repository

```bash
# Clone the repository
git clone https://github.com/yourusername/ALS_Spark.git
cd ALS_Spark

# Make scripts executable
chmod +x cluster.sh scale_cluster.sh install-deps.sh
```

### 3. System Requirements Check

Verify your system meets the requirements:

```bash
# Check Docker installation
docker --version
docker-compose --version

# Check available memory (Linux/macOS)
free -h  # Linux
sysctl hw.memsize  # macOS

# On Windows WSL2, check from PowerShell:
wsl --system
free -h
```

**Minimum Requirements:**
- **RAM**: 8GB total, 6GB available for Docker
- **Disk Space**: 10GB free space
- **CPU**: 2+ cores recommended

### 4. First-Time Setup

```bash
# Start with 1 worker node (minimal setup)
./cluster.sh start 1

# Wait for cluster to be ready (check http://localhost:8080)
# This may take 2-3 minutes on first run

# Install Python dependencies in all containers
./install-deps.sh

# Verify installation
docker compose exec spark-master python3 -c "import pyspark; import numpy; print('Dependencies installed successfully')"
```

### 5. Test the Setup

```bash
# Run a simple test to verify everything works
./cluster.sh run1

# This will:
# 1. Start a 1-node cluster
# 2. Download the MovieLens 32M dataset (~1GB)
# 3. Run the ALS recommendation engine
# 4. Display memory analysis and performance metrics
```

### 6. Troubleshooting Common Issues

#### Docker Permission Denied (Docker Engine):
```bash
# Add user to docker group (Linux/macOS)
sudo usermod -aG docker $USER
# Log out and log back in for group changes to take effect

# Alternative: Run with sudo (not recommended for regular use)
sudo ./cluster.sh start 1
```

#### Docker Service Not Running (Docker Engine):
```bash
# Check Docker service status
sudo systemctl status docker  # Linux
brew services list | grep docker  # macOS

# Start Docker service
sudo systemctl start docker  # Linux
brew services start docker   # macOS
```

#### Port Conflicts:
If ports 8080, 7077, or 4040 are already in use:
```bash
# Edit docker-compose.yml and change port mappings
# Example: change "8080:8080" to "8081:8080"
```

#### Insufficient Memory:
```bash
# Adjust memory limits in docker-compose.yml
# Reduce mem_limit values for workers (e.g., from 6g to 4g)
```

#### Slow Network/Download:
The MovieLens dataset is ~1GB. If download is slow:
```bash
# Manually download from: https://files.grouplens.org/datasets/movielens/ml-32m.zip
# Extract to ./data/ml-32m/ directory
```

#### Docker Desktop vs Engine Specific Issues:

**Docker Desktop:**
- Ensure "Use Docker Compose V2" is enabled in settings
- Allocate sufficient memory in Resources settings (minimum 6GB)
- Enable WSL2 integration on Windows

**Docker Engine:**
- Verify docker-compose-plugin is installed (not just docker-compose)
- Check user is in docker group: `groups | grep docker`
- Ensure Docker service is running: `sudo systemctl is-active docker`

### 7. Quick Start Commands Reference

```bash
# Start cluster with N workers (1-5)
./cluster.sh start 1
./cluster.sh start 2
./cluster.sh start 5

# Run experiments
./cluster.sh run1    # 1-node experiment
./cluster.sh run2    # 2-node experiment  
./cluster.sh run5    # 5-node experiment

# Cluster management
./cluster.sh status  # Check cluster status
./cluster.sh stop    # Stop all containers
./cluster.sh clean   # Remove containers and volumes
./cluster.sh logs    # View container logs

# Dynamic scaling
./cluster.sh scale 3 # Scale to 3 workers
```

### 8. Accessing Spark Web UIs

- **Spark Master UI**: http://localhost:8080
  - Shows cluster overview and worker status
- **Spark Application UI**: http://localhost:4040 (when job is running)
  - Shows detailed job metrics and DAG visualization

### 9. Next Steps

After successful setup:
1. Run the full experiment sequence: `./scale_cluster.sh`
2. Monitor memory usage in Spark UI during execution
3. Check console output for memory pressure analysis
4. Capture screenshots of Spark UI for documentation

## Prerequisites (Summary)

- Docker and Docker Compose installed
- Git for cloning repository
- 8GB+ RAM available for Docker
- 10GB+ free disk space
- Internet connection for downloading dataset

## Quick Start (After Setup)

Once you've completed the setup from scratch above, you can use these quick commands:

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

## Setup Verification Checklist

After completing the setup, verify everything works:

### For Docker Desktop Users:
```bash
# 1. Check Docker Desktop is running
docker ps

# 2. Start minimal cluster
./cluster.sh start 1

# 3. Verify Spark Master UI loads
curl -s http://localhost:8080 | grep -i spark

# 4. Check worker is registered
docker compose exec spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077 --conf "spark.executor.memory=1g" <<< "sc.parallelize(1 to 10).count()"

# 5. Run quick test
./cluster.sh run1
```

### For Docker Engine Users:
```bash
# 1. Verify Docker Engine is installed and running
docker --version
docker compose version
sudo systemctl is-active docker  # Should return "active"

# 2. Test Docker without sudo (after adding user to docker group)
docker run hello-world

# 3. Start minimal cluster
./cluster.sh start 1

# 4. Verify containers are running
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 5. Check Spark Master is accessible
curl -s http://localhost:8080 | grep -i spark || echo "Spark UI not yet ready, wait 30 seconds and retry"

# 6. Run quick test
./cluster.sh run1
```

**Expected Output:**
- Spark Master UI accessible at http://localhost:8080
- Worker node shows as "ALIVE" in UI
- Test job completes successfully
- MovieLens dataset downloads automatically
- ALS experiment runs with memory analysis output

## Common Issues and Solutions

### Issue: "Permission denied" when running scripts
```bash
chmod +x *.sh
```

### Issue: Docker containers fail to start
```bash
# Check Docker daemon is running
sudo systemctl status docker  # Linux
# Restart Docker Desktop on macOS/Windows
```

### Issue: Port 8080 already in use
```bash
# Edit docker-compose.yml and change port mapping
# Change "8080:8080" to "8081:8080"
```

### Issue: Out of memory errors
```bash
# Reduce memory limits in docker-compose.yml
# Change mem_limit from "6g" to "4g" for workers
```

### Issue: Slow dataset download
```bash
# Download manually and place in ./data/ml-32m/
wget https://files.grouplens.org/datasets/movielens/ml-32m.zip -O /tmp/ml-32m.zip
unzip /tmp/ml-32m.zip -d ./data/
```

## Technical Notes

- MovieLens 32M: ~32 million ratings, ~1GB raw CSV
- ALS expands data in memory significantly (factor matrices)
- With 6GB executor and ~3GB ALS memory requirement per partition, Spark can:
  - Cache data across executors
  - Handle larger partitions in memory
  - Use shuffle operations for factorization

## Getting Help

If you encounter issues:
1. Check the troubleshooting section above
2. Run `./cluster.sh logs` to see container logs
3. Verify Docker has sufficient resources (Settings → Resources)
4. Ensure no firewall is blocking container communication
