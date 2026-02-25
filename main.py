#!/usr/bin/env python3
"""
ALS Recommendation Engine using PySpark
Demonstrates handling of MovieLens 32M dataset larger than executor memory
"""

import os
import sys
import time
import requests
import zipfile
import shutil
import yaml
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, when
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark import SparkConf, SparkContext

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
DATA_DIR = Path("/data")
RATINGS_FILE = DATA_DIR / "ml-32m" / "ratings.csv"


class MemoryCalculator:
    @staticmethod
    def calculate_dataset_size(df) -> Dict[str, float]:
        num_records = df.count()
        estimated_bytes_per_row = 32
        estimated_memory_mb = (num_records * estimated_bytes_per_row) / (1024 * 1024)
        als_factor_multiplier = 3
        estimated_als_memory_mb = estimated_memory_mb * als_factor_multiplier

        return {
            "num_records": num_records,
            "raw_size_mb": estimated_memory_mb,
            "als_memory_mb": estimated_als_memory_mb,
            "als_factor_multiplier": als_factor_multiplier,
        }

    @staticmethod
    def print_memory_pressure(executor_memory_mb: float, dataset_info: Dict):
        print("\n" + "=" * 60)
        print("MEMORY PRESSURE ANALYSIS")
        print("=" * 60)
        print(f"Executor Memory Limit:     {executor_memory_mb:.2f} MB")
        print(f"Dataset Records:           {dataset_info['num_records']:,}")
        print(f"Estimated Raw Size:        {dataset_info['raw_size_mb']:.2f} MB")
        print(f"Estimated ALS Memory:      {dataset_info['als_memory_mb']:.2f} MB")
        print(f"Memory Multiplier (ALS):   {dataset_info['als_factor_multiplier']}x")

        if dataset_info["als_memory_mb"] > executor_memory_mb:
            print(f"\n*** MEMORY PRESSURE DETECTED ***")
            print(
                f"Dataset requires {dataset_info['als_memory_mb'] / executor_memory_mb:.1f}x available memory"
            )
            print("Spark will need to spill to disk!")
        print("=" * 60 + "\n")


class PartitionAnalyzer:
    @staticmethod
    def get_partition_stats(df) -> List[Tuple[int, int]]:
        """Extract partition statistics using rdd.glom()"""
        partition_counts = df.rdd.glom().map(len).collect()
        return list(enumerate(partition_counts))

    @staticmethod
    def print_partition_stats(df, label: str = ""):
        print(f"\n{'=' * 60}")
        print(f"PARTITION STATISTICS {label}")
        print("=" * 60)

        partition_stats = PartitionAnalyzer.get_partition_stats(df)

        print(f"Total Partitions: {len(partition_stats)}")
        print(f"Total Records:    {sum(count for _, count in partition_stats):,}")
        print(f"\nRecords per partition:")

        for partition_id, count in partition_stats:
            print(f"  Partition {partition_id:3d}: {count:>10,} records")

        if partition_stats:
            counts = [c for _, c in partition_stats]
            print(f"\nDistribution Stats:")
            print(f"  Min:    {min(counts):,}")
            print(f"  Max:    {max(counts):,}")
            print(f"  Avg:    {sum(counts) // len(counts):,}")

        print("=" * 60 + "\n")


class SparkUIExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext

    def get_active_jobs(self) -> Dict:
        status_tracker = self.sc.statusTracker()
        jobs = status_tracker.getJobIdsForGroup()

        job_info = []
        for job_id in jobs:
            try:
                job_status = status_tracker.getJobInfo(job_id)
                if job_status:
                    job_info.append(
                        {
                            "job_id": job_id,
                            "status": str(job_status.status),
                            "stage_ids": list(job_status.stageIds),
                        }
                    )
            except:
                pass

        return {"active_jobs": job_info, "total": len(job_info)}

    def get_storage_info(self) -> Dict:
        storage_info = []
        try:
            if self.sc._jsc:
                jvm = self.sc._jvm
                storage = jvm.org.apache.spark.storage.StorageRunner
                if storage:
                    rdds = self.sc._jsc.sc().getRDDStorageInfo()
                    for rdd in rdds:
                        storage_info.append(
                            {
                                "rdd_id": rdd.id(),
                                "name": rdd.name(),
                                "num_partitions": rdd.numPartitions(),
                                "memory_used_mb": rdd.memUsed() / (1024 * 1024),
                                "disk_used_mb": rdd.diskUsed() / (1024 * 1024),
                            }
                        )
        except Exception as e:
            logger.warning(f"Could not get detailed storage info: {e}")

        return {"storage_info": storage_info}

    def print_spark_ui_evidence(self, phase: str):
        print(f"\n{'*' * 60}")
        print(f"SPARK UI EVIDENCE - {phase}")
        print("*" * 60)

        active_jobs = self.get_active_jobs()
        print(f"\nActive Jobs: {active_jobs['total']}")
        for job in active_jobs["active_jobs"][:5]:
            print(
                f"  Job {job['job_id']}: {job['status']} - Stages: {job['stage_ids']}"
            )

        storage = self.get_storage_info()
        print(f"\nStorage Status:")
        if storage["storage_info"]:
            for si in storage["storage_info"]:
                print(
                    f"  RDD {si['rdd_id']}: {si['name']}, Partitions: {si['num_partitions']}, "
                    f"Memory: {si['memory_used_mb']:.2f}MB, Disk: {si['disk_used_mb']:.2f}MB"
                )
        else:
            print("  (Storage info not available - data may be in memory only)")

        print("*" * 60 + "\n")


class DataLoader:
    @staticmethod
    def download_movielens() -> Path:
        if RATINGS_FILE.exists():
            logger.info(f"Ratings file already exists at {RATINGS_FILE}")
            return RATINGS_FILE

        DATA_DIR.mkdir(exist_ok=True)
        zip_path = DATA_DIR / "ml-32m.zip"

        logger.info(f"Downloading MovieLens 32M from {MOVIELENS_URL}")
        logger.info("This may take several minutes...")

        with requests.get(MOVIELENS_URL, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            downloaded = 0

            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            if downloaded % (10 * 1024 * 1024) < 8192:
                                logger.info(f"Downloaded: {percent:.1f}%")

        logger.info("Extracting archive...")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(DATA_DIR)

        zip_path.unlink()
        logger.info("Download and extraction complete!")

        return RATINGS_FILE

    @staticmethod
    def load_ratings(spark: SparkSession, file_path: Path, partitions: int = None):
        logger.info(f"Loading ratings from {file_path}")

        df = spark.read.csv(str(file_path), header=True, inferSchema=True)

        df = df.select(
            col("userId").cast("integer"),
            col("movieId").cast("integer"),
            col("rating").cast("float"),
            col("timestamp").cast("long"),
        )

        if partitions:
            logger.info(f"Repartitioning to {partitions} partitions")
            df = df.repartition(partitions)

        return df


class ALSTrainer:
    def __init__(self, rank: int = 10, max_iter: int = 10, reg_param: float = 0.1):
        self.rank = rank
        self.max_iter = max_iter
        self.reg_param = reg_param
        self.cold_start_strategy = "drop"

    def train(self, train_data, num_executors: int) -> Tuple[ALS, float]:
        logger.info(
            f"Training ALS model: rank={self.rank}, maxIter={self.max_iter}, regParam={self.reg_param}"
        )

        als = ALS(
            rank=self.rank,
            maxIter=self.max_iter,
            regParam=self.reg_param,
            coldStartStrategy=self.cold_start_strategy,
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
        )

        start_time = time.time()
        model = als.fit(train_data)
        training_time = time.time() - start_time

        logger.info(f"Training completed in {training_time:.2f} seconds")

        return model, training_time

    def evaluate(self, model, test_data) -> Dict[str, float]:
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(
            metricName="rmse", labelCol="rating", predictionCol="prediction"
        )

        rmse = evaluator.evaluate(predictions)

        return {"rmse": rmse}


class BenchmarkRunner:
    MEMORY_CONFIG = {
        1: "6g",
        2: "6g",
        5: "6g",
    }

    def __init__(self, spark_master: str, executor_memory: str = "1g"):
        self.spark_master = spark_master
        self.requested_executor_memory = executor_memory

    def _parse_memory(self, mem_str: str) -> float:
        if mem_str.endswith("g"):
            return float(mem_str[:-1]) * 1024
        elif mem_str.endswith("m"):
            return float(mem_str[:-1])
        return float(mem_str)

    def _get_worker_memory_from_compose(
        self, compose_path: str = "docker-compose.yml"
    ) -> float:
        try:
            with open(compose_path) as f:
                config = yaml.safe_load(f)
        except FileNotFoundError:
            return None

        memories = []
        for name, spec in config.get("services", {}).items():
            if name.startswith("spark-worker"):
                mem_limit = spec.get("mem_limit", "1g")
                memories.append(self._parse_memory(mem_limit))

        return min(memories) if memories else None

    def create_spark_session(self, app_name: str, num_executors: int) -> SparkSession:
        builder = (
            SparkSession.builder.appName(app_name)
            .master(self.spark_master)
            .config("spark.executor.memory", self.executor_memory)
            .config("spark.executor.cores", "1")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.default.parallelism", num_executors)
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            .config("spark.rpc.message.maxSize", "256")
            .config("spark.kryoserializer.buffer.max", "512m")
        )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("INFO")

        return spark

    def run_experiment(self, num_nodes: int, data_partitions: int = None):
        self.executor_memory = self.MEMORY_CONFIG.get(
            num_nodes, self.requested_executor_memory
        )
        self.executor_memory_mb = self._parse_memory(self.executor_memory)

        print("\n" + "=" * 70)
        print(f"EXPERIMENT: {num_nodes} NODE(S)")
        print("=" * 70)

        num_executors = num_nodes

        spark = self.create_spark_session(f"ALS-ALS-{num_nodes}nodes", num_executors)
        ui_extractor = SparkUIExtractor(spark)

        load_start = time.time()

        try:
            ratings_df = DataLoader.load_ratings(spark, RATINGS_FILE, data_partitions)
            load_time = time.time() - load_start

            ui_extractor.print_spark_ui_evidence("After Data Loading")

            PartitionAnalyzer.print_partition_stats(
                ratings_df, f"({num_executors} executors)"
            )

            dataset_info = MemoryCalculator.calculate_dataset_size(ratings_df)
            MemoryCalculator.print_memory_pressure(
                self.executor_memory_mb, dataset_info
            )

            (train_data, test_data) = ratings_df.randomSplit([0.8, 0.2], seed=42)
            train_data = train_data.cache()
            test_data = test_data.cache()

            train_count = train_data.count()
            test_count = test_data.count()

            print(f"Training set: {train_count:,} records")
            print(f"Test set:     {test_count:,} records")

            ui_extractor.print_spark_ui_evidence("Before ALS Training")

            trainer = ALSTrainer(rank=10, max_iter=10, reg_param=0.1)
            model, train_time = trainer.train(train_data, num_executors)

            ui_extractor.print_spark_ui_evidence("After ALS Training")

            eval_results = trainer.evaluate(model, test_data)

            results = {
                "num_nodes": num_nodes,
                "load_time": load_time,
                "train_time": train_time,
                "total_time": load_time + train_time,
                "rmse": eval_results["rmse"],
                "train_records": train_count,
                "test_records": test_count,
                "partitions": data_partitions or ratings_df.rdd.getNumPartitions(),
            }

            print(f"\n{'*' * 60}")
            print(f"RESULTS - {num_nodes} NODE(S)")
            print("*" * 60)
            print(f"Data Loading Time:  {results['load_time']:.2f}s")
            print(f"ALS Training Time: {results['train_time']:.2f}s")
            print(f"Total Time:         {results['total_time']:.2f}s")
            print(f"RMSE:              {results['rmse']:.4f}")
            print(f"Partitions:        {results['partitions']}")
            print("*" * 60 + "\n")

        finally:
            spark.stop()
            time.sleep(2)

        return results


def main():
    print("\n" + "#" * 70)
    print("#  ALS RECOMMENDATION ENGINE - MOVIELENS 32M")
    print("#  Demonstrating Spark's ability to handle data larger than memory")
    print("#" * 70)

    logger.info("Downloading MovieLens 32M dataset...")
    DataLoader.download_movielens()

    spark_master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
    executor_memory = os.environ.get("EXECUTOR_MEMORY", "4g")

    print(f"\nSpark Master: {spark_master}")
    print(f"Executor Memory: {executor_memory}")

    runner = BenchmarkRunner(spark_master, executor_memory)

    experiment_configs = [(1, 4), (2, 8), (5, 20)]

    all_results = []

    for num_nodes, partitions in experiment_configs:
        print(f"\n\n{'#' * 70}")
        print(
            f"#  STARTING EXPERIMENT WITH {num_nodes} NODE(S), {partitions} PARTITIONS"
        )
        print(f"{'#' * 70}")

        try:
            results = runner.run_experiment(num_nodes, partitions)
            all_results.append(results)
        except Exception as e:
            logger.error(f"Experiment failed for {num_nodes} nodes: {e}")
            import traceback

            traceback.print_exc()

    print("\n" + "=" * 70)
    print("SUMMARY OF ALL EXPERIMENTS")
    print("=" * 70)
    print(
        f"{'Nodes':<8} {'Partitions':<12} {'Load(s)':<10} {'Train(s)':<10} {'Total(s)':<10} {'RMSE':<8}"
    )
    print("-" * 70)
    for r in all_results:
        print(
            f"{r['num_nodes']:<8} {r['partitions']:<12} {r['load_time']:<10.2f} {r['train_time']:<10.2f} {r['total_time']:<10.2f} {r['rmse']:<8.4f}"
        )
    print("=" * 70)

    print("\n" + "#" * 70)
    print("#  EXPERIMENT COMPLETE")
    print("#" * 70)


if __name__ == "__main__":
    main()
