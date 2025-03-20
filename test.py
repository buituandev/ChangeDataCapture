#!/usr/bin/env python3
import random
import time
import sys
import os
import json
from datetime import datetime, timedelta
import threading
import argparse
import signal
import queue

# ANSI color codes for colorized output
COLORS = {
    "RESET": "\033[0m",
    "INFO": "\033[94m",     # Blue
    "DEBUG": "\033[96m",    # Cyan
    "WARN": "\033[93m",     # Yellow
    "ERROR": "\033[91m",    # Red
    "FATAL": "\033[97;41m", # White on red background
    "SUCCESS": "\033[92m",  # Green
    "MAGENTA": "\033[95m",  # Magenta for special messages
    "BOLD": "\033[1m",      # Bold
    "UNDERLINE": "\033[4m", # Underline
}

# Components that might appear in Spark logs
COMPONENTS = [
    "SparkContext", "SparkSubmit", "DAGScheduler", "TaskScheduler", "TaskSetManager", 
    "Executor", "CoarseGrainedExecutorBackend", "ApplicationMaster", "YarnClusterScheduler",
    "BlockManager", "MemoryStore", "DiskStore", "NettyBlockTransferService", "MapOutputTracker",
    "ShuffleManager", "ShuffleBlockFetcherIterator", "SortShuffleManager", "ShuffleMapTask",
    "ResultTask", "SparkListener", "LiveListenerBus", "EventLoggingListener", "JobLogger",
    "HiveMetaStore", "HiveSessionState", "SQLContext", "SparkSession", "CatalystOptimizer",
    "Analyzer", "QueryExecution", "WholeStageCodegen", "UnsafeRow", "InMemoryFileIndex",
    "HadoopRDD", "FileScanRDD", "ShuffledRDD", "MapPartitionsRDD", "ParallelCollectionRDD",
    "StorageLevel", "JDBCRelation", "StreamingContext", "StreamingQueryManager", "DataStreamWriter",
    "RDDOperationScope", "JobWaiter", "ContextCleaner", "MetricsSystem", "SparkUI"
]

# Representative cluster nodes
CLUSTER_NODES = [
    "master-1", "worker-1", "worker-2", "worker-3", "worker-4", "worker-5", 
    "worker-6", "worker-7", "worker-8", "datanode-1", "datanode-2", "datanode-3"
]

# Common operations in Spark
OPERATIONS = [
    "map", "filter", "flatMap", "mapPartitions", "sample", "union", "intersection", 
    "distinct", "groupByKey", "reduceByKey", "aggregateByKey", "sortByKey", "join", 
    "cogroup", "cartesian", "pipe", "coalesce", "repartition", "repartitionAndSortWithinPartitions",
    "count", "first", "take", "takeSample", "takeOrdered", "saveAsTextFile", "saveAsSequenceFile",
    "saveAsObjectFile", "countByKey", "foreach", "foreachPartition", "collectAsMap"
]

# SQL operations
SQL_OPERATIONS = [
    "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "JOIN",
    "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL OUTER JOIN", "UNION", "INTERSECT"
]

# Job stages
JOB_STAGES = ["PREPARING", "RUNNING", "SUCCEEDED", "FAILED", "KILLED"]

# Job types
JOB_TYPES = [
    "DataLoad", "ETL", "DataTransformation", "DataValidation", "Analytics", 
    "MachineLearning", "ReportGeneration", "DataExport", "DataIngestion"
]

# Log levels with weighted probabilities
LOG_LEVELS = {
    "INFO": 0.70,    # 70% of logs
    "WARN": 0.15,    # 15% of logs
    "DEBUG": 0.10,   # 10% of logs
    "ERROR": 0.04,   # 4% of logs
    "FATAL": 0.01    # 1% of logs
}

# Common message patterns
MESSAGE_PATTERNS = {
    "INFO": [
        "Starting {operation} operation on {partitions} partitions",
        "Finished {operation} in {duration} ms",
        "Submitting job {job_id} with {tasks} tasks",
        "Registering RDD ID {rdd_id} with {partitions} partitions",
        "Storing data for stage {stage_id} with {size} bytes",
        "Executing SQL query: {sql_query}",
        "Created broadcast variable {broadcast_id} with {size} bytes",
        "Launching task {task_id} on executor {executor_id}",
        "Task {task_id} finished in {duration} ms",
        "Stage {stage_id} completed successfully with {tasks} tasks",
        "Job {job_id} finished in {duration} ms",
        "Persisting RDD {rdd_id} with storage level {storage_level}",
        "Allocating {memory} MB for executor {executor_id}",
        "Opened {connection_type} connection to {host}:{port}",
        "Received {size} bytes of shuffle data from {host}",
        "Starting {service} service on {host}:{port}",
        "Checkpoint for RDD {rdd_id} completed in {duration} ms",
        "Cluster status: {active_nodes} active nodes, {memory_used} GB used",
        "Scheduling stage {stage_id} with {tasks} tasks",
        "Recovered {records} records from {source}"
    ],
    "DEBUG": [
        "Serializing task {task_id} for stage {stage_id}",
        "Verifying shuffle data for map {map_id}",
        "Processing partition {partition_id} of RDD {rdd_id}",
        "Task {task_id} sent to executor {executor_id}",
        "Caching data for RDD {rdd_id}",
        "Memory usage: {memory_used} MB / {memory_total} MB",
        "Parsing SQL query: {sql_query}",
        "Shuffle block {block_id} mapped to executor {executor_id}",
        "Resolving dependency for stage {stage_id}",
        "Checking locality constraints for task {task_id}",
        "Applying optimization rule: {rule_name}",
        "Serialized task {task_id} is {size} bytes",
        "Executor {executor_id} heap usage: {heap_used} / {heap_max}",
        "Evaluating predicate {predicate}",
        "Generating code for stage {stage_id}",
        "Analyzing data schema: {schema}",
        "Thread dump for executor {executor_id}: {thread_info}",
        "Unpersisting RDD {rdd_id}",
        "Tracking dependency for stage {stage_id}",
        "Shuffle read: {bytes_read} bytes from {blocks} blocks"
    ],
    "WARN": [
        "Retrying task {task_id} after failure",
        "Task {task_id} failed, but will be retried",
        "Slow shuffle detected for stage {stage_id}",
        "Low disk space on executor {executor_id}: {disk_free} free",
        "Executor {executor_id} is low on memory ({memory_free} free)",
        "Possible data skew detected in stage {stage_id}",
        "Partial success for query {query_id}",
        "Some partitions had no data for stage {stage_id}",
        "Connection to {host} timed out, retrying",
        "Shuffle data fetch failed for block {block_id}, retrying",
        "Task {task_id} execution time exceeds threshold",
        "GC overhead limit exceeded on executor {executor_id}",
        "Shuffle spill occurred for task {task_id}",
        "Broadcast variable {broadcast_id} is too large ({size} bytes)",
        "Driver memory usage is high: {memory_used} MB",
        "Speculative execution triggered for task {task_id}",
        "Dynamic allocation adding {executors} executors",
        "Detected outdated configuration: {config_key}",
        "Possible network congestion detected",
        "Skipping {records} corrupt records in partition {partition_id}"
    ],
    "ERROR": [
        "Task {task_id} failed after {attempts} attempts",
        "Stage {stage_id} failed with exception: {exception}",
        "Job {job_id} aborted due to stage failure",
        "Failed to execute SQL query: {sql_query}",
        "Executor {executor_id} lost: {reason}",
        "Unable to fetch shuffle block {block_id}",
        "Out of memory error on executor {executor_id}",
        "Connection to {host} failed: {reason}",
        "Failed to allocate memory for task {task_id}",
        "Data corruption detected in block {block_id}",
        "Task {task_id} killed by driver",
        "Error reading file: {file_path}",
        "Failed to serialize task {task_id}",
        "Unexpected error in stage {stage_id}: {error_message}",
        "Executor {executor_id} exited with code {exit_code}",
        "Detected deadlock in stage {stage_id}",
        "Failed to initialize SparkContext: {error_message}",
        "Error writing shuffle data: {error_message}",
        "RPC timeout exceeded for {operation}",
        "Lost executor {executor_id} on {host}: {reason}"
    ],
    "FATAL": [
        "Driver failed with unrecoverable error: {error_message}",
        "Cluster lost: {reason}",
        "Job {job_id} terminated due to unrecoverable error",
        "Critical system failure: {error_message}",
        "Spark context stopped due to unrecoverable error",
        "Executor {executor_id} crashed with fatal error",
        "Fatal error in task {task_id}: {error_message}",
        "Application master failed with code {exit_code}",
        "Critical resource shortage: {resource_type}",
        "System error: {error_message}"
    ]
}

# Common storage levels
STORAGE_LEVELS = [
    "MEMORY_ONLY", "MEMORY_AND_DISK", "MEMORY_ONLY_SER", 
    "MEMORY_AND_DISK_SER", "DISK_ONLY", "MEMORY_ONLY_2", 
    "MEMORY_AND_DISK_2"
]

# Common exceptions
EXCEPTIONS = [
    "java.lang.OutOfMemoryError", "java.lang.NullPointerException", 
    "java.io.IOException", "java.net.SocketTimeoutException",
    "org.apache.spark.SparkException", "java.lang.IllegalArgumentException",
    "java.util.concurrent.TimeoutException", "java.lang.ClassCastException",
    "java.lang.ArithmeticException", "java.lang.ClassNotFoundException",
    "java.lang.UnsupportedOperationException", "java.sql.SQLException",
    "java.lang.ArrayIndexOutOfBoundsException", "java.lang.RuntimeException",
    "org.apache.hadoop.hdfs.BlockMissingException", "org.apache.spark.shuffle.FetchFailedException",
    "org.apache.spark.memory.SparkOutOfMemoryError"
]

# Common configuration keys
CONFIG_KEYS = [
    "spark.executor.memory", "spark.driver.memory", "spark.executor.cores",
    "spark.dynamicAllocation.enabled", "spark.executor.instances",
    "spark.shuffle.service.enabled", "spark.memory.fraction",
    "spark.sql.shuffle.partitions", "spark.default.parallelism",
    "spark.speculation", "spark.io.compression.codec", "spark.serializer",
    "spark.network.timeout", "spark.locality.wait", "spark.task.maxFailures",
    "spark.sql.autoBroadcastJoinThreshold", "spark.memory.storageFraction",
    "spark.scheduler.mode", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version"
]

# Common file paths
FILE_PATHS = [
    "/data/warehouse/table1", "/data/raw/incoming", "/data/processed/daily",
    "/data/raw/customer_data.csv", "/data/warehouse/sales_fact.parquet",
    "/data/analytics/reports", "/tmp/spark-checkpoint", "/data/archive/backup",
    "/data/metrics/daily_aggregates", "/data/reference/lookup_tables",
    "/user/hive/warehouse/events", "/data/raw/weblogs", "/data/etl/staging",
    "/data/lake/bronze", "/data/lake/silver", "/data/lake/gold"
]

# Common host names
HOSTS = [
    "master-001", "worker-001", "worker-002", "worker-003", "worker-004",
    "worker-005", "worker-006", "worker-007", "worker-008", "worker-009",
    "worker-010", "worker-011", "worker-012", "worker-013", "worker-014",
    "datanode-001", "datanode-002", "datanode-003", "datanode-004", "datanode-005",
    "namenode-001", "resourcemanager-001", "historyserver-001"
]

# Error and warning reasons
ERROR_REASONS = [
    "Connection timed out", "Heartbeat timeout", "Out of memory", "Disk full",
    "Task timed out", "Executor died", "Node failed", "Network partition",
    "Container killed by YARN", "GC overhead limit exceeded", "File not found",
    "Permission denied", "Connection refused", "Data corruption", "Memory leak",
    "Resource allocation failed", "RPC timeout", "Task killed by user",
    "Runtime assertion failed", "Container exceeded physical memory limits"
]

# Thread-safe queues for job and task tracking
active_jobs = queue.Queue()
active_stages = queue.Queue()
active_tasks = queue.Queue()

# State management
current_job_id = 0
current_stage_id = 0
current_task_id = 0
current_rdd_id = 1000
current_broadcast_id = 0
current_query_id = 0
current_block_id = 0
current_executor_id = 1

# Initialize some job and task data
for _ in range(5):
    active_jobs.put({
        "id": current_job_id,
        "name": f"{random.choice(JOB_TYPES)}Job",
        "status": "RUNNING",
        "start_time": datetime.now() - timedelta(minutes=random.randint(1, 60)),
        "stages": random.randint(3, 15),
        "tasks": random.randint(20, 500)
    })
    current_job_id += 1

for _ in range(10):
    active_stages.put({
        "id": current_stage_id,
        "job_id": random.randint(0, current_job_id - 1),
        "tasks": random.randint(10, 100),
        "status": "RUNNING",
        "start_time": datetime.now() - timedelta(minutes=random.randint(1, 30))
    })
    current_stage_id += 1

for _ in range(50):
    active_tasks.put({
        "id": current_task_id,
        "stage_id": random.randint(0, current_stage_id - 1),
        "executor_id": random.randint(1, 20),
        "status": "RUNNING",
        "start_time": datetime.now() - timedelta(seconds=random.randint(1, 300))
    })
    current_task_id += 1

# Initialize executor data
executors = {}
for i in range(1, 21):
    executors[i] = {
        "host": random.choice(HOSTS),
        "cores": random.choice([2, 4, 8, 16]),
        "memory": random.choice([4096, 8192, 16384, 32768]),
        "tasks": random.randint(0, 10),
        "status": "ALIVE" if random.random() > 0.05 else "DEAD"
    }

# Global variables for event tracking
running = True
app_start_time = datetime.now() - timedelta(minutes=random.randint(10, 120))
app_id = f"application_{int(app_start_time.timestamp())}_{random.randint(1000, 9999)}"

def get_random_log_level():
    """Return a random log level based on weighted probabilities."""
    r = random.random()
    cumulative = 0
    for level, prob in LOG_LEVELS.items():
        cumulative += prob
        if r <= cumulative:
            return level
    return "INFO"  # Fallback

def get_random_message(level):
    """Return a random message pattern for the given log level."""
    if level in MESSAGE_PATTERNS:
        return random.choice(MESSAGE_PATTERNS[level])
    return random.choice(MESSAGE_PATTERNS["INFO"])

def format_timestamp(dt=None):
    """Format a datetime object as a log timestamp."""
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

def format_log_entry(level, component, message, host=None):
    """Format a complete log entry."""
    timestamp = format_timestamp()
    process_info = f"[{random.randint(1000, 9999)}:{random.randint(1, 20)}]"
    thread_info = f"[{random.choice(['dispatcher', 'executor-task', 'heartbeat', 'scheduler', 'dag-scheduler', 'netty', 'shuffle', 'broadcast', 'rpc', 'task-result', 'storage', 'streaming'])}]"
    
    if host is None:
        host = random.choice(HOSTS)
    
    # Color the log level if color is enabled
    level_str = f"{COLORS[level]}{level}{COLORS['RESET']}" if args.color else level
    
    return f"{timestamp} {level_str} {process_info} {thread_info} {component} - {message}"

def fill_template(template):
    """Fill a message template with realistic values."""
    # Define all possible placeholders
    placeholders = {
        "operation": random.choice(OPERATIONS),
        "partitions": random.randint(1, 1000),
        "duration": random.randint(10, 60000),
        "job_id": random.randint(0, current_job_id),
        "tasks": random.randint(1, 500),
        "rdd_id": random.randint(0, current_rdd_id),
        "stage_id": random.randint(0, current_stage_id),
        "size": f"{random.randint(1, 1000)}.{random.randint(0, 9)}MB",
        "sql_query": f"{random.choice(SQL_OPERATIONS)} * FROM table{random.randint(1, 20)} WHERE field{random.randint(1, 10)} > {random.randint(0, 100)}",
        "broadcast_id": random.randint(0, current_broadcast_id),
        "task_id": random.randint(0, current_task_id),
        "executor_id": random.randint(1, 20),
        "storage_level": random.choice(STORAGE_LEVELS),
        "memory": random.randint(512, 32768),
        "connection_type": random.choice(["HTTP", "JDBC", "HDFS", "S3", "YARN"]),
        "host": random.choice(HOSTS),
        "port": random.randint(1024, 65535),
        "active_nodes": random.randint(5, 50),
        "memory_used": random.randint(10, 500),
        "records": random.randint(1000, 1000000),
        "source": random.choice(["HDFS", "Kafka", "S3", "JDBC", "Hive"]),
        "map_id": random.randint(0, 1000),
        "partition_id": random.randint(0, 1000),
        "memory_total": random.randint(1000, 32768),
        "block_id": f"rdd_{random.randint(0, 1000)}_{random.randint(0, 100)}",
        "rule_name": random.choice(["ConstantFolding", "PushDownPredicate", "CollapseProject", "CombineFilters"]),
        "heap_used": f"{random.randint(1, 30)}GB",
        "heap_max": f"{random.randint(31, 64)}GB",
        "predicate": f"col{random.randint(1, 10)} > {random.randint(0, 100)}",
        "schema": f"id:int,name:string,value:double",
        "thread_info": f"Thread-{random.randint(1, 100)}",
        "blocks": random.randint(1, 1000),
        "bytes_read": f"{random.randint(1, 1000)}MB",
        "disk_free": f"{random.randint(1, 100)}GB",
        "memory_free": f"{random.randint(1, 16)}GB",
        "query_id": random.randint(0, 1000),
        "attempts": random.randint(1, 4),
        "exception": random.choice(EXCEPTIONS),
        "reason": random.choice(ERROR_REASONS),
        "file_path": random.choice(FILE_PATHS),
        "error_message": random.choice(ERROR_REASONS),
        "exit_code": random.randint(1, 255),
        "resource_type": random.choice(["memory", "disk", "CPU", "network"]),
        "config_key": random.choice(CONFIG_KEYS),
        "executors": random.randint(1, 20),
    }
    
    # Fill the template
    for key, value in placeholders.items():
        placeholder = "{" + key + "}"
        if placeholder in template:
            template = template.replace(placeholder, str(value))
    
    return template

def generate_log_entry():
    """Generate a random log entry."""
    level = get_random_log_level()
    component = random.choice(COMPONENTS)
    host = random.choice(HOSTS)
    
    message_template = get_random_message(level)
    message = fill_template(message_template)
    
    return format_log_entry(level, component, message, host)

def job_status_updater():
    """Update job statuses periodically."""
    global current_job_id, current_stage_id, current_task_id
    
    while running:
        # Update job statuses
        for _ in range(random.randint(0, 3)):
            try:
                # Try to get a job
                if not active_jobs.empty():
                    job = active_jobs.get(block=False)
                    
                    # Update job status
                    r = random.random()
                    if r < 0.1:  # 10% chance of job completing
                        job["status"] = "SUCCEEDED" if random.random() < 0.9 else "FAILED"
                        # Log job completion
                        level = "INFO" if job["status"] == "SUCCEEDED" else "ERROR"
                        component = "JobScheduler"
                        message = f"Job {job['id']} {job['status'].lower()} in {random.randint(10, 600)} seconds"
                        log_entry = format_log_entry(level, component, message)
                        print(log_entry)
                        
                        # Submit a new job
                        new_job = {
                            "id": current_job_id,
                            "name": f"{random.choice(JOB_TYPES)}Job",
                            "status": "RUNNING",
                            "start_time": datetime.now(),
                            "stages": random.randint(3, 15),
                            "tasks": random.randint(20, 500)
                        }
                        active_jobs.put(new_job)
                        current_job_id += 1
                        
                        # Log job submission
                        log_entry = format_log_entry("INFO", "JobScheduler", f"Submitting job {new_job['id']} with {new_job['tasks']} tasks")
                        print(log_entry)
                    else:
                        # Put the job back
                        active_jobs.put(job)
            except queue.Empty:
                pass
        
        # Update stage statuses
        for _ in range(random.randint(0, 5)):
            try:
                if not active_stages.empty():
                    stage = active_stages.get(block=False)
                    
                    # Update stage status
                    r = random.random()
                    if r < 0.2:  # 20% chance of stage completing
                        stage["status"] = "SUCCEEDED" if random.random() < 0.9 else "FAILED"
                        
                        # Log stage completion
                        level = "INFO" if stage["status"] == "SUCCEEDED" else "ERROR"
                        component = "DAGScheduler"
                        message = f"Stage {stage['id']} {stage['status'].lower()} in {random.randint(5, 300)} seconds"
                        log_entry = format_log_entry(level, component, message)
                        print(log_entry)
                        
                        # Submit a new stage
                        new_stage = {
                            "id": current_stage_id,
                            "job_id": random.randint(0, current_job_id - 1),
                            "tasks": random.randint(10, 100),
                            "status": "RUNNING",
                            "start_time": datetime.now()
                        }
                        active_stages.put(new_stage)
                        current_stage_id += 1
                        
                        # Log stage submission
                        log_entry = format_log_entry("INFO", "DAGScheduler", f"Submitting stage {new_stage['id']} with {new_stage['tasks']} tasks for job {new_stage['job_id']}")
                        print(log_entry)
                    else:
                        # Put the stage back
                        active_stages.put(stage)
            except queue.Empty:
                pass
        
        # Update task statuses
        for _ in range(random.randint(0, 20)):
            try:
                if not active_tasks.empty():
                    task = active_tasks.get(block=False)
                    
                    # Update task status
                    r = random.random()
                    if r < 0.3:  # 30% chance of task completing
                        task["status"] = "SUCCEEDED" if random.random() < 0.9 else "FAILED"
                        
                        # Log task completion
                        level = "INFO" if task["status"] == "SUCCEEDED" else "ERROR"
                        component = "TaskSetManager"
                        
                        if task["status"] == "SUCCEEDED":
                            message = f"Task {task['id']} finished in {random.randint(100, 30000)} ms on executor {task['executor_id']}"
                        else:
                            error_reason = random.choice(ERROR_REASONS)
                            message = f"Task {task['id']} failed on executor {task['executor_id']}: {error_reason}"
                        
                        log_entry = format_log_entry(level, component, message)
                        print(log_entry)
                        
                        # Submit a new task
                        new_task = {
                            "id": current_task_id,
                            "stage_id": random.randint(0, current_stage_id - 1),
                            "executor_id": random.randint(1, 20),
                            "status": "RUNNING",
                            "start_time": datetime.now()
                        }
                        active_tasks.put(new_task)
                        current_task_id += 1
                        
                        # Log task submission
                        log_entry = format_log_entry("INFO", "TaskScheduler", f"Launching task {new_task['id']} on executor {new_task['executor_id']}")
                        print(log_entry)
                    else:
                        # Put the task back
                        active_tasks.put(task)
            except queue.Empty:
                pass
        
        # Update executor statuses
        for executor_id in list(executors.keys()):
            if random.random() < 0.005:  # 0.5% chance of executor failing
                if executors[executor_id]["status"] == "ALIVE":
                    executors[executor_id]["status"] = "DEAD"
                    reason = random.choice(ERROR_REASONS)
                    log_entry = format_log_entry("ERROR", "ExecutorAllocationManager", f"Executor {executor_id} on {executors[executor_id]['host']} lost: {reason}")
                    print(log_entry)
                    
                    # Add a new executor
                    new_executor_id = max(executors.keys()) + 1
                    executors[new_executor_id] = {
                        "host": random.choice(HOSTS),
                        "cores": random.choice([2, 4, 8, 16]),
                        "memory": random.choice([4096, 8192, 16384, 32768]),
                        "tasks": 0,
                        "status": "ALIVE"
                    }
                    log_entry = format_log_entry("INFO", "ExecutorAllocationManager", f"Launched executor {new_executor_id} on {executors[new_executor_id]['host']} with {executors[new_executor_id]['cores']} cores")
                    print(log_entry)
        
        # Sleep for a random interval
        time.sleep(random.uniform(1, 5))

def generate_special_log():
    """Generate special logs like startup, shutdown, or periodic events."""
    r = random.random()
    
    if r < 0.2:  # Startup logs
        spark_version = random.choice(["3.0.3", "3.1.2", "3.2.1", "3.3.0", "3.4.0"])
        scala_version = random.choice(["2.12.10", "2.12.15", "2.13.5"])
        java_version = random.choice(["1.8.0_302", "11.0.12", "11.0.15", "17.0.1"])
        
        logs = [
            format_log_entry("INFO", "SparkContext", f"Running Spark version {spark_version}"),
            format_log_entry("INFO", "SparkContext", f"Submitted application: {app_id}"),
            format_log_entry("INFO", "SparkContext", f"Spark configuration: spark.app.name={random.choice(JOB_TYPES)}Application"),
            format_log_entry("INFO", "SparkContext", f"Spark configuration: spark.master=yarn"),
            format_log_entry("INFO", "SparkContext", f"Spark configuration: spark.executor.memory={random.choice([4, 8, 16, 32])}g"),
            format_log_entry("INFO", "SparkContext", f"Using Scala version {scala_version}, Java version {java_version}")
        ]
        return logs
    
    elif r < 0.4:  # Metrics logs
        memory_usage = random.randint(10, 80)
        disk_usage = random.randint(10, 90)
        cpu_usage = random.randint(5, 95)
        active_executors = sum(1 for e in executors.values() if e["status"] == "ALIVE")
        
        logs = [
            format_log_entry("INFO", "MetricsSystem", f"Cluster metrics: {memory_usage}% memory usage, {disk_usage}% disk usage, {cpu_usage}% CPU usage"),
            format_log_entry("INFO", "MetricsSystem", f"Active executors: {active_executors}, Total memory: {sum(e['memory'] for e in executors.values() if e['status'] == 'ALIVE')}MB"),
            format_log_entry("INFO", "MetricsSystem", f"Active tasks: {sum(e['tasks'] for e in executors.values() if e['status'] == 'ALIVE')}")
        ]
        return logs
    
    elif r < 0.6:  # Configuration logs
        config_changes = random.randint(1, 3)
        logs = []
        for _ in range(config_changes):
            config_key = random.choice(CONFIG_KEYS)
            config_value = random.choice(["true", "false", str(random.randint(1, 1000)), f"{random.randint(1, 100)}g"])
            logs.append(format_log_entry("INFO", "SparkContext", f"Updated configuration: {config_key}={config_value}"))
        return logs
    
    else:  # Periodic status logs
        active_jobs_count = active_jobs.qsize()
        active_stages_count = active_stages.qsize()
        active_tasks_count = active_tasks.qsize()
        
        logs = [
            format_log_entry("INFO", "SparkContext", f"Application status: {active_jobs_count} active jobs, {active_stages_count} active stages, {active_tasks_count} active tasks"),
            format_log_entry("INFO", "SparkContext", f"Application running for {(datetime.now() - app_start_time).total_seconds():.0f} seconds")
        ]
        return logs

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global running
    print(f"\n{COLORS['INFO']}Received shutdown signal. Cleaning up...{COLORS['RESET']}")
    running = False

def main():
    """Main function to run the log generator."""
    global args
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Generate realistic Spark logs')
    parser.add_argument('--color', action='store_true', help='Enable colored output')
    parser.add_argument('--interval', type=float, default=0.1, help='Interval between log entries in seconds')
    args = parser.parse_args()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start the job status updater thread
    status_thread = threading.Thread(target=job_status_updater)
    status_thread.daemon = True
    status_thread.start()
    
    print(f"{COLORS['INFO']}Starting Spark log generator...{COLORS['RESET']}")
    print(f"{COLORS['INFO']}Application ID: {app_id}{COLORS['RESET']}")
    
    try:
        while running:
            # Generate regular log entries
            if random.random() < 0.8:  # 80% chance of regular log
                print(generate_log_entry())
            else:  # 20% chance of special log
                for log in generate_special_log():
                    print(log)
            
            time.sleep(args.interval)
    
    except KeyboardInterrupt:
        print(f"\n{COLORS['INFO']}Shutting down gracefully...{COLORS['RESET']}")
    finally:
        print(f"{COLORS['INFO']}Log generator stopped.{COLORS['RESET']}")

if __name__ == "__main__":
    main()