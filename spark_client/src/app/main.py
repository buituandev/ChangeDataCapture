"""
Main entry point for the CDC Stream Processing application.

This module sets up and runs the Spark Structured Streaming application
for processing Change Data Capture events from Kafka.
"""

import os
import time
import argparse
import logging
from typing import Optional

from pyspark.sql import SparkSession

from .config.config_manager import ConfigManager
from .processors.processor_factory import ProcessorFactory


class CDCStreamingApp:
    """
    CDC Streaming Application.
    
    This class sets up and runs the Spark Structured Streaming application
    for processing Change Data Capture events from Kafka.
    """
    
    def __init__(self, config_path: str, logger: Optional[logging.Logger] = None):
        """
        Initialize the CDC Streaming Application.
        
        Args:
            config_path: Path to the configuration file
            logger: Optional logger instance
        """
        self.config_path = config_path
        self.logger = logger or self._setup_logger()
        self.config_manager = ConfigManager(config_path, logger=self.logger)
        self.spark = self._setup_spark()
        self.processor = None
        self.query = None
        
        # State tracking
        self.restart_required = False
        self.batch_in_progress = False
        self.process_time = self.config_manager.get("processing_config", "process_time")
    
    def _setup_logger(self) -> logging.Logger:
        """Set up a logger for this class."""
        logger = logging.getLogger("CDCStreamingApp")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _setup_spark(self) -> SparkSession:
        """
        Set up and configure the SparkSession.
        
        Returns:
            Configured SparkSession
        """
        # Get S3/MinIO configuration
        s3_config = {
            "access_key": self.config_manager.get("s3_config", "access_key_id"),
            "secret_key": self.config_manager.get("s3_config", "secret_access_key"),
            "endpoint": self.config_manager.get("s3_config", "endpoint"),
            "path_style_access": str(self.config_manager.get("s3_config", "path_style_access")).lower(),
            "ssl_enabled": str(self.config_manager.get("s3_config", "ssl_enabled")).lower()
        }
        
        # Configure and build SparkSession
        spark = SparkSession.builder \
            .appName(f"CDC-Stream-{os.path.basename(self.config_path)}") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0,io.delta:delta-core_2.12:2.4.0") \
            .config("spark.hadoop.fs.s3a.access.key", s3_config["access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", s3_config["secret_key"]) \
            .config("spark.hadoop.fs.s3a.path.style.access", s3_config["path_style_access"]) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", s3_config["endpoint"]) \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_config["ssl_enabled"]) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.cores.max", 1) \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel('ERROR')
        
        return spark
    
    def _create_streaming_query(self):
        """
        Create and start a streaming query.
        
        Returns:
            pyspark.sql.streaming.StreamingQuery: The running streaming query
        """
        # Get Kafka configuration
        kafka_config = {
            "bootstrap_servers": self.config_manager.get("kafka_config", "bootstrap_servers"),
            "topic": self.config_manager.get("kafka_config", "topic"),
            "fail_on_data_loss": str(self.config_manager.get("kafka_config", "fail_on_data_loss")).lower()
        }
        
        # Get checkpoint directory
        checkpoint_dir = self.config_manager.get("delta_config", "checkpoint_dir")
        
        # Create processor if not already created
        if self.processor is None:
            self.processor = ProcessorFactory.create_processor(
                spark=self.spark,
                config_manager=self.config_manager,
                logger=self.logger
            )
        
        # Define batch processing function
        def managed_batch_processing(dataframe, batch_id):
            try:
                self.batch_in_progress = True
                current_process_time = self.process_time
                
                # Process the batch
                self.processor.process_batch(dataframe, batch_id)
                
                # Check if process time changed
                if self.process_time != current_process_time:
                    self.logger.info(f"Process time changed from {current_process_time} to {self.process_time}")
                    self.restart_required = True
            finally:
                self.batch_in_progress = False
        
        # Create and return the streaming query
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
            .option("subscribe", kafka_config["topic"]) \
            .option("failOnDataLoss", kafka_config["fail_on_data_loss"]) \
            .load() \
            .selectExpr("CAST(value AS STRING) as value")
        
        # Start the streaming query
        query = df.writeStream \
            .foreachBatch(managed_batch_processing) \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(processingTime=self.process_time) \
            .start()
        
        return query
    
    def run(self):
        """
        Run the streaming application.
        
        This method starts the streaming query and manages its lifecycle,
        including handling configuration changes and graceful shutdown.
        """
        self.logger.info("Starting CDC Streaming Application")
        self.logger.info(f"Using configuration from: {self.config_path}")
        
        # Create and start the query
        self.query = self._create_streaming_query()
        self.logger.info(f"Started streaming query with process time: {self.process_time}")
        
        try:
            # Main loop to monitor and manage the query
            while True:
                time.sleep(1)
                
                # Check if configuration was reloaded
                new_process_time = self.config_manager.get("processing_config", "process_time")
                if new_process_time != self.process_time:
                    self.process_time = new_process_time
                    self.restart_required = True
                
                # Restart the query if needed and safe to do so
                if self.restart_required and not self.batch_in_progress:
                    status = self.query.status
                    if status["isTriggerActive"] == False:
                        self.logger.info("Restarting query with new process time...")
                        self.query.stop()
                        self.query = self._create_streaming_query()
                        self.restart_required = False
                        self.logger.info(f"Query restarted with process time: {self.process_time}")
        
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal. Stopping stream processing...")
            self.logger.info("Waiting for any in-progress batch to complete before stopping...")
            
            # Wait for any in-progress batch to complete
            while self.batch_in_progress:
                time.sleep(1)
            
            # Stop the query
            try:
                if self.query and self.query.isActive:
                    self.query.stop()
                    self.logger.info("Query stopped")
            except Exception as e:
                self.logger.error(f"Error while stopping query: {str(e)}")
            
            self.logger.info("CDC Streaming Application stopped")


def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Spark Structured Streaming CDC application')
    parser.add_argument('--config', type=str, default="/opt/src/config.json",
                        help='Path to configuration file')
    return parser.parse_args()


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    # Create and run the application
    app = CDCStreamingApp(args.config)
    app.run() 