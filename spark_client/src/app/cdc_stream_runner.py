#!/usr/bin/env python3
"""
Command-line wrapper for the CDC Stream Processing application.

This script provides a simple command-line interface to run the CDC Streaming Application.
"""

import argparse
import logging
import sys
import os
import time
from typing import Dict, Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import CDCStreamingApp


def setup_logger() -> logging.Logger:
    """Set up the application logger."""
    logger = logging.getLogger("CDCStreamRunner")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def parse_args() -> Dict[str, Any]:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='CDC Stream Processing Application')
    parser.add_argument('--config', type=str, default="/opt/src/config.json",
                        help='Path to configuration file')
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Set logging level')
    return vars(parser.parse_args())


def main():
    """Main entry point for the application."""
    # Parse command line arguments
    args = parse_args()
    
    # Set up logger
    logger = setup_logger()
    log_level = getattr(logging, args['log_level'])
    logger.setLevel(log_level)
    
    # Log startup information
    logger.info("Starting CDC Stream Processing Application")
    logger.info(f"Using configuration file: {args['config']}")
    
    try:
        # Create and run the application
        start_time = time.time()
        app = CDCStreamingApp(args['config'], logger=logger)
        app.run()
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.exception(f"Application failed: {str(e)}")
        sys.exit(1)
    finally:
        end_time = time.time()
        runtime = end_time - start_time
        logger.info(f"Application runtime: {runtime:.2f} seconds")
    
    logger.info("Application exited successfully")
    sys.exit(0)


if __name__ == "__main__":
    main() 