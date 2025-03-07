#!/filepath: main.py
"""
Main module for the Finance Dashboard application.

This module serves as the entry point for the Finance Dashboard application,
orchestrating the data loading, processing, and visualization components.
"""

import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import polars as pl

from src.Config import Config
from src.DataWrangler import DataWrangler
from src.Process import Process
from src.FinanceDashboard import FinanceDashboard
from src.logger import create_logger


def setup_directories():
    """Create necessary directories for the application."""
    os.makedirs("input", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("output", exist_ok=True)


def log_error(logger, error_message, traceback_str):
    """
    Log errors with traceback information.

    Args:
        logger: The logger to use
        error_message: The error message to log
        traceback_str: The traceback string to include
    """
    logger.error(f"Error occurred: {error_message}")
    logger.error(f"Traceback:\n{traceback_str}")


def load_from_cache(file_path, logger, message=None):
    """
    Try to load data from a cached CSV file.

    Args:
        file_path: Path to the CSV file
        logger: The logger to use
        message: Optional message to log on successful load

    Returns:
        pl.DataFrame or None: DataFrame if loaded successfully, None otherwise
    """
    if os.path.exists(file_path):
        if message:
            logger.info(message)
        try:
            return pl.read_csv(file_path)
        except Exception as e:
            logger.error(f"Error loading cached data from {file_path}: {e}")
    return None


def load_data(data_wrangler, config, logger):
    """
    Load data from Numbers file or cached CSVs.

    Args:
        data_wrangler: The DataWrangler instance
        config: Configuration object
        logger: The logger to use

    Returns:
        dict: Dictionary of raw DataFrames

    Raises:
        ValueError: If essential data cannot be loaded
    """
    # Try to load data from Numbers file
    logger.info("Loading data from Numbers file...")
    try:
        raw_dfs = data_wrangler.load_updated_file()
        logger.info("Successfully loaded data from Numbers file")

        # Save skipped rows report if any rows were skipped
        if data_wrangler.skipped_rows:
            skipped_rows_path = data_wrangler.save_skipped_rows_report()
            logger.warning(
                f"Skipped {len(data_wrangler.skipped_rows)} rows during import. "
                f"Report saved to {skipped_rows_path}"
            )

        return raw_dfs

    except Exception as e:
        logger.warning(f"Error loading data from Numbers file: {e}")
        logger.info("Attempting to load data from cached CSV files...")

        # Try to load from cached files
        raw_dfs = {}
        raw_dfs["expenses"] = load_from_cache(
            config.get("raw_expenses_path"), logger, "Loading expenses from cache"
        )
        raw_dfs["income"] = load_from_cache(
            config.get("raw_income_path"), logger, "Loading income from cache"
        )
        raw_dfs["savings"] = load_from_cache(
            config.get("raw_savings_path"), logger, "Loading savings from cache"
        )

        # Check if we have the essential data
        if raw_dfs["expenses"] is None or raw_dfs["income"] is None:
            raise ValueError(
                "Could not load essential data from either Numbers file or cache"
            )

        return raw_dfs


def process_data(process, raw_dfs, config, logger):
    """
    Process the raw data into clean DataFrames.

    Args:
        process: The Process instance
        raw_dfs: Dictionary of raw DataFrames
        config: Configuration object
        logger: The logger to use

    Returns:
        tuple: Processed expenses, income, and savings DataFrames
    """
    # Process expenses and income data
    logger.info("Processing expenses data...")
    df_expenses = process.process_expense_income_data(raw_dfs["expenses"], "expenses")
    logger.info(f"Processed {len(df_expenses)} expense records")

    logger.info("Processing income data...")
    df_income = process.process_expense_income_data(raw_dfs["income"], "income")
    logger.info(f"Processed {len(df_income)} income records")

    # Save processed data
    processed_expenses_path = config.get("processed_expenses_path")
    processed_income_path = config.get("processed_income_path")

    if processed_expenses_path:
        df_expenses.write_csv(processed_expenses_path)
        logger.info(f"Saved processed expenses to {processed_expenses_path}")

    if processed_income_path:
        df_income.write_csv(processed_income_path)
        logger.info(f"Saved processed income to {processed_income_path}")

    # Process savings data if available
    df_savings = None
    if "savings" in raw_dfs and raw_dfs["savings"] is not None:
        logger.info("Processing savings data...")
        df_savings = process.process_savings_data(raw_dfs["savings"])
        logger.info(f"Processed {len(df_savings)} savings records")

        # Save processed savings data
        processed_savings_path = config.get("processed_savings_path")

        if processed_savings_path:
            df_savings.write_csv(processed_savings_path)
            logger.info(f"Saved processed savings to {processed_savings_path}")
    else:
        logger.info("No savings data available, checking for cached processed data...")
        df_savings = load_from_cache(
            config.get("processed_savings_path"),
            logger,
            "Loading processed savings data from cache",
        )

    return df_expenses, df_income, df_savings


def main():
    """
    Main application entry point.

    This function orchestrates the following workflow:
    1. Load configuration
    2. Initialize data components
    3. Load and process financial data
    4. Generate all visualization datasets
    5. Launch the dashboard

    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    # Setup directories
    setup_directories()

    # Initialize the application logger
    logger = create_logger("finance_dashboard")
    logger.info("Starting Finance Dashboard application...")

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), "src/config.yaml")
        logger.info(f"Loading configuration from {config_path}")
        config = Config(config_path)

        # Initialize data wrangler and process components with logger
        data_wrangler = DataWrangler(config, logger)
        process = Process(config, logger)

        # Load data
        raw_dfs = load_data(data_wrangler, config, logger)

        # Process data
        df_expenses, df_income, df_savings = process_data(
            process, raw_dfs, config, logger
        )

        # Generate all intermediate datasets for visualization
        logger.info("Generating visualization datasets...")
        process.generate_all_datasets(df_expenses, df_income, df_savings)
        logger.info("Visualization datasets generated successfully")

        # Initialize and run dashboard
        logger.info("Initializing dashboard...")
        dashboard = FinanceDashboard(config, logger)
        logger.info("Starting dashboard server...")
        dashboard.run_server(debug=True)

    except Exception as e:
        error_traceback = traceback.format_exc()
        log_error(logger, str(e), error_traceback)
        logger.critical(
            "The application has encountered a critical error and cannot continue."
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
