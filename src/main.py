#!/filepath: main.py
"""
Main module for the Finance Dashboard application.

This module serves as the entry point for the Finance Dashboard application,
orchestrating the data loading, processing, and visualization components.
"""

import logging
import os
import sys
import traceback
from pathlib import Path

import polars as pl

from config import Config
from data_wrangler import DataWrangler
from FinanceDashboard import FinanceDashboard
from logger import create_logger
from Process import Process


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
        result = data_wrangler.load_updated_file()
        logger.info("Successfully loaded data from Numbers file")

        # Save skipped rows report if any rows were skipped
        if result.skipped_rows:
            skipped_rows_path = data_wrangler.save_skipped_rows_report(
                result.skipped_rows
            )
            logger.warning(
                f"Skipped {len(result.skipped_rows)} rows during import. "
                f"Report saved to {skipped_rows_path}"
            )

        return result.dataframes

    except Exception as e:
        logger.error(f"Error loading data from Numbers file: {e}")
        logger.info("Attempting to load data from cached CSV files...")

        # Try to load from cached files
        raw_dfs = {}
        raw_paths = config.get("raw_paths")
        raw_dfs["expenses"] = load_from_cache(
            raw_paths.get("expenses"), logger, "Loading expenses from cache"
        )
        raw_dfs["income"] = load_from_cache(
            raw_paths.get("income"), logger, "Loading income from cache"
        )
        raw_dfs["savings"] = load_from_cache(
            raw_paths.get("savings"), logger, "Loading savings from cache"
        )

        # Check if we have the essential data
        if raw_dfs["expenses"] is None or raw_dfs["income"] is None:
            logger.critical(
                "Could not load essential data from either Numbers file or cache"
            )
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
        try:
            df_expenses.write_csv(processed_expenses_path)
            logger.info(f"Saved processed expenses to {processed_expenses_path}")
        except Exception as e:
            logger.error(f"Failed to save processed expenses: {e}")

    if processed_income_path:
        try:
            df_income.write_csv(processed_income_path)
            logger.info(f"Saved processed income to {processed_income_path}")
        except Exception as e:
            logger.error(f"Failed to save processed income: {e}")

    # Process savings data if available
    df_savings = None
    if "savings" in raw_dfs and raw_dfs["savings"] is not None:
        logger.info("Processing savings data...")
        df_savings = process.process_savings_data(raw_dfs["savings"])
        logger.info(f"Processed {len(df_savings)} savings records")

        # Save processed savings data
        processed_savings_path = config.get("processed_savings_path")

        if processed_savings_path:
            try:
                df_savings.write_csv(processed_savings_path)
                logger.info(f"Saved processed savings to {processed_savings_path}")
            except Exception as e:
                logger.error(f"Failed to save processed savings: {e}")
    else:
        logger.warning(
            "No savings data available, checking for cached processed data..."
        )
        df_savings = load_from_cache(
            config.get("processed_savings_path"),
            logger,
            "Loading processed savings data from cache",
        )
        if df_savings is None:
            logger.warning("No cached savings data found")

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
    logger = create_logger("finance_dashboard", level=logging.WARNING)
    logger.info("Starting Finance Dashboard application...")

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        logger.info(f"Loading configuration from {config_path}")

        try:
            config = Config(config_path)
        except FileNotFoundError:
            logger.critical(f"Configuration file not found at {config_path}")
            return 1
        except Exception as e:
            logger.critical(f"Failed to load configuration: {e}")
            return 1

        # Initialize data wrangler and process components with logger
        data_wrangler = DataWrangler(config, logger)
        process = Process(config, logger)

        # Load data
        try:
            raw_dfs = load_data(data_wrangler, config, logger)
        except ValueError as e:
            # Error already logged in load_data
            return 1

        # Process data
        try:
            df_expenses, df_income, df_savings = process_data(
                process, raw_dfs, config, logger
            )
        except Exception as e:
            error_traceback = traceback.format_exc()
            log_error(logger, f"Failed to process data: {str(e)}", error_traceback)
            return 1

        # Generate all intermediate datasets for visualization
        logger.info("Generating visualization datasets...")
        try:
            process.generate_all_datasets(df_expenses, df_income, df_savings)
            logger.info("Visualization datasets generated successfully")
        except Exception as e:
            error_traceback = traceback.format_exc()
            log_error(
                logger,
                f"Failed to generate visualization datasets: {str(e)}",
                error_traceback,
            )
            logger.error(
                "Continuing with dashboard initialization, but some features may not work properly"
            )

        # Initialize and run dashboard
        logger.info("Initializing dashboard...")
        try:
            dashboard = FinanceDashboard(config, logger)
            logger.info("Starting dashboard server...")
            dashboard.run_server(debug=True)
        except Exception as e:
            error_traceback = traceback.format_exc()
            log_error(logger, f"Failed to start dashboard: {str(e)}", error_traceback)
            return 1

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
