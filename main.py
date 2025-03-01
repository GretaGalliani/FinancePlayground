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
from src.logger import logger


def setup_directories():
    """Create necessary directories for the application."""
    os.makedirs("input", exist_ok=True)
    os.makedirs("logs", exist_ok=True)


def log_error(error_message, traceback_str):
    """
    Log errors to a file with timestamp.

    Args:
        error_message: The error message to log
        traceback_str: The traceback string to include
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/error_{timestamp}.log"

    with open(log_file, "w") as f:
        f.write(f"Error occurred at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Error message: {error_message}\n\n")
        f.write("Traceback:\n")
        f.write(traceback_str)

    print(f"Error details have been logged to {log_file}")


def load_from_cache(file_path, message=None):
    """
    Try to load data from a cached CSV file.

    Args:
        file_path: Path to the CSV file
        message: Optional message to print on successful load

    Returns:
        pl.DataFrame or None: DataFrame if loaded successfully, None otherwise
    """
    if os.path.exists(file_path):
        if message:
            print(message)
        try:
            return pl.read_csv(file_path)
        except Exception as e:
            print(f"Error loading cached data from {file_path}: {e}")
    return None


def main():
    """
    Main application entry point.

    This function orchestrates the following workflow:
    1. Load configuration
    2. Initialize data components
    3. Load and process financial data
    4. Launch the dashboard

    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    print("Starting Finance Dashboard application...")
    setup_directories()

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), "src/config.yaml")
        print(f"Loading configuration from {config_path}")
        config = Config(config_path)

        # Initialize data wrangler and process components
        data_wrangler = DataWrangler(config)
        process = Process(config)

        # Try to load data from Numbers file
        print("Loading data from Numbers file...")
        try:
            raw_dfs = data_wrangler.load_updated_file()
            print("Successfully loaded data from Numbers file")
        except Exception as e:
            print(f"Error loading data from Numbers file: {e}")
            print("Attempting to load data from cached CSV files...")

            # Try to load from cached files
            raw_dfs = {}
            raw_dfs["expenses"] = load_from_cache(
                config.get("raw_expenses_path"), "Loading expenses from cache"
            )
            raw_dfs["income"] = load_from_cache(
                config.get("raw_income_path"), "Loading income from cache"
            )
            raw_dfs["savings"] = load_from_cache(
                config.get("raw_savings_path"), "Loading savings from cache"
            )

            # Check if we have the essential data
            if raw_dfs["expenses"] is None or raw_dfs["income"] is None:
                raise ValueError(
                    "Could not load essential data from either Numbers file or cache"
                )

        # Process expenses and income data
        print("Processing expenses data...")
        df_expenses = process.process_expense_income_data(
            raw_dfs["expenses"], "expenses"
        )
        print(f"Processed {len(df_expenses)} expense records")

        print("Processing income data...")
        df_income = process.process_expense_income_data(raw_dfs["income"], "income")
        print(f"Processed {len(df_income)} income records")

        # Save processed data
        processed_expenses_path = config.get("processed_expenses_path")
        processed_income_path = config.get("processed_income_path")

        if processed_expenses_path:
            df_expenses.write_csv(processed_expenses_path)
            print(f"Saved processed expenses to {processed_expenses_path}")

        if processed_income_path:
            df_income.write_csv(processed_income_path)
            print(f"Saved processed income to {processed_income_path}")

        # Process savings data if available
        df_savings = None
        df_savings_monthly = None

        if "savings" in raw_dfs and raw_dfs["savings"] is not None:
            print("Processing savings data...")
            df_savings = process.process_savings_data(raw_dfs["savings"])
            print(f"Processed {len(df_savings)} savings records")

            print("Calculating monthly savings totals...")
            df_savings_monthly = process.calculate_savings_totals(df_savings)
            print(
                f"Generated monthly savings summary for {len(df_savings_monthly.select(pl.col('Month').unique()))} months"
            )

            # Save processed savings data
            processed_savings_path = config.get("processed_savings_path")
            monthly_savings_path = config.get("monthly_savings_path")

            if processed_savings_path:
                df_savings.write_csv(processed_savings_path)
                print(f"Saved processed savings to {processed_savings_path}")

            if monthly_savings_path:
                df_savings_monthly.write_csv(monthly_savings_path)
                print(f"Saved monthly savings summary to {monthly_savings_path}")
        else:
            print("No savings data available, checking for cached processed data...")
            df_savings = load_from_cache(
                config.get("processed_savings_path"),
                "Loading processed savings data from cache",
            )
            df_savings_monthly = load_from_cache(
                config.get("monthly_savings_path"),
                "Loading monthly savings data from cache",
            )

        # Initialize and run dashboard
        print("Initializing dashboard...")
        dashboard = FinanceDashboard(
            df_expenses, df_income, df_savings, df_savings_monthly
        )
        print("Starting dashboard server...")
        dashboard.run_server(debug=True)

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Critical error in main workflow: {e}")
        print(error_traceback)
        log_error(str(e), error_traceback)
        print("The application has encountered a critical error and cannot continue.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
