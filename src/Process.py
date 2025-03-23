#!/filepath: src/process.py
"""
Process module for standardizing and transforming financial data.

This module handles the transformation of raw financial data into standardized
formats suitable for analysis and visualization in the dashboard. It also
generates all intermediate datasets required for visualization.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, cast

import polars as pl
import pydantic
from pydantic import BaseModel, Field, validator

from config import Config


class FinancialRecord(BaseModel):
    """Base model for financial records validation."""

    Date: datetime
    Description: str
    Category: str
    Value: float

    class Config:
        """Pydantic configuration."""

        arbitrary_types_allowed = True


class SavingsRecord(FinancialRecord):
    """Model for savings records validation."""

    CategoryType: str


@dataclass
class ProcessingStats:
    """Statistics from data processing operations."""

    source_rows: int
    processed_rows: int
    invalid_rows: int = 0
    skipped_categories: List[str] = None
    errors: List[str] = None

    def __post_init__(self) -> None:
        """Initialize default values for lists."""
        if self.skipped_categories is None:
            self.skipped_categories = []
        if self.errors is None:
            self.errors = []


class SchemaValidator:
    """
    Validates and converts financial data to standard schema.

    This class is responsible for validating raw data against defined schemas
    and converting it to standardized formats.
    """

    def __init__(self, logger: logging.Logger):
        """
        Initialize the schema validator.

        Args:
            logger: Logger instance for logging validation events
        """
        self.logger = logger.getChild("SchemaValidator")

    def validate_expense_income(
        self, df: pl.DataFrame
    ) -> Tuple[pl.DataFrame, List[str]]:
        """
        Validate expense/income data against schema.

        Args:
            df: Raw expense or income DataFrame

        Returns:
            Tuple containing:
                - pl.DataFrame: Validated DataFrame
                - List[str]: List of validation errors
        """
        errors = []
        validated_rows = []

        for row in df.iter_rows(named=True):
            try:
                # Convert to dict and validate with pydantic
                validated = FinancialRecord(
                    Date=row["Date"],
                    Description=row["Description"],
                    Category=row["Category"],
                    Value=float(row["Value"]),
                )
                validated_rows.append(validated.dict())
            except pydantic.ValidationError as e:
                errors.append(f"Row validation error: {e}")
                self.logger.warning(f"Validation error for row: {row}, error: {e}")

        if not validated_rows:
            self.logger.error("No valid rows after validation")
            return pl.DataFrame(), errors

        return pl.DataFrame(validated_rows), errors

    def validate_savings(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, List[str]]:
        """
        Validate savings data against schema.

        Args:
            df: Raw savings DataFrame

        Returns:
            Tuple containing:
                - pl.DataFrame: Validated DataFrame
                - List[str]: List of validation errors
        """
        errors = []
        validated_rows = []

        for row in df.iter_rows(named=True):
            try:
                # Convert to dict and validate with pydantic
                validated = SavingsRecord(
                    Date=row["Date"],
                    Description=row["Description"],
                    Category=row["Category"],
                    CategoryType=row["CategoryType"],
                    Value=float(row["Value"]),
                )
                validated_rows.append(validated.dict())
            except pydantic.ValidationError as e:
                errors.append(f"Row validation error: {e}")
                self.logger.warning(f"Validation error for row: {row}, error: {e}")

        if not validated_rows:
            self.logger.error("No valid rows after validation")
            return pl.DataFrame(), errors

        return pl.DataFrame(validated_rows), errors


class DataTransformer:
    """
    Transforms financial data between different formats.

    This class is responsible for converting data between different formats
    and applying transformations to prepare data for analysis.
    """

    def __init__(self, config: Config, logger: logging.Logger):
        """
        Initialize the data transformer.

        Args:
            config: Application configuration
            logger: Logger instance for logging transformation events
        """
        self.config = config
        self.logger = logger.getChild("DataTransformer")

    def standardize_date_format(
        self, df: pl.DataFrame, date_col: str = "Date"
    ) -> pl.DataFrame:
        """
        Standardize date format in the DataFrame.

        Args:
            df: DataFrame to process
            date_col: Name of the date column

        Returns:
            pl.DataFrame: DataFrame with standardized date column
        """
        if date_col not in df.columns:
            self.logger.warning(f"Date column '{date_col}' not found in DataFrame")
            return df

        # Handle different date formats
        if df[date_col].dtype == pl.Utf8:
            try:
                # Try multiple date formats
                if any(
                    row and "/" in row for row in df[date_col] if isinstance(row, str)
                ):
                    return df.with_columns(
                        pl.col(date_col)
                        .str.strptime(pl.Datetime, "%d/%m/%y")
                        .alias(date_col)
                    )
                elif any(
                    row and "-" in row for row in df[date_col] if isinstance(row, str)
                ):
                    return df.with_columns(
                        pl.col(date_col)
                        .str.strptime(pl.Datetime, "%Y-%m-%d")
                        .alias(date_col)
                    )
                else:
                    # General conversion
                    return df.with_columns(
                        pl.col(date_col).str.to_datetime().alias(date_col)
                    )
            except Exception as e:
                self.logger.error(f"Error converting date column: {e}")
                # Fallback to general conversion
                return df.with_columns(
                    pl.col(date_col).str.to_datetime().alias(date_col)
                )
        elif df[date_col].dtype == pl.Date:
            # Convert Date to Datetime
            return df.with_columns(pl.col(date_col).cast(pl.Datetime).alias(date_col))

        return df

    def normalize_categories(
        self, df: pl.DataFrame, valid_categories: List[str], default_category: str
    ) -> pl.DataFrame:
        """
        Normalize categories in the DataFrame.

        Args:
            df: DataFrame to process
            valid_categories: List of valid category names
            default_category: Default category to use for invalid categories

        Returns:
            pl.DataFrame: DataFrame with normalized categories
        """
        if "Category" not in df.columns:
            self.logger.warning("No Category column found in DataFrame")
            return df

        # Check for null categories
        null_count = df.filter(pl.col("Category").is_null()).height
        if null_count > 0:
            self.logger.warning(
                f"Found {null_count} records with null categories. Using default: {default_category}"
            )

        # Replace null or invalid categories with default
        return df.with_columns(
            pl.when(
                pl.col("Category").is_null()
                | ~pl.col("Category").is_in(valid_categories)
            )
            .then(pl.lit(default_category))
            .otherwise(pl.col("Category"))
            .alias("Category")
        )

    def clean_string_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean string columns by removing whitespace.

        Args:
            df: DataFrame to clean

        Returns:
            pl.DataFrame: DataFrame with cleaned string columns
        """
        # Get all string columns
        string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]

        if not string_cols:
            return df

        # Apply string cleaning to all string columns
        return df.with_columns([pl.col(col).str.strip() for col in string_cols])

    def add_month_column(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add a Month column based on the Date column.

        Args:
            df: DataFrame to process

        Returns:
            pl.DataFrame: DataFrame with Month column added
        """
        if "Date" not in df.columns:
            self.logger.warning("No Date column found in DataFrame")
            return df

        return df.with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Month"))


class AnalyticsGenerator:
    """
    Generates analytical datasets for visualization.

    This class is responsible for creating derived datasets for
    visualization and analysis from processed financial data.
    """

    def __init__(self, config: Config, logger: logging.Logger):
        """
        Initialize the analytics generator.

        Args:
            config: Application configuration
            logger: Logger instance for logging analytics events
        """
        self.config = config
        self.logger = logger.getChild("AnalyticsGenerator")
        self.output_folder = config.get("output_folder", "output")
        self._ensure_output_folder()

    def _ensure_output_folder(self) -> None:
        """Create the output folder if it doesn't exist."""
        os.makedirs(self.output_folder, exist_ok=True)

    def monthly_summary(
        self,
        df_expenses: pl.DataFrame,
        df_income: pl.DataFrame,
        start_date: datetime,
        end_date: datetime,
    ) -> pl.DataFrame:
        """
        Generate monthly summary of expenses and income.

        Args:
            df_expenses: Processed expense DataFrame
            df_income: Processed income DataFrame
            start_date: Start date for the summary
            end_date: End date for the summary

        Returns:
            pl.DataFrame: Monthly summary DataFrame
        """
        # Filter by date range
        expenses_filtered = df_expenses.filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )
        income_filtered = df_income.filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        # Calculate monthly expenses
        monthly_expenses = (
            expenses_filtered.with_columns(
                pl.col("Date").dt.strftime("%Y-%m").alias("Month")
            )
            .groupby("Month")
            .agg(pl.sum("Value").alias("Expenses"))
            .sort("Month")
        )

        # Calculate monthly income
        monthly_income = (
            income_filtered.with_columns(
                pl.col("Date").dt.strftime("%Y-%m").alias("Month")
            )
            .groupby("Month")
            .agg(pl.sum("Value").alias("Income"))
            .sort("Month")
        )

        # Join expenses and income
        monthly_summary = monthly_expenses.join(
            monthly_income, on="Month", how="outer"
        ).fill_null(0)

        # Calculate balance
        monthly_summary = monthly_summary.with_columns(
            (pl.col("Income") - pl.col("Expenses")).alias("Balance")
        )

        return monthly_summary

    def category_breakdown(
        self,
        df: pl.DataFrame,
        start_date: datetime,
        end_date: datetime,
        value_column: str = "Value",
    ) -> pl.DataFrame:
        """
        Generate breakdown of values by category.

        Args:
            df: DataFrame to analyze
            start_date: Start date for the analysis
            end_date: End date for the analysis
            value_column: Name of the value column

        Returns:
            pl.DataFrame: Category breakdown DataFrame
        """
        return (
            df.filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))
            .groupby("Category")
            .agg(pl.sum(value_column).alias("Total"))
            .sort("Total", descending=True)
        )

    def time_series_by_category(
        self,
        df: pl.DataFrame,
        start_date: datetime,
        end_date: datetime,
        value_column: str = "Value",
        output_column: str = "Value",
    ) -> pl.DataFrame:
        """
        Generate time series data by category.

        Args:
            df: DataFrame to analyze
            start_date: Start date for the analysis
            end_date: End date for the analysis
            value_column: Name of the input value column
            output_column: Name of the output value column

        Returns:
            pl.DataFrame: Time series DataFrame
        """
        return (
            df.filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))
            .with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Month"))
            .groupby(["Month", "Category"])
            .agg(pl.sum(value_column).alias(output_column))
            .sort(["Month", "Category"])
        )

    def calculate_savings_metrics(self, df_savings: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate savings metrics by month.

        Args:
            df_savings: Processed savings DataFrame

        Returns:
            pl.DataFrame: Savings metrics DataFrame
        """
        if len(df_savings) == 0:
            self.logger.warning("No savings data available for metrics calculation")
            return pl.DataFrame(
                schema={
                    "Month": pl.Utf8,
                    "TotalSavings": pl.Float64,
                    "TotalAllocated": pl.Float64,
                    "TotalSpent": pl.Float64,
                }
            )

        # Add Month column if not present
        if "Month" not in df_savings.columns:
            df_savings = df_savings.with_columns(
                pl.col("Date").dt.strftime("%Y-%m").alias("Month")
            )

        # Add allocation type based on category type
        df_processed = df_savings.with_columns(
            pl.when(pl.col("CategoryType") == "Accantonamento")
            .then(pl.lit("Allocation"))
            .otherwise(pl.lit("Savings"))
            .alias("AllocationType")
        )

        # Calculate metrics by month
        all_months = sorted(df_processed["Month"].unique().to_list())
        metrics = []

        # Running totals
        total_savings = 0.0
        total_allocated = 0.0
        total_spent = 0.0

        for month in all_months:
            # Get data for this month
            month_data = df_processed.filter(pl.col("Month") == month)

            # Calculate savings (positive values in Risparmio categories)
            month_savings = (
                month_data.filter(
                    (pl.col("CategoryType") == "Risparmio") & (pl.col("Value") > 0)
                )["Value"].sum()
                or 0.0
            )
            total_savings += month_savings

            # Calculate allocations (positive values in Accantonamento categories)
            month_allocated = (
                month_data.filter(
                    (pl.col("CategoryType") == "Accantonamento") & (pl.col("Value") > 0)
                )["Value"].sum()
                or 0.0
            )

            # Calculate withdrawals from allocations
            month_allocated_withdrawals = abs(
                month_data.filter(
                    (pl.col("CategoryType") == "Accantonamento") & (pl.col("Value") < 0)
                )["Value"].sum()
                or 0.0
            )

            # Update total allocated funds
            total_allocated = (
                total_allocated + month_allocated - month_allocated_withdrawals
            )

            # Calculate spent funds (withdrawals from non-Accantonamento categories)
            month_spent = abs(
                month_data.filter(
                    (pl.col("CategoryType") != "Accantonamento") & (pl.col("Value") < 0)
                )["Value"].sum()
                or 0.0
            )
            total_spent += month_spent

            # Store monthly metrics
            metrics.append(
                {
                    "Month": month,
                    "TotalSavings": total_savings,
                    "TotalAllocated": total_allocated,
                    "TotalSpent": total_spent,
                }
            )

        return pl.DataFrame(metrics)

    def savings_allocation_status(
        self, df_savings_metrics: pl.DataFrame, df_savings: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Generate allocation status for savings categories.

        Args:
            df_savings_metrics: Savings metrics DataFrame
            df_savings: Processed savings DataFrame

        Returns:
            pl.DataFrame: Allocation status DataFrame
        """
        if len(df_savings_metrics) == 0 or len(df_savings) == 0:
            self.logger.warning("No savings data available for allocation status")
            return pl.DataFrame(
                schema={
                    "Category": pl.Utf8,
                    "Type": pl.Utf8,
                    "Value": pl.Float64,
                }
            )

        # Get the latest month
        last_month = df_savings_metrics["Month"].max()

        # Filter savings data for the latest month
        monthly_data = df_savings.filter(
            pl.col("Month") == last_month
            if "Month" in df_savings.columns
            else pl.col("Date").dt.strftime("%Y-%m") == last_month
        )

        if len(monthly_data) == 0:
            return pl.DataFrame(
                schema={
                    "Category": pl.Utf8,
                    "Type": pl.Utf8,
                    "Value": pl.Float64,
                }
            )

        # Prepare visualization data
        allocation_data = []

        # Get all categories
        categories = monthly_data["Category"].unique().to_list()

        for category in categories:
            # Get category data
            category_data = monthly_data.filter(pl.col("Category") == category)
            category_type = (
                category_data["CategoryType"][0]
                if len(category_data) > 0
                else "Unknown"
            )

            # Process positive transactions (additions)
            positive_data = category_data.filter(pl.col("Value") > 0)
            if len(positive_data) > 0:
                positive_sum = positive_data["Value"].sum()
                allocation_data.append(
                    {
                        "Category": category,
                        "Type": "Allocated"
                        if category_type == "Accantonamento"
                        else "Saved",
                        "Value": positive_sum,
                    }
                )

            # Process negative transactions (withdrawals)
            negative_data = category_data.filter(pl.col("Value") < 0)
            if len(negative_data) > 0:
                negative_sum = abs(negative_data["Value"].sum())
                allocation_data.append(
                    {
                        "Category": category,
                        "Type": "Spent from Allocations"
                        if category_type == "Accantonamento"
                        else "Spent from Savings",
                        "Value": negative_sum,
                    }
                )

        return (
            pl.DataFrame(allocation_data)
            if allocation_data
            else pl.DataFrame(
                schema={
                    "Category": pl.Utf8,
                    "Type": pl.Utf8,
                    "Value": pl.Float64,
                }
            )
        )


class FileManager:
    """
    Manages file operations for datasets.

    This class is responsible for saving datasets to files and managing
    file paths for the application.
    """

    def __init__(self, config: Config, logger: logging.Logger):
        """
        Initialize the file manager.

        Args:
            config: Application configuration
            logger: Logger instance for logging file operations
        """
        self.config = config
        self.logger = logger.getChild("FileManager")
        self.output_folder = config.get("output_folder", "output")
        os.makedirs(self.output_folder, exist_ok=True)

    def save_dataset(self, df: pl.DataFrame, config_key: str) -> bool:
        """
        Save a DataFrame to a CSV file.

        Args:
            df: DataFrame to save
            config_key: Key in the configuration for the file path

        Returns:
            bool: True if saved successfully, False otherwise
        """
        if len(df) == 0:
            self.logger.warning(f"Empty DataFrame for {config_key}, skipping save")
            return False

        path = self.config.get(config_key)
        if not path:
            self.logger.warning(f"Missing path configuration for {config_key}")
            return False

        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Round all float columns to 2 decimal places before saving
            df_to_save = df.clone()
            for col in df.columns:
                if df[col].dtype in (pl.Float32, pl.Float64):
                    df_to_save = df_to_save.with_columns(
                        pl.col(col).round(2).alias(col)
                    )

            # Save to CSV with proper floating point precision
            df_to_save.write_csv(path, float_precision=2)
            self.logger.info(f"Saved dataset to {path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving dataset to {path}: {e}")
            return False


class Process:
    """
    Main class for processing and standardizing financial data.

    This class coordinates the transformation of raw financial data into
    standardized formats, applies consistent schema validation, and
    calculates aggregate metrics for visualization.
    """

    def __init__(self, config: Config, logger: logging.Logger):
        """
        Initialize the Process class with configuration and logger.

        Args:
            config: Configuration object containing mappings and settings
            logger: Logger instance from the main application
        """
        self.config = config
        self.logger = logger.getChild("Process")
        self.logger.info("Process initialized")

        # Initialize components
        self.schema_validator = SchemaValidator(logger)
        self.data_transformer = DataTransformer(config, logger)
        self.analytics_generator = AnalyticsGenerator(config, logger)
        self.file_manager = FileManager(config, logger)

    def process_expense_income_data(
        self, df: pl.DataFrame, data_type: str = "expenses"
    ) -> pl.DataFrame:
        """
        Process expense or income data into a standardized format.

        Args:
            df: Raw expense or income DataFrame
            data_type: Type of data ("expenses" or "income")

        Returns:
            pl.DataFrame: Standardized DataFrame with consistent schema
        """
        self.logger.info(f"Processing {data_type} data with {len(df)} rows")

        # Get the appropriate column mapping
        column_mapping = self.config.get(f"{data_type}_column_mapping")
        if not column_mapping:
            error_msg = f"Missing column mapping for {data_type}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Drop "Mese" column if it exists
        if "Mese" in df.columns:
            df = df.drop("Mese")

        # Clean and standardize data
        df_cleaned = self.data_transformer.clean_string_columns(df)
        df_date = self.data_transformer.standardize_date_format(df_cleaned, "Data")

        # Rename columns according to mapping
        df_renamed = df_date.rename(column_mapping)

        # Validate categories
        valid_categories = self.config.get(f"valid_{data_type}_categories", [])
        default_category = self.config.get("default_category", "Altro")

        df_with_valid_categories = self.data_transformer.normalize_categories(
            df_renamed, valid_categories, default_category
        )

        # Validate data against schema
        df_validated, errors = self.schema_validator.validate_expense_income(
            df_with_valid_categories
        )

        if errors:
            self.logger.warning(
                f"Encountered {len(errors)} validation errors during {data_type} processing"
            )

        # Make sure there's no month column in the output
        if "Mese" in df_validated.columns:
            df_validated = df_validated.drop("Mese")

        self.logger.info(
            f"Successfully processed {len(df_validated)} {data_type} records"
        )
        return df_validated

    def process_savings_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Process savings data into a standardized format.

        Args:
            df: Raw savings DataFrame

        Returns:
            pl.DataFrame: Standardized DataFrame with consistent schema
        """
        self.logger.info(f"Processing savings data with {len(df)} rows")

        # Get column mapping from config
        column_mapping = self.config.get("savings_column_mapping")
        if not column_mapping:
            error_msg = "Missing savings column mapping in configuration"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Drop "Mese" column if it exists
        if "Mese" in df.columns:
            df = df.drop("Mese")

        # Clean and standardize data
        df_cleaned = self.data_transformer.clean_string_columns(df)
        df_date = self.data_transformer.standardize_date_format(df_cleaned, "Data")

        # Rename columns according to mapping
        df_renamed = df_date.rename(column_mapping)

        # Validate categories
        valid_categories = self.config.get("valid_savings_categories", [])
        default_category = self.config.get("default_category", "Varie")

        df_with_valid_categories = self.data_transformer.normalize_categories(
            df_renamed, valid_categories, default_category
        )

        # Validate data against schema
        df_validated, errors = self.schema_validator.validate_savings(
            df_with_valid_categories
        )

        if errors:
            self.logger.warning(
                f"Encountered {len(errors)} validation errors during savings processing"
            )

        # Make sure there's no month column in the output
        if "Mese" in df_validated.columns:
            df_validated = df_validated.drop("Mese")

        self.logger.info(f"Successfully processed {len(df_validated)} savings records")
        return df_validated

    def generate_all_datasets(
        self,
        df_expenses: pl.DataFrame,
        df_income: pl.DataFrame,
        df_savings: Optional[pl.DataFrame] = None,
    ) -> None:
        """
        Generate all intermediate datasets for visualization.

        Args:
            df_expenses: Processed expense DataFrame
            df_income: Processed income DataFrame
            df_savings: Processed savings DataFrame (optional)
        """
        self.logger.info("Generating all datasets for visualization...")

        # Calculate date range - use all available data
        min_date = min(
            df_expenses["Date"].min(),
            df_income["Date"].min(),
            df_savings["Date"].min()
            if df_savings is not None and len(df_savings) > 0
            else datetime.now(),
        )

        max_date = max(
            df_expenses["Date"].max(),
            df_income["Date"].max(),
            df_savings["Date"].max()
            if df_savings is not None and len(df_savings) > 0
            else datetime.now(),
        )

        self.logger.info(
            f"Generating datasets for date range: {min_date.date()} to {max_date.date()}"
        )

        # Generate and save monthly summary
        monthly_summary = self.analytics_generator.monthly_summary(
            df_expenses, df_income, min_date, max_date
        )
        self.file_manager.save_dataset(monthly_summary, "monthly_summary_path")

        # Generate and save expenses summaries
        monthly_expenses = monthly_summary.select(["Month", "Expenses"])
        self.file_manager.save_dataset(monthly_expenses, "monthly_expenses_path")

        expenses_by_category = self.analytics_generator.category_breakdown(
            df_expenses, min_date, max_date
        )
        self.file_manager.save_dataset(
            expenses_by_category, "expenses_by_category_path"
        )

        expenses_stacked = self.analytics_generator.time_series_by_category(
            df_expenses, min_date, max_date, "Value", "Expenses"
        )
        self.file_manager.save_dataset(expenses_stacked, "expenses_stacked_path")

        # Generate and save income summaries
        monthly_income = monthly_summary.select(["Month", "Income"])
        self.file_manager.save_dataset(monthly_income, "monthly_income_path")

        income_by_category = self.analytics_generator.category_breakdown(
            df_income, min_date, max_date
        )
        self.file_manager.save_dataset(income_by_category, "income_by_category_path")

        income_stacked = self.analytics_generator.time_series_by_category(
            df_income, min_date, max_date, "Value", "Income"
        )
        self.file_manager.save_dataset(income_stacked, "income_stacked_path")

        # Generate savings datasets if available
        if df_savings is not None and len(df_savings) > 0:
            # Ensure Month column exists
            if "Month" not in df_savings.columns:
                df_savings = self.data_transformer.add_month_column(df_savings)

            # Calculate and save savings metrics
            savings_metrics = self.analytics_generator.calculate_savings_metrics(
                df_savings
            )
            self.file_manager.save_dataset(savings_metrics, "savings_metrics_path")

            # Get last month data for category breakdown
            if len(savings_metrics) > 0:
                last_month = savings_metrics["Month"].max()
                last_month_data = savings_metrics.filter(pl.col("Month") == last_month)

                # Generate savings category breakdown
                savings_by_category = (
                    df_savings.filter(
                        (pl.col("CategoryType") == "Risparmio")
                        & (pl.col("Month") == last_month)
                    )
                    .groupby("Category")
                    .agg(pl.sum("Value").alias("Value"))
                )
                self.file_manager.save_dataset(
                    savings_by_category, "savings_by_category_path"
                )

                # Generate allocation status
                allocation_status = self.analytics_generator.savings_allocation_status(
                    savings_metrics, df_savings
                )
                self.file_manager.save_dataset(
                    allocation_status, "savings_allocation_path"
                )

        self.logger.info("All datasets generated successfully")


if __name__ == "__main__":
    # Set up logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("process")
