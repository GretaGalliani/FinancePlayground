"""
Process module for standardizing and transforming financial data.

This module handles the transformation of raw financial data into standardized
formats suitable for analysis and visualization in the dashboard. It also
generates all intermediate datasets required for visualization.
"""

import os
import pandera.polars as pa
from pandera.typing.polars import DataFrame
import polars as pl
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta


class ExpenseIncomeSchema(pa.DataFrameModel):
    """Schema for standardized expense and income data."""

    Date: pl.Datetime
    Description: pl.String
    Category: pl.String  # We'll handle null categories in our processing logic
    Value: pl.Float64

    class Config:
        coerce = True  # Attempt to coerce types when possible
        strict = False  # Don't be too strict with validation


class SavingsSchema(pa.DataFrameModel):
    """Schema for standardized savings data."""

    Date: pl.Datetime
    Description: pl.String
    Category: pl.String
    CategoryType: pl.String
    Value: pl.Float64


class Process:
    """
    A class for processing and standardizing financial data.

    This class transforms raw financial data from the DataWrangler into
    standardized formats, applies consistent schema validation, and
    calculates aggregate metrics. It also generates all necessary
    intermediate datasets for visualization.

    Attributes:
        config: Configuration object containing mappings and settings
    """

    def __init__(self, config, logger):
        """
        Initialize the Process class with configuration and logger.

        Args:
            config: Configuration object containing column mappings and settings
            logger: Logger instance from the main application
        """
        self.config = config
        self.logger = logger.getChild("Process")
        self.logger.info("Process initialized")
        self._ensure_output_folder()

    def _ensure_output_folder(self):
        """Create the output folder if it doesn't exist."""
        output_folder = self.config.get("output_folder", "output")
        os.makedirs(output_folder, exist_ok=True)

    @pa.check_types
    def process_expense_income_data(
        self, df: DataFrame, data_type: str = "expenses"
    ) -> DataFrame[ExpenseIncomeSchema]:
        """
        Process expense or income data into a standardized format.

        Args:
            df: Raw expense or income DataFrame
            data_type: Type of data ("expenses" or "income")

        Returns:
            DataFrame[ExpenseIncomeSchema]: Standardized DataFrame with consistent schema
        """
        # Drop "Mese" column if it exists
        if "Mese" in df.columns:
            df = df.drop("Mese")

        # Get the appropriate column mapping
        column_mapping = self.config.get(f"{data_type}_column_mapping", {})

        if not column_mapping:
            raise ValueError(f"Missing column mapping for {data_type}")

        # Convert date column to a sensible format
        df_date = self._convert_to_date(df, "Data")

        # Clean string columns
        df_string = self._clean_string_columns(df_date)

        # Rename columns according to mapping
        df_renamed = df_string.rename(column_mapping)

        # Validate and fix categories
        df_with_valid_categories = self._validate_and_fix_categories(
            df_renamed, data_type
        )

        # Make sure we don't have a month/mese column in the output
        if "Mese" in df_with_valid_categories.columns:
            df_with_valid_categories = df_with_valid_categories.drop("Mese")

        return df_with_valid_categories

    @pa.check_types
    def process_savings_data(self, df: DataFrame) -> DataFrame[SavingsSchema]:
        """
        Process savings data into a standardized format.

        Args:
            df: Raw savings DataFrame

        Returns:
            DataFrame[SavingsSchema]: Standardized DataFrame with consistent schema
        """
        # Drop "Mese" column if it exists
        if "Mese" in df.columns:
            df = df.drop("Mese")

        # Get column mapping from config
        column_mapping = self.config.get("savings_column_mapping", {})

        if not column_mapping:
            raise ValueError("Missing savings column mapping in configuration")

        # Convert date column to a sensible format
        df_date = self._convert_to_date(df, "Data")

        # Clean string columns
        df_clean = self._clean_string_columns(df_date)

        # Rename columns according to mapping
        df_renamed = df_clean.rename(column_mapping)

        # Validate and fix categories for savings
        df_with_valid_categories = self._validate_and_fix_categories(
            df_renamed, "savings"
        )

        # Make sure we don't have a month/mese column in the output
        if "Mese" in df_with_valid_categories.columns:
            df_with_valid_categories = df_with_valid_categories.drop("Mese")

        return df_with_valid_categories

    def generate_all_datasets(self, df_expenses, df_income, df_savings):
        """
        Generate all intermediate datasets for visualization.

        Args:
            df_expenses: Processed expense DataFrame
            df_income: Processed income DataFrame
            df_savings: Processed savings DataFrame
        """
        self.logger.info("Generating all datasets for visualization...")

        # Calculate date range - use all available data
        min_date = min(
            df_expenses["Date"].min(),
            df_income["Date"].min(),
            df_savings["Date"].min() if df_savings is not None else datetime.now(),
        )
        max_date = max(
            df_expenses["Date"].max(),
            df_income["Date"].max(),
            df_savings["Date"].max() if df_savings is not None else datetime.now(),
        )

        # Generate income and expense datasets
        self.generate_monthly_summary(df_expenses, df_income, min_date, max_date)
        self.generate_expense_breakdown(df_expenses, min_date, max_date)
        self.generate_income_breakdown(df_income, min_date, max_date)

        # Generate savings datasets
        if df_savings is not None:
            df_savings_monthly = self.calculate_savings_totals(df_savings)
            self.generate_savings_datasets(
                df_savings, df_savings_monthly, min_date, max_date
            )

        self.logger.info("All datasets generated successfully")

    def generate_monthly_summary(self, df_expenses, df_income, start_date, end_date):
        """
        Generate monthly summary datasets for expenses and income.

        Args:
            df_expenses: Processed expense DataFrame
            df_income: Processed income DataFrame
            start_date: Start date for the summary
            end_date: End date for the summary
        """
        # Monthly expenses
        monthly_expenses = (
            df_expenses.filter(
                (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
            )
            .with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Month"))
            .groupby("Month")
            .agg(pl.sum("Value").alias("Expenses"))
            .sort("Month")
        )
        self._save_dataset(monthly_expenses, "monthly_expenses_path")

        # Monthly income
        monthly_income = (
            df_income.filter(
                (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
            )
            .with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Month"))
            .groupby("Month")
            .agg(pl.sum("Value").alias("Income"))
            .sort("Month")
        )
        self._save_dataset(monthly_income, "monthly_income_path")

        # Combined monthly summary
        monthly_summary = monthly_expenses.join(
            monthly_income, on="Month", how="outer"
        ).fill_null(0)

        # Calculate balance
        monthly_summary = monthly_summary.with_columns(
            (pl.col("Income") - pl.col("Expenses")).alias("Balance")
        )

        self._save_dataset(monthly_summary, "monthly_summary_path")

    def generate_expense_breakdown(self, df_expenses, start_date, end_date):
        """
        Generate expense breakdown datasets by category.

        Args:
            df_expenses: Processed expense DataFrame
            start_date: Start date for the summary
            end_date: End date for the summary
        """
        # Expenses by category
        expenses_by_category = (
            df_expenses.filter(
                (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
            )
            .groupby("Category")
            .agg(pl.sum("Value").alias("Total"))
            .sort("Total", descending=True)
        )
        self._save_dataset(expenses_by_category, "expenses_by_category_path")

        # Stacked expenses by month and category
        expenses_stacked = (
            df_expenses.filter(
                (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
            )
            .with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Month"))
            .groupby(["Month", "Category"])
            .agg(pl.sum("Value").alias("Expenses"))
            .sort(["Month", "Category"])
        )
        self._save_dataset(expenses_stacked, "expenses_stacked_path")

    def generate_income_breakdown(self, df_income, start_date, end_date):
        """
        Generate income breakdown datasets by category.

        Args:
            df_income: Processed income DataFrame
            start_date: Start date for the summary
            end_date: End date for the summary
        """
        # Income by category
        income_by_category = (
            df_income.filter(
                (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
            )
            .groupby("Category")
            .agg(pl.sum("Value").alias("Total"))
            .sort("Total", descending=True)
        )
        self._save_dataset(income_by_category, "income_by_category_path")

        # Stacked income by month and category
        income_stacked = (
            df_income.filter(
                (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
            )
            .with_columns(pl.col("Date").dt.strftime("%Y-%m").alias("Month"))
            .groupby(["Month", "Category"])
            .agg(pl.sum("Value").alias("Income"))
            .sort(["Month", "Category"])
        )
        self._save_dataset(income_stacked, "income_stacked_path")

    def calculate_savings_totals(self, df: DataFrame) -> pl.DataFrame:
        """
        Calculate monthly running totals for each savings category and type.

        Args:
            df: Processed savings DataFrame

        Returns:
            pl.DataFrame: Monthly aggregated savings with running totals
        """
        # Drop "Mese" column if it exists
        if "Mese" in df.columns:
            df = df.drop("Mese")

        if len(df) == 0:
            return pl.DataFrame(
                {
                    "Month": [],
                    "Category": [],
                    "CategoryType": [],
                    "MonthlyValue": [],
                    "TotalValue": [],
                    "TotalSavings": [],
                    "TotalAllocated": [],
                    "TotalSpent": [],
                }
            )

        # First, add the Month column and map the transaction types correctly
        df_with_month = df.with_columns(
            [
                pl.col("Date").dt.strftime("%Y-%m").alias("Month"),
                # Determine AllocationType based on CategoryType
                pl.when(pl.col("CategoryType") == "Accantonamento")
                .then(pl.lit("Allocation"))
                .otherwise(pl.lit("Savings"))
                .alias("AllocationType"),
            ]
        )

        # Add an IsAllocation field for internal use (needed for calculations)
        df_with_month = df_with_month.with_columns(
            pl.when(pl.col("AllocationType") == "Allocation")
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("IsAllocation")
        )

        # Group by month, category, category type, and allocation type
        monthly_aggregates = df_with_month.groupby(
            ["Month", "Category", "CategoryType", "AllocationType"]
        ).agg(pl.sum("Value").alias("MonthlyValue"))

        # Process each Category-CategoryType-AllocationType group separately
        # We'll build the results step by step
        result_frames = []

        # Get all unique combinations
        unique_combinations = df_with_month.select(
            pl.col("Category"), pl.col("CategoryType"), pl.col("AllocationType")
        ).unique()

        # For each combination, calculate running totals
        for row in unique_combinations.iter_rows(named=True):
            category = row["Category"]
            category_type = row["CategoryType"]
            allocation_type = row["AllocationType"]

            # Filter data for this combination
            filtered = monthly_aggregates.filter(
                (pl.col("Category") == category)
                & (pl.col("CategoryType") == category_type)
                & (pl.col("AllocationType") == allocation_type)
            ).sort("Month")

            # If there's data for this combination
            if len(filtered) > 0:
                # Calculate running total
                filtered_with_total = filtered.with_columns(
                    pl.col("MonthlyValue").cum_sum().alias("TotalValue")
                )

                # Add to results
                result_frames.append(filtered_with_total)

        # Combine all results
        if result_frames:
            all_results = pl.concat(result_frames)
        else:
            return pl.DataFrame(
                {
                    "Month": [],
                    "Category": [],
                    "CategoryType": [],
                    "AllocationType": [],
                    "MonthlyValue": [],
                    "TotalValue": [],
                    "TotalSavings": [],
                    "TotalAllocated": [],
                    "TotalSpent": [],
                }
            )

        # Calculate key metrics by month
        monthly_metrics = []

        # Get all available months
        all_months = all_results["Month"].unique().sort()

        # Running totals
        total_savings = 0.0
        total_allocated = 0.0
        total_spent = 0.0

        for month in all_months:
            # Get data for this month
            month_data = df_with_month.filter(pl.col("Month") == month)

            # 1. TOTAL SAVINGS - All Savings type category transactions (positive values)
            savings_data = month_data.filter(
                (pl.col("CategoryType") == "Risparmio") & (pl.col("Value") > 0)
            )
            month_savings = savings_data["Value"].sum() or 0.0
            total_savings += month_savings

            # 2. ALLOCATED FUNDS - All Allocation type category transactions (positive values)
            # Only consider positive values for allocated funds (adding to allocations)
            alloc_data = month_data.filter(
                (pl.col("CategoryType") == "Accantonamento") & (pl.col("Value") > 0)
            )
            month_allocated = alloc_data["Value"].sum() or 0.0

            # Subtract negative values for withdrawals from allocations
            alloc_withdrawal_data = month_data.filter(
                (pl.col("CategoryType") == "Accantonamento") & (pl.col("Value") < 0)
            )
            month_allocated_withdrawals = abs(
                alloc_withdrawal_data["Value"].sum() or 0.0
            )

            # Adjust the total allocated funds
            total_allocated = (
                total_allocated + month_allocated - month_allocated_withdrawals
            )

            # 3. SPENT FUNDS - Only consider non-Accantonamento withdrawals
            spent_data = month_data.filter(
                (pl.col("CategoryType") != "Accantonamento") & (pl.col("Value") < 0)
            )
            month_spent = abs(
                spent_data["Value"].sum() or 0.0
            )  # Make positive for display
            total_spent += month_spent

            # Store monthly metrics
            monthly_metrics.append(
                {
                    "Month": month,
                    "TotalSavings": total_savings,
                    "TotalAllocated": total_allocated,
                    "TotalSpent": total_spent,
                }
            )

        # Convert metrics to DataFrame
        metrics_df = pl.DataFrame(monthly_metrics)

        # Join metrics to the main results
        final_results = all_results.join(metrics_df, on="Month", how="left")

        # Fill nulls with zeros
        final_results = final_results.with_columns(
            [
                pl.col("TotalSavings").fill_null(0.0),
                pl.col("TotalAllocated").fill_null(0.0),
                pl.col("TotalSpent").fill_null(0.0),
            ]
        )

        return final_results

    def generate_savings_datasets(
        self, df_savings, df_savings_monthly, start_date, end_date
    ):
        """
        Generate savings-related datasets for visualization.

        Args:
            df_savings: Processed savings DataFrame
            df_savings_monthly: Monthly savings totals DataFrame
            start_date: Start date for the summary
            end_date: End date for the summary
        """
        # Filter data for the selected date range
        df_savings_filtered = df_savings.filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        # Filter monthly data
        start_month = datetime.strftime(start_date, "%Y-%m")
        end_month = datetime.strftime(end_date, "%Y-%m")
        df_monthly_filtered = df_savings_monthly.filter(
            (pl.col("Month") >= start_month) & (pl.col("Month") <= end_month)
        )

        # 1. Savings metrics by month
        savings_metrics = (
            df_monthly_filtered.groupby("Month")
            .agg(
                pl.first("TotalSavings").alias("TotalSavings"),
                pl.first("TotalAllocated").alias("TotalAllocated"),
                pl.first("TotalSpent").alias("TotalSpent"),
            )
            .sort("Month")
        )
        self._save_dataset(savings_metrics, "savings_metrics_path")

        # 2. Savings by category (last month)
        last_month = df_monthly_filtered["Month"].max()
        last_month_data = df_monthly_filtered.filter(pl.col("Month") == last_month)

        category_totals = (
            last_month_data.filter(pl.col("CategoryType") == "Risparmio")
            .groupby("Category")
            .agg(pl.sum("TotalValue").alias("Value"))
        )
        self._save_dataset(category_totals, "savings_by_category_path")

        # 3. Allocation status data
        # Prepare data for visualization
        visualization_data = []

        # Get all categories
        categories = df_monthly_filtered["Category"].unique().to_list()

        for category in categories:
            # Determine category type (Risparmio or Accantonamento)
            category_type_data = df_monthly_filtered.filter(
                pl.col("Category") == category
            )
            category_type = (
                category_type_data["CategoryType"][0]
                if len(category_type_data) > 0
                else "Unknown"
            )

            # Filter last month data for this category
            category_data = last_month_data.filter(pl.col("Category") == category)

            # Get positive transactions (additions)
            positive_data = category_data.filter(pl.col("MonthlyValue") > 0)
            if len(positive_data) > 0:
                positive_sum = positive_data["MonthlyValue"].sum()
                visualization_data.append(
                    {
                        "Category": category,
                        "Type": (
                            "Allocated"
                            if category_type == "Accantonamento"
                            else "Saved"
                        ),
                        "Value": positive_sum,
                    }
                )

            # Get negative transactions (withdrawals/spending)
            negative_data = category_data.filter(pl.col("MonthlyValue") < 0)
            if len(negative_data) > 0:
                negative_sum = abs(negative_data["MonthlyValue"].sum())
                visualization_data.append(
                    {
                        "Category": category,
                        "Type": (
                            "Spent from Allocations"
                            if category_type == "Accantonamento"
                            else "Spent from Savings"
                        ),
                        "Value": negative_sum,
                    }
                )

        # Convert to DataFrame and save
        if visualization_data:
            allocation_df = pl.DataFrame(visualization_data)
            self._save_dataset(allocation_df, "savings_allocation_path")

    def _save_dataset(self, df, config_key):
        """
        Save a DataFrame to a CSV file.

        Args:
            df: DataFrame to save
            config_key: Key in the configuration for the output path
        """
        if len(df) == 0:
            self.logger.warning(f"Empty DataFrame for {config_key}, skipping save")
            return

        path = self.config.get(config_key)
        if not path:
            self.logger.warning(f"Missing path configuration for {config_key}")
            return

        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.write_csv(path)
            self.logger.info(f"Saved dataset to {path}")
        except Exception as e:
            self.logger.error(f"Error saving dataset to {path}: {e}")

    def _validate_and_fix_categories(
        self, df: pl.DataFrame, data_type: str
    ) -> pl.DataFrame:
        """
        Validate categories against the configured list and fix invalid ones.

        Args:
            df: DataFrame to validate
            data_type: Type of data ("expenses" or "income")

        Returns:
            pl.DataFrame: DataFrame with validated categories
        """
        if "Category" not in df.columns:
            self.logger.warning("No Category column found in the DataFrame")
            return df

        # Get valid categories from config
        valid_categories_key = f"valid_{data_type}_categories"
        valid_categories = self.config.get(valid_categories_key, [])
        default_category = self.config.get("default_category", "Miscellaneous")

        # Check for null categories
        null_count = df.filter(pl.col("Category").is_null()).height
        if null_count > 0:
            self.logger.warning(
                f"Found {null_count} records with null categories. Using default category: {default_category}"
            )

        # Check for invalid categories
        if valid_categories:
            invalid_categories = df.filter(
                ~pl.col("Category").is_null()
                & ~pl.col("Category").is_in(valid_categories)
            ).select(pl.col("Category").unique())

            if len(invalid_categories) > 0:
                invalid_list = invalid_categories["Category"].to_list()
                self.logger.warning(
                    f"Found invalid categories: {', '.join(invalid_list)}. Using default category: {default_category}"
                )

        # Replace null or invalid categories with default
        if valid_categories:
            df = df.with_columns(
                pl.when(
                    pl.col("Category").is_null()
                    | ~pl.col("Category").is_in(valid_categories)
                )
                .then(pl.lit(default_category))
                .otherwise(pl.col("Category"))
                .alias("Category")
            )
        else:
            # If no valid categories are defined, just replace nulls
            df = df.with_columns(
                pl.when(pl.col("Category").is_null())
                .then(pl.lit(default_category))
                .otherwise(pl.col("Category"))
                .alias("Category")
            )

        return df

    def _convert_to_date(
        self, df: pl.DataFrame, date_col: str = "Data"
    ) -> pl.DataFrame:
        """
        Convert a date column to datetime format.

        Args:
            df: DataFrame to process
            date_col: Name of the date column to convert

        Returns:
            pl.DataFrame: DataFrame with converted date column
        """
        if date_col not in df.columns:
            return df

        # Check the data type and format of the date column
        if df[date_col].dtype == pl.Utf8:
            # Try multiple date formats
            try:
                # First try DD/MM/YY format
                if "/" in df[date_col][0]:
                    return df.with_columns(
                        pl.col(date_col)
                        .str.strptime(pl.Datetime, format="%d/%m/%y")
                        .alias(date_col)
                    )
                # Then try YYYY-MM-DD format
                elif "-" in df[date_col][0]:
                    return df.with_columns(
                        pl.col(date_col)
                        .str.strptime(pl.Datetime, format="%Y-%m-%d")
                        .alias(date_col)
                    )
            except Exception:
                # If specific formats fail, try a more general approach
                return df.with_columns(
                    pl.col(date_col).str.to_datetime().alias(date_col)
                )
        elif df[date_col].dtype == pl.Date:
            # Convert Date to Datetime
            return df.with_columns(pl.col(date_col).cast(pl.Datetime).alias(date_col))

        return df

    def _clean_string_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean string columns by removing leading/trailing whitespace.

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
