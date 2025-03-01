"""
Process module for standardizing and transforming financial data.

This module handles the transformation of raw financial data into standardized
formats suitable for analysis and visualization in the dashboard.
"""

import pandera.polars as pa
from pandera.typing.polars import DataFrame
import polars as pl
from typing import Dict, Optional, List

from src.logger import logger


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
    Account: pl.String
    Value: pl.Float64
    Type: pl.String


class Config:
    coerce = True  # Attempt to coerce types when possible
    strict = False  # Don't be too strict with validation


class MonthlySavingsSchema(pa.DataFrameModel):
    """Schema for aggregated monthly savings data."""

    Month: pl.String
    Category: pl.String
    Account: pl.String
    Type: pl.String
    MonthlyValue: pl.Float64
    TotalValue: pl.Float64


class Process:
    """
    A class for processing and standardizing financial data.

    This class transforms raw financial data from the DataWrangler into
    standardized formats, applies consistent schema validation, and
    calculates aggregate metrics.

    Attributes:
        config: Configuration object containing mappings and settings
    """

    def __init__(self, config):
        """
        Initialize the Process class with configuration.

        Args:
            config: Configuration object containing column mappings and settings
        """
        self.config = config

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
        savings_types = self.config.get("savings_types", {})

        if not column_mapping:
            raise ValueError("Missing savings column mapping in configuration")

        # Convert date column to a sensible format
        df_date = self._convert_to_date(df, "Data")

        # Clean string columns
        df_clean = self._clean_string_columns(df_date)

        # Rename columns according to mapping
        df_renamed = df_clean.rename(column_mapping)

        # Translate savings types if mapping exists
        if savings_types and "Type" in df_renamed.columns:
            # Create a mapping expression using when-then-otherwise
            type_expr = None
            for italian, english in savings_types.items():
                if type_expr is None:
                    type_expr = pl.when(pl.col("Type") == italian).then(pl.lit(english))
                else:
                    type_expr = type_expr.when(pl.col("Type") == italian).then(
                        pl.lit(english)
                    )

            # Apply the default value if no match
            type_expr = type_expr.otherwise(pl.col("Type"))

            # Apply the translation
            df_renamed = df_renamed.with_columns(type_expr.alias("Type"))

        # Validate and fix categories for savings
        df_with_valid_categories = self._validate_and_fix_categories(
            df_renamed, "savings"
        )

        # Make sure we don't have a month/mese column in the output
        if "Mese" in df_with_valid_categories.columns:
            df_with_valid_categories = df_with_valid_categories.drop("Mese")

        return df_with_valid_categories

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
                    "Account": [],
                    "AllocationType": [],
                    "MonthlyValue": [],
                    "TotalValue": [],
                }
            )

        # First, add the Month column and flag for allocation types
        df_with_month = df.with_columns(
            [
                pl.col("Date").dt.strftime("%Y-%m").alias("Month"),
                pl.when(
                    (pl.col("Type") == "Allocation")
                    | (pl.col("Type") == "Accantonamento")
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("IsAllocation"),
            ]
        )

        # Group by month, category, account, and allocation type
        monthly_aggregates = df_with_month.groupby(
            ["Month", "Category", "Account", "IsAllocation"]
        ).agg(pl.sum("Value").alias("MonthlyValue"))

        # Process each Category-Account-IsAllocation group separately
        # We'll build the results step by step
        result_frames = []

        # Get all unique combinations
        unique_combinations = df_with_month.select(
            pl.col("Category"), pl.col("Account"), pl.col("IsAllocation")
        ).unique()

        # For each combination, calculate running totals
        for row in unique_combinations.iter_rows(named=True):
            category = row["Category"]
            account = row["Account"]
            is_allocation = row["IsAllocation"]

            # Filter data for this combination
            filtered = monthly_aggregates.filter(
                (pl.col("Category") == category)
                & (pl.col("Account") == account)
                & (pl.col("IsAllocation") == is_allocation)
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
                    "Account": [],
                    "IsAllocation": [],
                    "MonthlyValue": [],
                    "TotalValue": [],
                }
            )

        # Create a readable allocation type column
        all_results = all_results.with_columns(
            pl.when(pl.col("IsAllocation"))
            .then(pl.lit("Allocation"))
            .otherwise(pl.lit("Spent"))
            .alias("AllocationType")
        )

        # Calculate total savings across all categories (only for spent amounts)
        total_savings = (
            all_results.filter(~pl.col("IsAllocation"))
            .groupby("Month")
            .agg(pl.sum("TotalValue").alias("TotalSavings"))
        )

        # Join the total savings back to all results
        final_results = all_results.join(total_savings, on="Month", how="left")

        return final_results

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
            logger.warning("No Category column found in the DataFrame")
            return df

        # Get valid categories from config
        valid_categories_key = f"valid_{data_type}_categories"
        valid_categories = self.config.get(valid_categories_key, [])
        default_category = self.config.get("default_category", "Miscellaneous")

        # Check for null categories
        null_count = df.filter(pl.col("Category").is_null()).height
        if null_count > 0:
            logger.warning(
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
                logger.warning(
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
