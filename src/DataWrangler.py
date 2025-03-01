"""
DataWrangler module for loading and processing financial data from Numbers files.

This module handles the extraction and initial processing of financial data from
Apple Numbers files, converting them to structured Polars DataFrames and saving
the results to CSV files for further processing.
"""

import os
import warnings
from typing import Dict, List, Optional, Any

import polars as pl
from numbers_parser import Document
from pathlib import Path


class DataWrangler:
    """
    A class for loading and processing financial data from Numbers files.

    This class handles extracting data from Apple Numbers spreadsheets and converting
    it to Polars DataFrames. It supports multiple sheet types (expenses, income, savings)
    and applies initial data cleaning and validation.

    Attributes:
        config: Configuration object containing paths and mappings
    """

    def __init__(self, config):
        """
        Initialize the DataWrangler with configuration.

        Args:
            config: Configuration object containing paths and mappings
        """
        self.config = config

    def load_updated_file(self) -> Dict[str, pl.DataFrame]:
        """
        Load and process all sheets from the Numbers file.

        Returns:
            Dict[str, pl.DataFrame]: Dictionary of DataFrames for expenses, income, and savings

        Raises:
            FileNotFoundError: If the Numbers file doesn't exist
            ValueError: If no valid sheets are found or if data processing fails
        """
        # Suppress the specific RuntimeWarning about Numbers version
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", category=RuntimeWarning, message="unsupported version*"
            )

            # Check if the Numbers file exists
            numbers_path = self.config.get("numbers_file_path")
            if not numbers_path:
                raise ValueError("Missing numbers_file_path in configuration")

            if not os.path.exists(numbers_path):
                raise FileNotFoundError(f"Numbers file not found at: {numbers_path}")

            print(f"Loading Numbers file from: {numbers_path}")

            try:
                # Open the Numbers document
                doc = Document(numbers_path)

                # Get all sheets
                sheets = doc.sheets
                if not sheets:
                    raise ValueError("No sheets found in the document")

                # Print all available sheet names for debugging
                sheet_names = [sheet.name for sheet in sheets]
                print(f"Available sheets: {', '.join(sheet_names)}")

                # Dictionary to store DataFrames
                dfs = {}

                # Get sheet mappings from config
                sheet_mappings = self.config.get("sheet_mappings", {})
                if not sheet_mappings:
                    raise ValueError("Sheet mappings not found in configuration")

                # Process each sheet we're interested in
                for sheet in sheets:
                    sheet_name_lower = sheet.name.lower()

                    # Check if this sheet should be processed
                    if sheet_name_lower not in sheet_mappings:
                        print(f"Skipping sheet: {sheet.name} (not a target sheet)")
                        continue

                    # Get the standardized English name for this sheet
                    english_name = sheet_mappings[sheet_name_lower]
                    print(f"\nProcessing sheet: {sheet.name} as {english_name}")

                    # Get tables from the sheet
                    tables = sheet.tables
                    if not tables:
                        print(f"No tables found in {sheet.name} sheet, skipping...")
                        continue

                    # Get rows from the first table
                    data = tables[0].rows(values_only=True)

                    # Process the data and create DataFrame
                    df = self._process_sheet_data(data, sheet.name)

                    # Special handling: if this is income data, filter out the Welfare category
                    if english_name == "income":
                        # Filter out Welfare category if it exists
                        categories_column = self._get_category_column_name(df)
                        if categories_column and categories_column in df.columns:
                            excluded_categories = self.config.get(
                                "excluded_income_categories", ["Welfare"]
                            )
                            df = df.filter(
                                ~pl.col(categories_column).is_in(excluded_categories)
                            )
                            print(
                                f"Filtered out excluded income categories: {excluded_categories}"
                            )

                    # Store DataFrame with standardized name
                    dfs[english_name] = df

                    # Save to CSV
                    raw_path_key = f"raw_{english_name}_path"
                    raw_path = self.config.get(raw_path_key)

                    if raw_path:
                        self._save_to_csv(df, raw_path)
                        print(f"Saved {english_name} data to: {raw_path}")
                    else:
                        print(f"Warning: No path configured for {raw_path_key}")

                    print(f"Successfully processed {len(df)} rows from {sheet.name}")

                if not dfs:
                    raise ValueError("No valid sheets found in the document")

                return dfs

            except Exception as e:
                print(f"Error processing Numbers file: {str(e)}")
                raise

    def _get_category_column_name(self, df: pl.DataFrame) -> Optional[str]:
        """
        Get the name of the category column in a DataFrame.

        Args:
            df: DataFrame to search

        Returns:
            Optional[str]: Name of the category column if found, None otherwise
        """
        # First check for the standard English name
        if "Category" in df.columns:
            return "Category"

        # Then check for the Italian name
        if "Categoria" in df.columns:
            return "Categoria"

        # Try to infer from column mapping
        income_mapping = self.config.get("income_column_mapping", {})
        for italian_name, english_name in income_mapping.items():
            if english_name == "Category" and italian_name in df.columns:
                return italian_name

        return None

    def _process_sheet_data(
        self, data: List[List[Any]], sheet_name: str
    ) -> pl.DataFrame:
        """
        Process data from a sheet and create a DataFrame.

        Args:
            data: Raw data from the sheet as a list of lists
            sheet_name: Name of the sheet being processed

        Returns:
            pl.DataFrame: Processed data as a Polars DataFrame

        Raises:
            ValueError: If no data is found or if column headers are invalid
        """
        if not data:
            raise ValueError(f"No data found in the {sheet_name} sheet")

        # Extract and validate column headers
        columns = data[0]
        if not columns or any(col is None for col in columns):
            raise ValueError(f"Invalid or missing column headers in {sheet_name} sheet")

        # Convert None values in column headers to empty strings
        columns = [col if col is not None else "" for col in columns]

        print(
            f"Found columns in {sheet_name}: {', '.join(str(col) for col in columns)}"
        )

        # Extract and validate data rows
        rows = data[1:]
        if not rows:
            raise ValueError(f"No data rows found in {sheet_name} sheet")

        # Filter out rows with incorrect length or too many None values
        valid_rows = []
        skipped_rows = 0

        for row in rows:
            if row is None:
                skipped_rows += 1
                continue

            # Pad or truncate row to match column length
            if len(row) < len(columns):
                row = row + [None] * (len(columns) - len(row))
            elif len(row) > len(columns):
                row = row[: len(columns)]

            # Check if row has too many None values
            none_count = sum(1 for cell in row if cell is None)
            if none_count > len(columns) // 2:  # Skip if more than half are None
                skipped_rows += 1
                continue

            valid_rows.append(row)

        if skipped_rows > 0:
            print(f"Skipped {skipped_rows} invalid rows in {sheet_name}")

        if not valid_rows:
            raise ValueError(
                f"No valid data rows after filtering in {sheet_name} sheet"
            )

        # Create Polars DataFrame with validated data
        try:
            df = pl.DataFrame(data=valid_rows, schema=columns)

            # Handle missing values with more robust approach
            df = self._clean_dataframe(df, sheet_name)

            return df

        except Exception as e:
            raise ValueError(f"Error creating DataFrame for {sheet_name}: {str(e)}")

    def _clean_dataframe(self, df: pl.DataFrame, sheet_name: str) -> pl.DataFrame:
        """
        Clean and prepare the DataFrame for further processing.

        Args:
            df: Raw DataFrame to clean
            sheet_name: Name of the sheet for context

        Returns:
            pl.DataFrame: Cleaned DataFrame
        """
        # Handle numeric columns with currency format (€)
        sheet_mappings = self.config.get("sheet_mappings", {})
        sheet_type = None

        # Find the standard name for this sheet
        for italian_name, english_name in sheet_mappings.items():
            if italian_name.lower() in sheet_name.lower():
                sheet_type = english_name
                break

        if not sheet_type:
            print(f"Warning: Could not determine sheet type for {sheet_name}")
            return df

        # Get column with monetary values based on sheet type
        value_column = None
        if sheet_type == "expenses" or sheet_type == "income":
            value_column = "Importo"
        elif sheet_type == "savings":
            value_column = "Importo"

        # Process monetary values if found
        if value_column and value_column in df.columns:
            # Apply currency cleaning to monetary columns
            df = df.with_columns(
                pl.col(value_column)
                .cast(pl.Utf8)
                .str.replace(r"[^\d,\.\-]", "")  # Remove currency symbols and spaces
                .str.replace(",", ".")  # Replace comma with dot for decimal
                .cast(pl.Float64)
                .round(2)  # Round to exactly two decimal places
            )

        # Replace empty strings with None
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.when(pl.col(col) == "")
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        # Drop rows where all string columns are null
        string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]
        if string_cols:
            df = df.filter(
                ~pl.fold(
                    True,
                    lambda acc, s: acc & s.is_null(),
                    [pl.col(c) for c in string_cols],
                )
            )

        return df

    def _save_to_csv(self, df: pl.DataFrame, path: str) -> None:
        """
        Save a DataFrame to CSV, creating directories as needed.

        Args:
            df: DataFrame to save
            path: Path to save the CSV file
        """
        try:
            # Create directory if it doesn't exist
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)

            # Round all float columns to 2 decimal places before saving
            df_to_save = df.clone()
            for col in df.columns:
                if df[col].dtype in (pl.Float32, pl.Float64):
                    df_to_save = df_to_save.with_columns(
                        pl.col(col).round(2).alias(col)
                    )

            # Save to CSV
            # Use the float_precision argument to ensure proper floating point formatting
            df_to_save.write_csv(path, float_precision=2)
        except Exception as e:
            print(f"Error saving DataFrame to {path}: {str(e)}")
