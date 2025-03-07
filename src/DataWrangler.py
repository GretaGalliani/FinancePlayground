#!/filepath: data_wrangler.py
"""
DataWrangler module for loading and processing financial data from Numbers files.

This module handles the extraction and initial processing of financial data from
Apple Numbers files, converting them to structured Polars DataFrames and saving
the results to CSV files for further processing.
"""

import os
import warnings
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
import json
from datetime import datetime

import polars as pl
from numbers_parser import Document
from pathlib import Path


@dataclass
class SkippedRow:
    """Class to store information about skipped rows."""

    sheet_name: str
    row_index: int  # Original index in the sheet
    row_data: List[Any]
    reason: str


class DataWrangler:
    """
    A class for loading and processing financial data from Numbers files.

    This class handles extracting data from Apple Numbers spreadsheets and converting
    it to Polars DataFrames. It supports multiple sheet types (expenses, income, savings)
    and applies initial data cleaning and validation.

    Attributes:
        config: Configuration object containing paths and mappings
        logger: Logger instance for this class
        skipped_rows: List of rows that were skipped during processing
    """

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize the DataWrangler with configuration and logger.

        Args:
            config: Configuration object containing paths and mappings
            logger: Logger instance from the main application
        """
        self.config = config
        self.logger = logger.getChild("DataWrangler")
        self.skipped_rows: List[SkippedRow] = []

        # Set up log directory
        os.makedirs("logs", exist_ok=True)
        self.logger.info("DataWrangler initialized")

    def load_updated_file(self) -> Dict[str, pl.DataFrame]:
        """
        Load and process all sheets from the Numbers file.

        Returns:
            Dict[str, pl.DataFrame]: Dictionary of DataFrames for expenses, income, and savings

        Raises:
            FileNotFoundError: If the Numbers file doesn't exist
            ValueError: If no valid sheets are found or if data processing fails

        Note:
            Any rows that are skipped during processing will be stored in the
            `skipped_rows` attribute and can be saved using `save_skipped_rows_report`.
        """
        self.logger.info("Starting to load Numbers file")

        # Get Numbers file path from config
        numbers_path = self._get_config_value("numbers_file_path")

        # Validate file exists
        if not os.path.exists(numbers_path):
            self.logger.error(f"Numbers file not found at: {numbers_path}")
            raise FileNotFoundError(f"Numbers file not found at: {numbers_path}")

        # Process the document
        doc = self._open_document(numbers_path)
        sheet_mappings = self._get_config_value("sheet_mappings")

        # Extract data frames from document sheets
        dfs = self._extract_dataframes(doc, sheet_mappings)

        if not dfs:
            self.logger.error("No valid sheets found in the document")
            raise ValueError("No valid sheets found in the document")

        self.logger.info("Completed loading all sheets successfully")
        return dfs

    def _open_document(self, numbers_path: str) -> Document:
        """
        Open a Numbers document with warning suppression.

        Args:
            numbers_path: Path to the Numbers file

        Returns:
            Document: The opened Numbers document

        Raises:
            Exception: If document cannot be opened
        """
        self.logger.info(f"Loading Numbers file from: {numbers_path}")

        # Suppress the specific RuntimeWarning about Numbers version
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", category=RuntimeWarning, message="unsupported version*"
            )

            try:
                # Open the Numbers document
                doc = Document(numbers_path)

                # Validate document has sheets
                if not doc.sheets:
                    self.logger.error("No sheets found in the document")
                    raise ValueError("No sheets found in the document")

                # Log all available sheet names
                sheet_names = [sheet.name for sheet in doc.sheets]
                self.logger.info(f"Available sheets: {', '.join(sheet_names)}")

                return doc
            except Exception as e:
                self.logger.exception(f"Error opening Numbers file: {str(e)}")
                raise

    def _extract_dataframes(
        self, doc: Document, sheet_mappings: Dict[str, str]
    ) -> Dict[str, pl.DataFrame]:
        """
        Extract DataFrames from document sheets.

        Args:
            doc: Numbers document
            sheet_mappings: Mapping of sheet names to standardized names

        Returns:
            Dict[str, pl.DataFrame]: Dictionary of processed DataFrames
        """
        dfs = {}

        for sheet in doc.sheets:
            sheet_name_lower = sheet.name.lower()

            # Check if this sheet should be processed
            if sheet_name_lower not in sheet_mappings:
                self.logger.debug(f"Skipping sheet: {sheet.name} (not a target sheet)")
                continue

            # Get the standardized English name for this sheet
            english_name = sheet_mappings[sheet_name_lower]
            self.logger.info(f"Processing sheet: {sheet.name} as {english_name}")

            # Skip sheets without tables
            tables = sheet.tables
            if not tables:
                self.logger.warning(
                    f"No tables found in {sheet.name} sheet, skipping..."
                )
                continue

            # Get rows from the first table
            raw_data = tables[0].rows(values_only=True)

            # Process the data and create DataFrame
            df = self._process_sheet_data(raw_data, sheet.name)

            # Apply sheet-specific transformations
            df = self._apply_sheet_transformations(df, english_name)

            # Store DataFrame with standardized name
            dfs[english_name] = df

            # Save to CSV if path is configured
            self._save_df_to_csv(df, english_name)

            self.logger.info(f"Successfully processed {len(df)} rows from {sheet.name}")

        return dfs

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
        self.logger.info(f"Processing data from sheet: {sheet_name}")

        if not data:
            self.logger.error(f"No data found in the {sheet_name} sheet")
            raise ValueError(f"No data found in the {sheet_name} sheet")

        # Extract and validate column headers
        columns = self._validate_columns(data[0], sheet_name)

        # Extract and validate data rows
        rows = data[1:]
        if not rows:
            self.logger.error(f"No data rows found in {sheet_name} sheet")
            raise ValueError(f"No data rows found in {sheet_name} sheet")

        # Process rows to ensure they're valid
        valid_rows = self._get_valid_rows(rows, columns, sheet_name)

        # Create Polars DataFrame with validated data
        try:
            # Handle datetime conversion issues by converting all values to strings first
            # We'll parse specific columns to proper types after creating the DataFrame
            sanitized_rows = []
            for row in valid_rows:
                sanitized_row = []
                for value in row:
                    # Convert datetime objects to strings to avoid type inference issues
                    if isinstance(value, datetime):
                        sanitized_row.append(value.strftime("%Y-%m-%d"))
                    else:
                        sanitized_row.append(value)
                sanitized_rows.append(sanitized_row)

            # Create DataFrame with strings to avoid type inference issues
            df = pl.DataFrame(data=sanitized_rows, schema=columns)

            # Clean the dataframe (handle type conversions)
            df = self._clean_dataframe(df, sheet_name)

            self.logger.info(
                f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns"
            )
            return df

        except Exception as e:
            self.logger.exception(
                f"Error creating DataFrame for {sheet_name}: {str(e)}"
            )
            raise ValueError(f"Error creating DataFrame for {sheet_name}: {str(e)}")

    def _validate_columns(self, columns: List[Any], sheet_name: str) -> List[str]:
        """
        Validate and normalize column headers.

        Args:
            columns: Raw column headers
            sheet_name: Name of the sheet

        Returns:
            List[str]: Validated column headers

        Raises:
            ValueError: If column headers are invalid
        """
        if not columns or any(col is None for col in columns):
            self.logger.error(
                f"Invalid or missing column headers in {sheet_name} sheet"
            )
            raise ValueError(f"Invalid or missing column headers in {sheet_name} sheet")

        # Convert None values in column headers to empty strings
        columns = [col if col is not None else "" for col in columns]

        self.logger.info(
            f"Found columns in {sheet_name}: {', '.join(str(col) for col in columns)}"
        )

        return columns

    def _get_valid_rows(
        self, rows: List[List[Any]], columns: List[str], sheet_name: str
    ) -> List[List[Any]]:
        """
        Filter and normalize data rows.

        Args:
            rows: Raw data rows
            columns: Column headers
            sheet_name: Name of the sheet

        Returns:
            List[List[Any]]: Validated rows
        """
        valid_rows = []
        skipped_count = 0

        # Check for date column index
        date_column_candidates = ["Data", "Date", "Datum"]
        date_column_idx = None
        for candidate in date_column_candidates:
            if candidate in columns:
                date_column_idx = columns.index(candidate)
                break

        for i, row in enumerate(rows):
            # Ignore completely empty rows
            if row is None or all(cell is None or cell == "" for cell in row):
                continue

            original_row = row.copy() if row else None

            # Skip rows with null date values if date column was identified
            if date_column_idx is not None and (
                len(row) <= date_column_idx
                or row[date_column_idx] is None
                or row[date_column_idx] == ""
            ):
                # Don't record these as skipped rows since we're intentionally ignoring them
                continue

            # Pad or truncate row to match column length
            if len(row) < len(columns):
                row = row + [None] * (len(columns) - len(row))
            elif len(row) > len(columns):
                row = row[: len(columns)]

            # Check if row has too many None values
            none_count = sum(1 for cell in row if cell is None or cell == "")
            if none_count > len(columns) // 2:  # Skip if more than half are None/empty
                self.skipped_rows.append(
                    SkippedRow(
                        sheet_name=sheet_name,
                        row_index=i + 2,  # +2 for 1-based indexing and header row
                        row_data=original_row,
                        reason=f"Too many empty values ({none_count}/{len(columns)} fields are empty)",
                    )
                )
                skipped_count += 1
                continue

            valid_rows.append(row)

        if skipped_count > 0:
            self.logger.warning(f"Skipped {skipped_count} invalid rows in {sheet_name}")

        if not valid_rows:
            self.logger.error(
                f"No valid data rows after filtering in {sheet_name} sheet"
            )
            raise ValueError(
                f"No valid data rows after filtering in {sheet_name} sheet"
            )

        return valid_rows

    def _apply_sheet_transformations(
        self, df: pl.DataFrame, sheet_type: str
    ) -> pl.DataFrame:
        """
        Apply sheet-specific transformations.

        Args:
            df: DataFrame to transform
            sheet_type: Type of sheet (income, expenses, savings)

        Returns:
            pl.DataFrame: Transformed DataFrame
        """
        # Special handling for income data
        if sheet_type == "income":
            # Filter out excluded categories
            categories_column = self._get_category_column_name(df)
            if categories_column and categories_column in df.columns:
                excluded_categories = self._get_config_value(
                    "excluded_income_categories", ["Welfare"]
                )
                df = df.filter(~pl.col(categories_column).is_in(excluded_categories))
                self.logger.info(
                    f"Filtered out excluded income categories: {excluded_categories}"
                )

        return df

    def _save_df_to_csv(self, df: pl.DataFrame, english_name: str) -> None:
        """
        Save DataFrame to CSV if path is configured.

        Args:
            df: DataFrame to save
            english_name: Standardized name of the sheet
        """
        raw_path_key = f"raw_{english_name}_path"
        raw_path = self._get_config_value(raw_path_key, required=False)

        if raw_path:
            self._save_to_csv(df, raw_path)
            self.logger.info(f"Saved {english_name} data to: {raw_path}")
        else:
            self.logger.warning(f"No path configured for {raw_path_key}")

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
        income_mapping = self._get_config_value("income_column_mapping", {})
        for italian_name, english_name in income_mapping.items():
            if english_name == "Category" and italian_name in df.columns:
                return italian_name

        self.logger.warning("Could not determine category column name")
        return None

    def _clean_dataframe(self, df: pl.DataFrame, sheet_name: str) -> pl.DataFrame:
        """
        Clean and prepare the DataFrame for further processing.

        Args:
            df: Raw DataFrame to clean
            sheet_name: Name of the sheet for context

        Returns:
            pl.DataFrame: Cleaned DataFrame
        """
        self.logger.info(f"Cleaning DataFrame for sheet: {sheet_name}")

        # Find the standard name for this sheet
        sheet_type = self._get_sheet_type(sheet_name)

        # Handle date columns (often causing issues in the 'Risparmi' sheet)
        date_column_candidates = ["Data", "Date", "Datum"]

        # First, replace empty strings with None in all columns to standardize missing values
        df = self._replace_empty_strings(df)

        for col in df.columns:
            if col in date_column_candidates:
                try:
                    # Try to convert to date format (any remaining rows with null dates
                    # should have been filtered out in _get_valid_rows)
                    self.logger.debug(f"Converting column {col} to date format")
                    df = df.with_columns(
                        pl.col(col)
                        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                        .alias(col)
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Failed to convert column {col} to date: {str(e)}"
                    )

        # Process monetary values if found
        df = self._clean_monetary_columns(df, sheet_type)

        # Drop rows where all string columns are null
        df = self._drop_empty_rows(df)

        self.logger.info(f"DataFrame cleaning completed for {sheet_name}")
        return df

    def _get_sheet_type(self, sheet_name: str) -> Optional[str]:
        """
        Determine the type of sheet from its name.

        Args:
            sheet_name: Name of the sheet

        Returns:
            Optional[str]: Standard sheet type if found, None otherwise
        """
        sheet_mappings = self._get_config_value("sheet_mappings", {})

        for italian_name, english_name in sheet_mappings.items():
            if italian_name.lower() in sheet_name.lower():
                return english_name

        self.logger.warning(f"Could not determine sheet type for {sheet_name}")
        return None

    def _clean_monetary_columns(
        self, df: pl.DataFrame, sheet_type: Optional[str]
    ) -> pl.DataFrame:
        """
        Clean monetary columns in the DataFrame.

        Args:
            df: DataFrame to clean
            sheet_type: Type of sheet

        Returns:
            pl.DataFrame: DataFrame with cleaned monetary columns
        """
        if not sheet_type:
            return df

        # Get column with monetary values based on sheet type
        value_column = None
        if sheet_type in ["expenses", "income", "savings"]:
            value_column = "Importo"

        # Process monetary values if found
        if value_column and value_column in df.columns:
            self.logger.debug(f"Cleaning monetary column: {value_column}")
            try:
                df = df.with_columns(
                    pl.col(value_column)
                    .cast(pl.Utf8)
                    .str.replace(
                        r"[^\d,\.\-]", ""
                    )  # Remove currency symbols and spaces
                    .str.replace(",", ".")  # Replace comma with dot for decimal
                    .cast(pl.Float64)
                    .round(2)  # Round to exactly two decimal places
                )
                self.logger.debug("Successfully cleaned monetary column")
            except Exception as e:
                self.logger.warning(f"Error cleaning monetary column: {str(e)}")

        return df

    def _replace_empty_strings(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Replace empty strings with None in all string columns.

        Args:
            df: DataFrame to clean

        Returns:
            pl.DataFrame: DataFrame with empty strings replaced
        """
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.when(pl.col(col) == "")
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        return df

    def _drop_empty_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Drop rows where all string columns are null.

        Args:
            df: DataFrame to clean

        Returns:
            pl.DataFrame: DataFrame with empty rows dropped
        """
        # Note: Empty rows should already be filtered out in _get_valid_rows
        # This is just an additional safeguard
        string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]
        if string_cols:
            rows_before = len(df)
            df = df.filter(
                ~pl.fold(
                    True,
                    lambda acc, s: acc & (s.is_null() | (s == "")),
                    [pl.col(c) for c in string_cols],
                )
            )
            rows_dropped = rows_before - len(df)
            if rows_dropped > 0:
                self.logger.info(
                    f"Dropped {rows_dropped} rows with all null/empty string columns"
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

            # Save to CSV with proper floating point precision
            df_to_save.write_csv(path, float_precision=2)
            self.logger.info(f"Successfully saved DataFrame to {path}")

        except Exception as e:
            self.logger.error(f"Error saving DataFrame to {path}: {str(e)}")
            raise IOError(f"Error saving DataFrame to {path}: {str(e)}")

    def _get_config_value(
        self, key: str, default: Any = None, required: bool = True
    ) -> Any:
        """
        Get a value from the configuration.

        Args:
            key: Configuration key
            default: Default value if key is not found
            required: Whether the key is required

        Returns:
            Any: Value from configuration or default

        Raises:
            ValueError: If key is required but not found
        """
        value = self.config.get(key, default)
        if required and value is None:
            self.logger.error(f"Missing {key} in configuration")
            raise ValueError(f"Missing {key} in configuration")
        return value

    def save_skipped_rows_report(self, output_path: Optional[str] = None) -> str:
        """
        Save a report of skipped rows to help fix data issues.

        Args:
            output_path: Path to save the report. If None, a default path will be used.

        Returns:
            str: Path where the report was saved

        Note:
            The report is saved in JSON format and contains detailed information
            about each skipped row, including the sheet name, row index, and reason.
        """
        if not self.skipped_rows:
            self.logger.info("No skipped rows to report")
            return ""

        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = os.path.join("output", "reports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"skipped_rows_{timestamp}.json")

        directory = os.path.dirname(output_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        # Convert skipped rows to serializable format
        skipped_data = []
        for row in self.skipped_rows:
            # Convert any non-serializable objects to strings
            serializable_row_data = []
            for cell in row.row_data:
                if isinstance(cell, (str, int, float, bool, type(None))):
                    serializable_row_data.append(cell)
                else:
                    serializable_row_data.append(str(cell))

            skipped_data.append(
                {
                    "sheet_name": row.sheet_name,
                    "row_index": row.row_index,
                    "row_data": serializable_row_data,
                    "reason": row.reason,
                }
            )

        # Group by sheet for easier analysis
        report = {
            "summary": {
                "total_skipped_rows": len(self.skipped_rows),
                "sheets_with_issues": len(
                    set(row.sheet_name for row in self.skipped_rows)
                ),
            },
            "skipped_rows_by_sheet": {},
        }

        for row in skipped_data:
            sheet_name = row["sheet_name"]
            if sheet_name not in report["skipped_rows_by_sheet"]:
                report["skipped_rows_by_sheet"][sheet_name] = []
            report["skipped_rows_by_sheet"][sheet_name].append(row)

        # Save to file
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved skipped rows report to {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Error saving skipped rows report: {str(e)}")
            raise IOError(f"Error saving skipped rows report: {str(e)}")


if __name__ == "__main__":
    # Example usage
    import logging

    # Create a basic configuration
    sample_config = {
        "numbers_file_path": "path/to/finances.numbers",
        "sheet_mappings": {
            "spese": "expenses",
            "entrate": "income",
            "risparmi": "savings",
        },
        "excluded_income_categories": ["Welfare"],
        "raw_expenses_path": "data/raw/expenses.csv",
        "raw_income_path": "data/raw/income.csv",
        "raw_savings_path": "data/raw/savings.csv",
    }

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("FinanceApp")

    # Create DataWrangler instance
    wrangler = DataWrangler(sample_config, logger)

    try:
        # Load data from Numbers file
        dataframes = wrangler.load_updated_file()

        # Print summary of loaded data
        for sheet_name, df in dataframes.items():
            print(
                f"Loaded {sheet_name} data with {len(df)} rows and {len(df.columns)} columns"
            )

        # Save report of skipped rows for later analysis
        if wrangler.skipped_rows:
            report_path = wrangler.save_skipped_rows_report()
            print(
                f"Saved report of {len(wrangler.skipped_rows)} skipped rows to {report_path}"
            )

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
