#!/filepath: data_wrangler.py
"""
DataWrangler module for loading and processing financial data from Numbers files.

This module handles the extraction and initial processing of financial data from
Apple Numbers files, converting them to structured Polars DataFrames and saving
the results to CSV files for further processing.
"""

import json
import logging
import os
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import polars as pl  # type: ignore
from numbers_parser import Document  # type: ignore

from config import Config
from models import ProcessingResult, SkippedRow


class NumbersDocumentReader:
    """
    Handles opening and reading Numbers documents.

    This class encapsulates the logic for safely opening Numbers files
    and extracting available sheets.
    """

    def __init__(self, logger: logging.Logger):
        """
        Initialize the reader with a logger.

        Args:
            logger: Logger instance
        """
        self.logger = logger.getChild("NumbersDocumentReader")

    def open_document(self, file_path: str) -> Document:
        """
        Open a Numbers document with warning suppression.

        Args:
            file_path: Path to the Numbers file

        Returns:
            Document: The opened Numbers document

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If no sheets are found in the document
            Exception: For other document opening errors
        """
        self.logger.info(f"Loading Numbers file from: {file_path}")

        if not os.path.exists(file_path):
            self.logger.error(f"Numbers file not found at: {file_path}")
            raise FileNotFoundError(f"Numbers file not found at: {file_path}")

        # Suppress the specific RuntimeWarning about Numbers version
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", category=RuntimeWarning, message="unsupported version*"
            )

            try:
                # Open the Numbers document
                doc = Document(file_path)

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


class DataFrameProcessor:
    """
    Handles processing and cleaning of DataFrames.

    This class is responsible for validating and cleaning data
    after it has been extracted from a Numbers document.
    """

    def __init__(self, config: Config, logger: logging.Logger):
        """
        Initialize the processor with configuration and logger.

        Args:
            config: Application configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger.getChild("DataFrameProcessor")

    def process_sheet_data(
        self, data: List[List[Any]], sheet_name: str
    ) -> Tuple[pl.DataFrame, List[SkippedRow]]:
        """
        Process data from a sheet and create a DataFrame.

        Args:
            data: Raw data from the sheet as a list of lists
            sheet_name: Name of the sheet being processed

        Returns:
            Tuple containing:
                - pl.DataFrame: Processed data as a Polars DataFrame
                - List[SkippedRow]: Rows that were skipped during processing

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
        valid_rows, skipped_rows = self._get_valid_rows(rows, columns, sheet_name)

        # Create Polars DataFrame with validated data
        try:
            # Sanitize rows for DataFrame creation
            sanitized_rows = self._sanitize_rows(valid_rows)

            # Create DataFrame with strings to avoid type inference issues
            df = pl.DataFrame(data=sanitized_rows, schema=columns)

            # Clean the dataframe (handle type conversions)
            df = self._clean_dataframe(df, sheet_name)

            self.logger.info(
                f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns"
            )
            return df, skipped_rows

        except Exception as e:
            self.logger.exception(
                f"Error creating DataFrame for {sheet_name}: {str(e)}"
            )
            raise ValueError(
                f"Error creating DataFrame for {sheet_name}: {str(e)}"
            ) from e

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
        columns = [str(col) if col is not None else "" for col in columns]

        self.logger.info(f"Found columns in {sheet_name}: {', '.join(columns)}")

        return columns

    def _get_valid_rows(
        self, rows: List[List[Any]], columns: List[str], sheet_name: str
    ) -> Tuple[List[List[Any]], List[SkippedRow]]:
        """
        Filter and normalize data rows.

        Args:
            rows: Raw data rows
            columns: Column headers
            sheet_name: Name of the sheet

        Returns:
            Tuple containing:
                - List[List[Any]]: Validated rows
                - List[SkippedRow]: Rows that were skipped
        """
        valid_rows = []
        skipped_rows = []

        # Check for date column index
        date_column_idx = self._find_date_column_index(columns)

        for i, row in enumerate(rows):
            # Ignore completely empty rows
            if row is None or all(cell is None or cell == "" for cell in row):
                continue

            original_row = row.copy() if row else []  # Ensure it's never None

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
            # Skip if more than half are None/empty
            if none_count > len(columns) // 2:
                skipped_rows.append(
                    SkippedRow(
                        sheet_name=sheet_name,
                        row_index=i + 2,  # +2 for 1-based indexing and header row
                        row_data=original_row,
                        reason=f"Too many empty values ({none_count}/{len(columns)} fields are empty)",
                    )
                )
                continue

            valid_rows.append(row)

        if not valid_rows:
            self.logger.error(
                f"No valid data rows after filtering in {sheet_name} sheet"
            )
            raise ValueError(
                f"No valid data rows after filtering in {sheet_name} sheet"
            )

        return valid_rows, skipped_rows

    def _find_date_column_index(self, columns: List[str]) -> Optional[int]:
        """
        Find the index of the date column in the header row.

        Args:
            columns: Column headers

        Returns:
            Optional[int]: Index of date column if found, None otherwise
        """
        date_column_candidate = self.config.get("numbers_date_column")
        if date_column_candidate in columns:
            return columns.index(date_column_candidate)
        return None

    def _sanitize_rows(self, rows: List[List[Any]]) -> List[List[Any]]:
        """
        Sanitize row values for DataFrame creation.

        Args:
            rows: Raw data rows

        Returns:
            List[List[Any]]: Sanitized rows
        """
        sanitized_rows = []
        for row in rows:
            sanitized_row = []
            for value in row:
                # Convert datetime objects to strings to avoid type inference issues
                if isinstance(value, datetime):
                    sanitized_row.append(value.strftime("%Y-%m-%d"))
                else:
                    sanitized_row.append(value)
            sanitized_rows.append(sanitized_row)
        return sanitized_rows

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

        # First, replace empty strings with None in all columns to standardize missing values
        df = self._replace_empty_strings(df)

        # Process date columns
        df = self._process_date_columns(df)

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
        sheet_mappings = self.config.get("numbers_sheet_mappings")

        for italian_name, english_name in sheet_mappings.items():
            if italian_name.lower() in sheet_name.lower():
                return english_name

        self.logger.warning(f"Could not determine sheet type for {sheet_name}")
        return None

    def _process_date_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Process date columns in the DataFrame.

        Args:
            df: DataFrame to process

        Returns:
            pl.DataFrame: DataFrame with processed date columns
        """
        date_column_candidates = self.config.get("date_column_candidates")

        for col in df.columns:
            if col in date_column_candidates:
                try:
                    # Try to convert to date format
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

        return df

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

    def apply_sheet_transformations(
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
        # Apply specific transformations based on sheet type
        if sheet_type == "income":
            # Get the category column name
            self._get_category_column_name(df)
            # The transformation logic should be implemented as needed

        return df

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

        self.logger.warning("Could not determine category column name")
        return None


class FileHandler:
    """
    Handles file operations like saving DataFrames to CSV.
    """

    def __init__(self, logger: logging.Logger):
        """
        Initialize the file handler with a logger.

        Args:
            logger: Logger instance
        """
        self.logger = logger.getChild("FileHandler")

    def save_to_csv(self, df: pl.DataFrame, path: str) -> None:
        """
        Save a DataFrame to CSV, creating directories as needed.

        Args:
            df: DataFrame to save
            path: Path to save the CSV file

        Raises:
            IOError: If saving fails
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
            raise IOError(f"Error saving DataFrame to {path}: {str(e)}") from e

    def save_skipped_rows_report(
        self, skipped_rows: List[SkippedRow], output_path: Optional[str] = None
    ) -> str:
        """
        Save a report of skipped rows to help fix data issues.

        Args:
            skipped_rows: List of skipped rows to report
            output_path: Path to save the report. If None, a default path will be used.

        Returns:
            str: Path where the report was saved

        Raises:
            IOError: If saving fails
        """
        if not skipped_rows:
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
        for row in skipped_rows:
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
                "total_skipped_rows": len(skipped_rows),
                "sheets_with_issues": len(set(row.sheet_name for row in skipped_rows)),
            },
            "skipped_rows_by_sheet": {},
        }

        # Initialize the sheet-based collections
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
            raise IOError(f"Error saving skipped rows report: {str(e)}") from e


class DataWrangler:
    """
    Main class for loading and processing financial data from Numbers files.

    This class coordinates the extraction, processing, and saving of data
    from Apple Numbers spreadsheets.
    """

    def __init__(self, config: Config, logger: logging.Logger):
        """
        Initialize the DataWrangler with configuration and logger.

        Args:
            config: Configuration object
            logger: Logger instance from the main application
        """
        self.config = config
        self.logger = logger.getChild("DataWrangler")

        # Create helper objects
        self.document_reader = NumbersDocumentReader(logger)
        self.df_processor = DataFrameProcessor(config, logger)
        self.file_handler = FileHandler(logger)

        # Set up log directory
        os.makedirs("logs", exist_ok=True)
        self.logger.info("DataWrangler initialized")

    def load_updated_file(self) -> ProcessingResult:
        """
        Load and process all sheets from the Numbers file.

        Returns:
            ProcessingResult: Object containing DataFrames and skipped rows

        Raises:
            FileNotFoundError: If the Numbers file doesn't exist
            ValueError: If no valid sheets are found or if data processing fails
        """
        self.logger.info("Starting to load Numbers file")

        # Get Numbers file path from config
        numbers_path = self.config.get("numbers_file_path")

        # Process the document
        doc = self.document_reader.open_document(numbers_path)
        sheet_mappings = self.config.get("numbers_sheet_mappings")

        # Extract data frames from document sheets
        dfs, all_skipped_rows = self._extract_dataframes(doc, sheet_mappings)

        if not dfs:
            self.logger.error("No valid sheets found in the document")
            raise ValueError("No valid sheets found in the document")

        self.logger.info("Completed loading all sheets successfully")
        return ProcessingResult(dataframes=dfs, skipped_rows=all_skipped_rows)

    def _extract_dataframes(
        self, doc: Document, sheet_mappings: Dict[str, str]
    ) -> Tuple[Dict[str, pl.DataFrame], List[SkippedRow]]:
        """
        Extract DataFrames from document sheets.

        Args:
            doc: Numbers document
            sheet_mappings: Mapping of sheet names to standardized names

        Returns:
            Tuple containing:
                - Dict[str, pl.DataFrame]: Dictionary of processed DataFrames
                - List[SkippedRow]: All skipped rows
        """
        dfs = {}
        all_skipped_rows = []

        for sheet in doc.sheets:
            sheet_name_lower = sheet.name.lower()

            # Check if this sheet should be processed
            if not any(name.lower() == sheet_name_lower for name in sheet_mappings):
                self.logger.debug(f"Skipping sheet: {sheet.name} (not a target sheet)")
                continue

            # Find the matching key in sheet_mappings
            matching_key = next(
                (key for key in sheet_mappings if key.lower() in sheet_name_lower), None
            )

            if not matching_key:
                continue

            # Get the standardized English name for this sheet
            english_name = sheet_mappings[matching_key]
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
            df, skipped_rows = self.df_processor.process_sheet_data(
                raw_data, sheet.name
            )
            all_skipped_rows.extend(skipped_rows)

            # Apply sheet-specific transformations
            df = self.df_processor.apply_sheet_transformations(df, english_name)

            # Store DataFrame with standardized name
            dfs[english_name] = df

            # Save to CSV if path is configured
            self._save_df_to_csv(df, english_name)

            self.logger.info(f"Successfully processed {len(df)} rows from {sheet.name}")

        return dfs, all_skipped_rows

    def _save_df_to_csv(self, df: pl.DataFrame, english_name: str) -> None:
        """
        Save DataFrame to CSV if path is configured.

        Args:
            df: DataFrame to save
            english_name: Standardized name of the sheet
        """
        csv_paths = self.config.get("raw_paths")

        path = csv_paths.get(english_name)

        if path:
            self.file_handler.save_to_csv(df, path)
            self.logger.info(f"Saved {english_name} data to: {path}")
        else:
            self.logger.warning(f"No path configured for {english_name}")

    def save_skipped_rows_report(
        self, skipped_rows: List[SkippedRow], output_path: Optional[str] = None
    ) -> str:
        """
        Save a report of skipped rows to help fix data issues.

        Args:
            skipped_rows: List of skipped rows to report
            output_path: Path to save the report. If None, a default path will be used.

        Returns:
            str: Path where the report was saved
        """
        return self.file_handler.save_skipped_rows_report(skipped_rows, output_path)
