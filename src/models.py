#!/filepath: models.py
"""Data models for finance applications."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl # pylint: disable=import-error
from pydantic import BaseModel # pylint: disable=import-error


@dataclass
class SkippedRow:
    """
    Represents a row that was skipped during data processing.

    Attributes:
        sheet_name: Name of the sheet where the row was found
        row_index: Index of the row in the original data (1-based for human readability)
        row_data: The original row data that was skipped
        reason: Reason why the row was skipped
    """

    sheet_name: str
    row_index: int
    row_data: List[Any]
    reason: str


@dataclass
class ProcessingResult:
    """
    Represents the result of processing a financial data file.

    Attributes:
        dataframes: Dictionary mapping sheet names to processed DataFrames
        skipped_rows: List of rows that were skipped during processing
    """

    dataframes: Dict[str, pl.DataFrame]
    skipped_rows: List[SkippedRow]

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics about the processing result.

        Returns:
            Dict[str, Any]: Summary statistics
        """
        sheets_with_issues = set(row.sheet_name for row in self.skipped_rows)

        return {
            "total_frames": len(self.dataframes),
            "total_rows": {name: len(df) for name, df in self.dataframes.items()},
            "total_skipped_rows": len(self.skipped_rows),
            "sheets_with_issues": len(sheets_with_issues),
            "sheets_with_issues_names": list(sheets_with_issues),
        }

# pylint: disable=too-few-public-methods
class FinancialRecord(BaseModel):
    """
    Base model for financial records validation.

    Attributes:
        Date: Transaction date
        Description: Transaction description
        Category: Transaction category
        Value: Transaction amount
    """

    Date: datetime
    Description: str
    Category: str
    Value: float

    class Config:
        """Pydantic configuration."""
        arbitrary_types_allowed = True


class SavingsRecord(FinancialRecord):
    """
    Model for savings records validation.

    Attributes:
        CategoryType: Type of savings category
    """

    CategoryType: str


@dataclass
class ProcessingStats:
    """
    Statistics from data processing operations.

    Attributes:
        source_rows: Number of rows in the source data
        processed_rows: Number of rows successfully processed
        invalid_rows: Number of rows that failed validation
        skipped_categories: List of categories that were skipped
        errors: List of errors encountered during processing
    """

    source_rows: int
    processed_rows: int
    invalid_rows: int = 0
    skipped_categories: Optional[List[str]] = None
    errors: Optional[List[str]] = None

    def __post_init__(self) -> None:
        """Initialize default values for lists."""
        if self.skipped_categories is None:
            self.skipped_categories = []
        if self.errors is None:
            self.errors = []
