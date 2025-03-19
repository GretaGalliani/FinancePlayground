#!/filepath: models.py
"""Data models for finance applications."""

from dataclasses import dataclass
from typing import Any, Dict, List

import polars as pl


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
