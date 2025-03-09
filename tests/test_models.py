#!/filepath: tests/test_models.py
"""
Test suite for the data models.

This module contains tests for the SkippedRow and ProcessingResult classes in models.py.
"""
import os
import sys
import unittest

import polars as pl

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.models import (  # pylint: disable=wrong-import-position,import-error
    ProcessingResult,
    SkippedRow,
)


class TestSkippedRow(unittest.TestCase):
    """Tests for the SkippedRow dataclass."""

    def test_skipped_row_creation(self) -> None:
        """Test SkippedRow can be created with proper attributes."""
        row = SkippedRow(
            sheet_name="test_sheet",
            row_index=5,
            row_data=["data1", "data2", None],
            reason="Too many empty values",
        )

        self.assertEqual(row.sheet_name, "test_sheet")
        self.assertEqual(row.row_index, 5)
        self.assertEqual(row.row_data, ["data1", "data2", None])
        self.assertEqual(row.reason, "Too many empty values")

    def test_skipped_row_equality(self) -> None:
        """Test that two SkippedRow objects with the same attributes are equal."""
        row1 = SkippedRow(
            sheet_name="test_sheet",
            row_index=5,
            row_data=["data1", "data2", None],
            reason="Too many empty values",
        )

        row2 = SkippedRow(
            sheet_name="test_sheet",
            row_index=5,
            row_data=["data1", "data2", None],
            reason="Too many empty values",
        )

        self.assertEqual(row1, row2)

    def test_skipped_row_inequality(self) -> None:
        """Test that two SkippedRow objects with different attributes are not equal."""
        row1 = SkippedRow(
            sheet_name="test_sheet",
            row_index=5,
            row_data=["data1", "data2", None],
            reason="Too many empty values",
        )

        row2 = SkippedRow(
            sheet_name="different_sheet",
            row_index=5,
            row_data=["data1", "data2", None],
            reason="Too many empty values",
        )

        self.assertNotEqual(row1, row2)


class TestProcessingResult(unittest.TestCase):
    """Tests for the ProcessingResult dataclass."""

    def setUp(self) -> None:
        """Set up test data for ProcessingResult tests."""
        # Create sample DataFrames
        self.expenses_df = pl.DataFrame(
            {
                "Date": ["2023-01-01", "2023-01-02"],
                "Category": ["Food", "Transport"],
                "Amount": [100.0, 50.0],
            }
        )

        self.income_df = pl.DataFrame(
            {
                "Date": ["2023-01-15", "2023-01-30"],
                "Category": ["Salary", "Freelance"],
                "Amount": [3000.0, 500.0],
            }
        )

        # Create sample skipped rows
        self.skipped_rows = [
            SkippedRow(
                sheet_name="expenses",
                row_index=3,
                row_data=["2023-01-03", None, None],
                reason="Too many empty values",
            ),
            SkippedRow(
                sheet_name="income",
                row_index=4,
                row_data=[None, "Unknown", 250.0],
                reason="Missing date",
            ),
        ]

        # Create ProcessingResult
        self.dataframes = {"expenses": self.expenses_df, "income": self.income_df}
        self.result = ProcessingResult(
            dataframes=self.dataframes, skipped_rows=self.skipped_rows
        )

    def test_processing_result_creation(self) -> None:
        """Test ProcessingResult can be created with proper attributes."""
        self.assertEqual(self.result.dataframes, self.dataframes)
        self.assertEqual(self.result.skipped_rows, self.skipped_rows)

    def test_get_summary_method(self) -> None:
        """Test the get_summary method returns correct statistics."""
        summary = self.result.get_summary()

        # Check summary structure
        self.assertIsInstance(summary, dict)
        self.assertIn("total_frames", summary)
        self.assertIn("total_rows", summary)
        self.assertIn("total_skipped_rows", summary)
        self.assertIn("sheets_with_issues", summary)
        self.assertIn("sheets_with_issues_names", summary)

        # Check values
        self.assertEqual(summary["total_frames"], 2)
        self.assertEqual(summary["total_rows"], {"expenses": 2, "income": 2})
        self.assertEqual(summary["total_skipped_rows"], 2)
        self.assertEqual(summary["sheets_with_issues"], 2)
        self.assertCountEqual(
            summary["sheets_with_issues_names"], ["expenses", "income"]
        )  # pylint: disable=line-too-long

    def test_empty_processing_result(self) -> None:
        """Test ProcessingResult with empty data."""
        empty_result = ProcessingResult(dataframes={}, skipped_rows=[])
        summary = empty_result.get_summary()

        self.assertEqual(summary["total_frames"], 0)
        self.assertEqual(summary["total_rows"], {})
        self.assertEqual(summary["total_skipped_rows"], 0)
        self.assertEqual(summary["sheets_with_issues"], 0)
        self.assertEqual(summary["sheets_with_issues_names"], [])

    def test_processing_result_with_no_skipped_rows(self) -> None:
        """Test ProcessingResult with dataframes but no skipped rows."""
        result = ProcessingResult(dataframes=self.dataframes, skipped_rows=[])
        summary = result.get_summary()

        self.assertEqual(summary["total_frames"], 2)
        self.assertEqual(summary["total_skipped_rows"], 0)
        self.assertEqual(summary["sheets_with_issues"], 0)
        self.assertEqual(summary["sheets_with_issues_names"], [])


if __name__ == "__main__":
    unittest.main()
