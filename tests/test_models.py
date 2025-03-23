#!/filepath: tests/test_models.py
"""
Test suite for the data models.

This module contains tests for model classes in models.py.
"""
import os
import sys
import unittest
from datetime import datetime
from dataclasses import field

import polars as pl
import pytest
from pydantic import ValidationError

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.models import (  # pylint: disable=wrong-import-position,import-error
    ProcessingResult,
    SkippedRow,
    FinancialRecord,
    SavingsRecord,
    ProcessingStats,
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


class TestFinancialRecord(unittest.TestCase):
    """Tests for the FinancialRecord Pydantic model."""

    def test_financial_record_creation(self) -> None:
        """Test FinancialRecord can be created with valid data."""
        record = FinancialRecord(
            Date=datetime(2023, 1, 1),
            Description="Grocery shopping",
            Category="Food",
            Value=75.50
        )

        self.assertEqual(record.Date, datetime(2023, 1, 1))
        self.assertEqual(record.Description, "Grocery shopping")
        self.assertEqual(record.Category, "Food")
        self.assertEqual(record.Value, 75.50)

    def test_financial_record_validation(self) -> None:
        """Test that FinancialRecord validates data types."""
        # Should raise ValidationError with invalid date
        with self.assertRaises(ValidationError):
            FinancialRecord(
                Date="not-a-date",
                Description="Grocery shopping",
                Category="Food",
                Value=75.50
            )

        # Should raise ValidationError with invalid value
        with self.assertRaises(ValidationError):
            FinancialRecord(
                Date=datetime(2023, 1, 1),
                Description="Grocery shopping",
                Category="Food",
                Value="not-a-number"
            )

    def test_financial_record_to_dict(self) -> None:
        """Test that FinancialRecord can be converted to a dictionary."""
        record = FinancialRecord(
            Date=datetime(2023, 1, 1),
            Description="Grocery shopping",
            Category="Food",
            Value=75.50
        )
        
        record_dict = record.dict()
        self.assertIsInstance(record_dict, dict)
        self.assertEqual(record_dict["Date"], datetime(2023, 1, 1))
        self.assertEqual(record_dict["Description"], "Grocery shopping")
        self.assertEqual(record_dict["Category"], "Food")
        self.assertEqual(record_dict["Value"], 75.50)


class TestSavingsRecord(unittest.TestCase):
    """Tests for the SavingsRecord Pydantic model."""

    def test_savings_record_creation(self) -> None:
        """Test SavingsRecord can be created with valid data."""
        record = SavingsRecord(
            Date=datetime(2023, 1, 1),
            Description="Vacation fund",
            Category="Travel",
            CategoryType="Accantonamento",
            Value=200.00
        )

        self.assertEqual(record.Date, datetime(2023, 1, 1))
        self.assertEqual(record.Description, "Vacation fund")
        self.assertEqual(record.Category, "Travel")
        self.assertEqual(record.CategoryType, "Accantonamento")
        self.assertEqual(record.Value, 200.00)

    def test_savings_record_inheritance(self) -> None:
        """Test that SavingsRecord inherits from FinancialRecord."""
        record = SavingsRecord(
            Date=datetime(2023, 1, 1),
            Description="Vacation fund",
            Category="Travel",
            CategoryType="Accantonamento",
            Value=200.00
        )
        
        self.assertIsInstance(record, FinancialRecord)
        
    def test_savings_record_validation(self) -> None:
        """Test that SavingsRecord validates data types."""
        # Should raise ValidationError with missing CategoryType
        with self.assertRaises(ValidationError):
            SavingsRecord(
                Date=datetime(2023, 1, 1),
                Description="Vacation fund",
                Category="Travel",
                Value=200.00
            )


class TestProcessingStats(unittest.TestCase):
    """Tests for the ProcessingStats dataclass."""

    def test_processing_stats_creation(self) -> None:
        """Test ProcessingStats can be created with required attributes."""
        stats = ProcessingStats(source_rows=100, processed_rows=95)
        
        self.assertEqual(stats.source_rows, 100)
        self.assertEqual(stats.processed_rows, 95)
        self.assertEqual(stats.invalid_rows, 0)  # Default value
        self.assertEqual(stats.skipped_categories, [])  # Empty list by default
        self.assertEqual(stats.errors, [])  # Empty list by default

    def test_processing_stats_with_custom_values(self) -> None:
        """Test ProcessingStats can be created with custom values."""
        stats = ProcessingStats(
            source_rows=100,
            processed_rows=90,
            invalid_rows=10,
            skipped_categories=["Unknown", "Invalid"],
            errors=["Date parsing error", "Missing required field"]
        )
        
        self.assertEqual(stats.source_rows, 100)
        self.assertEqual(stats.processed_rows, 90)
        self.assertEqual(stats.invalid_rows, 10)
        self.assertEqual(stats.skipped_categories, ["Unknown", "Invalid"])
        self.assertEqual(stats.errors, ["Date parsing error", "Missing required field"])

    def test_processing_stats_default_lists(self) -> None:
        """Test that default lists are initialized correctly."""
        stats = ProcessingStats(source_rows=100, processed_rows=95)
        
        # Default lists should be initialized as empty lists
        self.assertEqual(stats.skipped_categories, [])
        self.assertEqual(stats.errors, [])
        
        # Lists should be mutable
        stats.skipped_categories.append("Unknown")
        stats.errors.append("Error message")
        
        self.assertEqual(stats.skipped_categories, ["Unknown"])
        self.assertEqual(stats.errors, ["Error message"])


if __name__ == "__main__":
    unittest.main()