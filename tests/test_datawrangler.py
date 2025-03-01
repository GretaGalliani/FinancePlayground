"""
Test suite for the DataWrangler class.

This module contains tests for the DataWrangler class using pytest and hypothesis
for property-based testing.
"""

import os
import pytest
import polars as pl
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
from numbers_parser import Document, Sheet, Table

from src.Config import Config
from src.DataWrangler import DataWrangler


class TestDataWrangler:
    """Test suite for the DataWrangler class."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config object for testing."""
        config_dict = {
            "numbers_file_path": "/path/to/test.numbers",
            "raw_expenses_path": "input/finance_raw_expenses.csv",
            "raw_income_path": "input/finance_raw_income.csv",
            "raw_savings_path": "input/finance_raw_savings.csv",
            "sheet_mappings": {
                "uscite": "expenses",
                "entrate": "income",
                "risparmi": "savings",
            },
        }

        config = MagicMock(spec=Config)
        config.get.side_effect = lambda key, default=None: config_dict.get(key, default)

        return config

    @pytest.fixture
    def mock_document(self):
        """Create a mock Document object for testing."""
        # Create mock sheets
        expense_sheet = MagicMock(spec=Sheet)
        expense_sheet.name = "Uscite"

        income_sheet = MagicMock(spec=Sheet)
        income_sheet.name = "Entrate"

        savings_sheet = MagicMock(spec=Sheet)
        savings_sheet.name = "Risparmi"

        # Create mock tables with test data
        expense_table = MagicMock(spec=Table)
        expense_table.rows.return_value = [
            ["Data", "Descrizione", "Categoria", "Importo"],
            ["01/01/24", "Test expense", "Groceries", "42,50 €"],
            ["02/01/24", "Another expense", "Utilities", "100,00 €"],
        ]
        expense_sheet.tables = [expense_table]

        income_table = MagicMock(spec=Table)
        income_table.rows.return_value = [
            ["Data", "Descrizione", "Categoria", "Importo"],
            ["05/01/24", "Test income", "Salary", "2000,00 €"],
            ["10/01/24", "Another income", "Dividends", "500,00 €"],
        ]
        income_sheet.tables = [income_table]

        savings_table = MagicMock(spec=Table)
        savings_table.rows.return_value = [
            ["Data", "Descrizione", "Categoria", "Account", "Importo", "Tipo"],
            [
                "01/01/24",
                "Initial savings",
                "General",
                "BBVA",
                "1000,00 €",
                "Trasferimento",
            ],
            [
                "10/01/24",
                "Monthly saving",
                "Vacation",
                "BBVA",
                "200,00 €",
                "Trasferimento",
            ],
        ]
        savings_sheet.tables = [savings_table]

        # Create mock document with sheets
        doc = MagicMock(spec=Document)
        doc.sheets = [expense_sheet, income_sheet, savings_sheet]

        return doc

    def test_init(self, mock_config):
        """Test DataWrangler initialization."""
        wrangler = DataWrangler(mock_config)
        assert wrangler.config == mock_config

    @patch("src.DataWrangler.Document")
    @patch("os.path.exists")
    def test_load_updated_file(
        self, mock_exists, mock_document_class, mock_config, mock_document
    ):
        """Test loading and processing data from Numbers file."""
        # Setup mocks
        mock_exists.return_value = True
        mock_document_class.return_value = mock_document

        # Create DataWrangler and call method
        wrangler = DataWrangler(mock_config)

        # Patch save_to_csv to avoid file operations
        with patch.object(wrangler, "_save_to_csv"):
            dfs = wrangler.load_updated_file()

        # Check results
        assert "expenses" in dfs
        assert "income" in dfs
        assert "savings" in dfs

        # Check expense DataFrame
        expense_df = dfs["expenses"]
        assert isinstance(expense_df, pl.DataFrame)
        assert len(expense_df) == 2
        assert list(expense_df.columns) == [
            "Data",
            "Descrizione",
            "Categoria",
            "Importo",
            "Mese",
        ]

        # Check income DataFrame
        income_df = dfs["income"]
        assert isinstance(income_df, pl.DataFrame)
        assert len(income_df) == 2
        assert list(income_df.columns) == [
            "Data",
            "Descrizione",
            "Categoria",
            "Importo",
            "Mese",
        ]

        # Check savings DataFrame
        savings_df = dfs["savings"]
        assert isinstance(savings_df, pl.DataFrame)
        assert len(savings_df) == 2
        assert list(savings_df.columns) == [
            "Data",
            "Descrizione",
            "Categoria",
            "Account",
            "Importo",
            "Tipo",
        ]

    def test_process_sheet_data(self, mock_config):
        """Test processing sheet data into a DataFrame."""
        wrangler = DataWrangler(mock_config)

        # Test data
        test_data = [
            ["Data", "Descrizione", "Categoria", "Importo"],
            ["01/01/24", "Test row 1", "Category A", "100,00 €"],
            ["02/01/24", "Test row 2", "Category B", "200,50 €"],
            ["03/01/24", "Test row 3", "Category C", "300,75 €"],
        ]

        # Process test data
        df = wrangler._process_sheet_data(test_data, "Test Sheet")

        # Check results
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["Data", "Descrizione", "Categoria", "Importo"]

        # Check numeric conversion
        assert df["Importo"].dtype == pl.Float64
        assert df["Importo"][0] == 100.0
        assert df["Importo"][1] == 200.5
        assert df["Importo"][2] == 300.75

    def test_process_sheet_data_missing_values(self, mock_config):
        """Test processing sheet data with missing values."""
        wrangler = DataWrangler(mock_config)

        # Test data with missing values
        test_data = [
            ["Data", "Descrizione", "Categoria", "Importo"],
            ["01/01/24", "Test row 1", None, "100,00 €"],
            ["02/01/24", "Test row 2", "Category B", None],
            [None, None, None, None],  # This row should be skipped
            ["03/01/24", "Test row 3", "Category C", "300,75 €"],
        ]

        # Process test data
        df = wrangler._process_sheet_data(test_data, "Test Sheet")

        # Check results - the all-null row should be skipped
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3

    def test_process_sheet_data_empty(self, mock_config):
        """Test processing empty sheet data."""
        wrangler = DataWrangler(mock_config)

        # Empty test data
        test_data = []

        # Process should raise ValueError
        with pytest.raises(ValueError, match="No data found"):
            wrangler._process_sheet_data(test_data, "Empty Sheet")

    def test_clean_dataframe(self, mock_config):
        """Test cleaning and preparing a DataFrame."""
        wrangler = DataWrangler(mock_config)

        # Create test DataFrame
        test_df = pl.DataFrame(
            {
                "Data": ["01/01/24", "02/01/24", "03/01/24"],
                "Descrizione": ["Test item 1", "", "Test item 3"],
                "Categoria": ["Food", "Utilities", "Transport"],
                "Importo": ["42,50 €", "100 €", "-25,75 €"],
            }
        )

        # Use a mock to set sheet_name in meta
        df_with_meta = test_df.clone()
        df_with_meta.meta = {"sheet_name": "uscite"}

        # Clean DataFrame
        cleaned_df = wrangler._clean_dataframe(df_with_meta, "Uscite")

        # Check results
        assert cleaned_df["Importo"].dtype == pl.Float64
        assert cleaned_df["Importo"][0] == 42.5
        assert cleaned_df["Importo"][1] == 100.0
        assert cleaned_df["Importo"][2] == -25.75

        # Empty string should be replaced with None
        assert cleaned_df["Descrizione"][1] is None

    def test_save_to_csv(self, mock_config):
        """Test saving DataFrame to CSV."""
        wrangler = DataWrangler(mock_config)

        # Create test DataFrame
        test_df = pl.DataFrame({"Data": ["01/01/24", "02/01/24"], "Value": [100, 200]})

        # Create temporary directory for test
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = os.path.join(temp_dir, "test_output.csv")

            # Save DataFrame
            wrangler._save_to_csv(test_df, test_path)

            # Check if file was created
            assert os.path.exists(test_path)

            # Read back and verify
            df_read = pl.read_csv(test_path)
            assert df_read.shape == test_df.shape
            assert list(df_read.columns) == list(test_df.columns)

    def test_file_not_found(self, mock_config):
        """Test handling of missing Numbers file."""
        wrangler = DataWrangler(mock_config)

        # Mock os.path.exists to return False (file not found)
        with patch("os.path.exists", return_value=False):
            with pytest.raises(FileNotFoundError):
                wrangler.load_updated_file()

    def test_invalid_data(self, mock_config):
        """Test handling of invalid data."""
        wrangler = DataWrangler(mock_config)

        # Test data with no rows (only headers)
        test_data = [["Data", "Descrizione", "Categoria", "Importo"]]

        with pytest.raises(ValueError, match="No data rows found"):
            wrangler._process_sheet_data(test_data, "Invalid Sheet")

        # Test data with invalid headers
        test_data = [[None, None, None, None], ["01/01/24", "Test", "Category", "100"]]

        with pytest.raises(ValueError, match="Invalid or missing column headers"):
            wrangler._process_sheet_data(test_data, "Invalid Sheet")
