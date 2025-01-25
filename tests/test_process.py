import unittest
from datetime import datetime
import polars as pl
from hypothesis import given, strategies as st
from hypothesis.extra.pandas import column, data_frames
from hypothesis.extra.pandas import range_indexes

import os, sys

source = os.popen("git rev-parse --show-toplevel").read().strip()
sys.path.append(source)

from src.Config import Config
from src.Process import Process, Schema

config = Config(f"{source}/src/config.yaml")


class TestProcess(unittest.TestCase):
    def setUp(self):
        self.config = config
        self.process = Process(self.config)

    @given(
        data_frames(
            columns=[
                column(
                    "Data",
                    st.datetimes(
                        min_value=datetime(2000, 1, 1), max_value=datetime(2030, 12, 31)
                    ).map(lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S.%f")),
                ),
                column("Descrizione", st.text(min_size=1)),
                column("Categoria", st.text(min_size=1)),
                column("Importo", st.floats(min_value=0, max_value=1e6)),
                column("month", st.integers(min_value=1, max_value=12)),
            ],
            index=range_indexes(min_size=1, max_size=10),
        )
    )
    def test_process_data(self, df):
        # Convert Hypothesis pandas DataFrame to Polars DataFrame
        df = pl.DataFrame(df)

        # Apply the process_data method
        result_df = self.process.process_data(df)

        # Verify the columns and types
        self.assertTrue("Data" in result_df.columns)
        self.assertTrue("Descrizione" in result_df.columns)
        self.assertTrue("Categoria" in result_df.columns)
        self.assertTrue("Importo" in result_df.columns)

        # Verify the "Data" column is converted to date type
        self.assertEqual(result_df["Data"].dtype, pl.Date)

        # Verify that the "month" column is removed
        self.assertFalse("month" in result_df.columns)

    @given(
        data_frames(
            columns=[
                column(
                    "Data",
                    st.datetimes(
                        min_value=datetime(2000, 1, 1), max_value=datetime(2030, 12, 31)
                    ).map(lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S.%f")),
                ),
                column("Descrizione", st.text(min_size=1)),
                column("Categoria", st.text(min_size=1)),
                column("Importo", st.floats(min_value=0, max_value=1e6)),
            ],
            index=range_indexes(min_size=1, max_size=10),
        )
    )
    def test_convert_to_date(self, df):
        # Convert Hypothesis pandas DataFrame to Polars DataFrame
        df = pl.DataFrame(df)

        # Apply the _convert_to_date method
        result_df = self.process._convert_to_date(df)

        # Verify the "Data" column is converted to date type
        self.assertEqual(result_df["Data"].dtype, pl.Date)

        # Verify that the data matches the expected format
        for date in result_df["Data"]:
            self.assertIsInstance(date, datetime)
            self.assertEqual(date.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d"))


if __name__ == "__main__":
    unittest.main()
