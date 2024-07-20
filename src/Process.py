# pylint: disable=too-few-public-methods
import pandera.polars as pa
from pandera.typing.polars import DataFrame
import polars as pl


class Schema(pa.DataFrameModel):
    Date: pl.Date
    Description: pl.String
    Category: pl.String
    Value: pl.Float64


class Process:
    def __init__(self, config):
        self.config = config

    @pa.check_types
    def process_data(self, df: DataFrame) -> DataFrame[Schema]:
        # Remove month column
        df_no_month = df.drop("Mese")

        # Convert Data column to a sensible format
        df_date = self._convert_to_date(df_no_month)

        # Remove white spaces at the end of string text
        df_string = self._fix_string(df_date)

        # Change column name
        df_renamed = df_string.rename(
            {
                "Data": "Date",
                "Descrizione": "Description",
                "Categoria": "Category",
                "Importo": "Value",
            }
        )

        return df_renamed

    def _convert_to_date(
        self, df: pl.DataFrame, data_col: str = "Data"
    ) -> pl.DataFrame:
        """Function to convert a column in pl.Date format

        Args:
            df (pl.DataFrame): DataFrame to convert
            data_col (str, optional): Column containing the Date. Defaults to "Data".

        Returns:
            pl.DataFrame: DataFrame with the column data_col converted to pl.Date after extraction of yyyy-mm-dd
        """
        return df.with_columns(
            # Extract only the day
            pl.col(data_col)
            .str.extract(r"(\d{4}-\d{2}-\d{2})")
            .str.to_date(format="%Y-%m-%d")
        )

    def _fix_string(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Functions to format correctly string columns removing leading and trailing white spaces.

        Args:
            df (pl.DataFrame): DataFrame with string columns

        Returns:
            pl.DataFrame: DataFrame with string columns processed.
        """
        return df.with_columns(pl.col(pl.Utf8).str.strip_chars(" "))
