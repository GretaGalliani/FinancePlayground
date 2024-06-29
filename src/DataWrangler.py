import polars as pl
from numbers_parser import Document


class DataWrangler:
    def __init__(self, config):
        self.config = config  # type: ignore

    def load_updated_file(self):
        doc = Document(self.config.get("libro_spese_path"))

        # Get the first sheet which contains my finance data
        sheets = doc.sheets
        first_sheet = sheets[0]

        # Get the first and unique table in the first sheet
        tables = first_sheet.tables
        data = tables[0].rows(values_only=True)

        # Extract data from the first table
        columns = data[0]
        rows = data[1:]
        # Create Polars DataFrame. Remove null values
        df = pl.DataFrame(data=rows, schema=columns).drop_nulls()

        # Write the DataFrame to a CSV file
        df.write_csv(self.config.get("finance_raw_path"))
