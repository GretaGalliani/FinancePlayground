import os
import yaml
import polars as pl

from src.Config import Config
from src.DataWrangler import DataWrangler
from src.Process import Process
from src.FinanceDashboard import FinanceDashboard


def main():
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "src/config.yaml")
    config = Config(config_path)

    # Initialize data wrangler and process data
    data_wrangler = DataWrangler(config)
    process = Process(config)

    try:
        # Load data from Numbers file
        raw_dfs = data_wrangler.load_updated_file()

        # Process expenses and income data
        df_spese = process.process_data(raw_dfs["spese"])
        df_entrate = process.process_data(raw_dfs["entrate"])

        # Initialize and run dashboard
        dashboard = FinanceDashboard(df_spese, df_entrate)
        dashboard.run_server(debug=True)

    except Exception as e:
        print(f"Error in main workflow: {e}")


if __name__ == "__main__":
    main()
