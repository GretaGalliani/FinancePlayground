#!/filepath: src/dashboard/callbacks.py
"""
Dashboard callbacks module for handling data loading and interactive components.

This module contains the DatasetLoader class for handling dataset operations
and the callback functions for the Dash dashboard interactions. It provides
data loading, filtering, and callback management functionality for the
financial dashboard.
"""

import calendar
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import dash
import polars as pl
from dash import Input, Output


class DatasetLoader:
    """
    Handles loading and filtering datasets for the dashboard.

    This class is responsible for loading CSV files specified in configuration
    and providing filtered views of the data based on date ranges.
    """

    def __init__(self, config: Any, logger: logging.Logger):
        """
        Initialize the dataset loader.

        Args:
            config: Configuration object containing file paths
            logger: Logger instance for logging data operations
        """
        self.config = config
        self.logger = logger.getChild("DatasetLoader")
        self.datasets: Dict[str, Optional[pl.DataFrame]] = {}
        self.min_date: datetime = datetime.now()
        self.max_date: datetime = datetime.now()
        self.min_month: str = ""
        self.max_month: str = ""

    def load_all_datasets(self) -> None:
        """Load all datasets required for visualization and determine date range."""
        # Load monthly summary data
        self.datasets["monthly_summary"] = self._load_csv("monthly_summary_path")

        # Load expense and income breakdowns
        self.datasets["expenses_by_category"] = self._load_csv(
            "expenses_by_category_path"
        )
        self.datasets["expenses_stacked"] = self._load_csv("expenses_stacked_path")
        self.datasets["income_by_category"] = self._load_csv("income_by_category_path")
        self.datasets["income_stacked"] = self._load_csv("income_stacked_path")

        # Load savings data
        self.datasets["savings_metrics"] = self._load_csv("savings_metrics_path")
        self.datasets["savings_by_category"] = self._load_csv(
            "savings_by_category_path"
        )
        self.datasets["savings_allocation"] = self._load_csv("savings_allocation_path")
        self.datasets["processed_savings"] = self._load_csv("processed_savings_path")

        # Load processed raw data for filtering by date
        self.datasets["processed_expenses"] = self._load_csv("processed_expenses_path")
        self.datasets["processed_income"] = self._load_csv("processed_income_path")

        # Determine date range
        self._determine_date_range()

    def _load_csv(self, config_key: str) -> Optional[pl.DataFrame]:
        """
        Load a CSV file specified in the configuration.

        Args:
            config_key: Key in the configuration for the file path

        Returns:
            pl.DataFrame or None: DataFrame if the file exists, None otherwise
        """
        path = self.config.get(config_key)
        if not path or not os.path.exists(path):
            return None

        try:
            return pl.read_csv(path)
        except Exception as e:
            self.logger.error(f"Error loading {path}: {str(e)}")
            return None

    def _determine_date_range(self) -> None:
        """Determine the min and max dates from the loaded data."""
        monthly_summary = self.datasets["monthly_summary"]

        if monthly_summary is not None and len(monthly_summary) > 0:
            self.min_month = monthly_summary["Month"].min()
            self.max_month = monthly_summary["Month"].max()

            # Convert to date objects for the date picker
            self.min_date = datetime.strptime(f"{self.min_month}-01", "%Y-%m-%d")
            self.max_date = datetime.strptime(f"{self.max_month}-01", "%Y-%m-%d")
            # Add a month to the max date to include the full month
            self.max_date = (self.max_date.replace(day=28) + timedelta(days=4)).replace(
                day=1
            ) - timedelta(days=1)
        else:
            # Default to current month if no data
            today = datetime.now()
            self.min_date = today.replace(day=1) - timedelta(days=180)  # 6 months ago
            self.max_date = today
            self.min_month = self.min_date.strftime("%Y-%m")
            self.max_month = self.max_date.strftime("%Y-%m")

    def filter_monthly_dataset(
        self, config_key: str, start_month: str, end_month: str
    ) -> Optional[pl.DataFrame]:
        """
        Filter a dataset by month range.

        Args:
            config_key: Key in the configuration for the file path
            start_month: Start month in 'YYYY-MM' format
            end_month: End month in 'YYYY-MM' format

        Returns:
            pl.DataFrame or None: Filtered DataFrame
        """
        df = self._load_csv(config_key)
        if df is None or len(df) == 0 or "Month" not in df.columns:
            return df

        return df.filter(
            (pl.col("Month") >= start_month) & (pl.col("Month") <= end_month)
        )

    def filter_daily_dataset(
        self, config_key: str, start_date: datetime, end_date: datetime
    ) -> Optional[pl.DataFrame]:
        """
        Filter a dataset by date range.

        Args:
            config_key: Key in the configuration for the file path
            start_date: Start date
            end_date: End date

        Returns:
            pl.DataFrame or None: Filtered DataFrame
        """
        df = self._load_csv(config_key)
        if df is None or len(df) == 0 or "Date" not in df.columns:
            return df

        # Convert string dates to datetime if needed
        if df["Date"].dtype == pl.Utf8:
            df = df.with_columns(pl.col("Date").str.to_datetime().alias("Date"))

        return df.filter((pl.col("Date") >= start_date) & (pl.col("Date") <= end_date))

    def get_dataset(self, name: str) -> Optional[pl.DataFrame]:
        """
        Get a loaded dataset by name.

        Args:
            name: Name of the dataset

        Returns:
            pl.DataFrame or None: The requested dataset
        """
        return self.datasets.get(name)

    def calculate_category_breakdown(
        self,
        dataset_key: str,
        start_date: datetime,
        end_date: datetime,
        is_income: bool = False,
    ) -> Optional[pl.DataFrame]:
        """
        Calculate category breakdown based on date range.

        Args:
            dataset_key: Key for the raw dataset in self.datasets
            start_date: Start date
            end_date: End date
            is_income: Whether this is income data (affects sign)

        Returns:
            pl.DataFrame or None: Filtered and aggregated DataFrame with category breakdown
        """
        df = self.datasets.get(dataset_key)
        if (
            df is None
            or len(df) == 0
            or "Date" not in df.columns
            or "Category" not in df.columns
        ):
            self.logger.warning(
                f"Cannot calculate category breakdown: invalid dataset {dataset_key}"
            )
            return None

        # Ensure Date is datetime
        if df["Date"].dtype == pl.Utf8:
            df = df.with_columns(pl.col("Date").str.to_datetime().alias("Date"))

        # Filter by date range
        filtered_df = df.filter(
            (pl.col("Date") >= start_date) & (pl.col("Date") <= end_date)
        )

        if len(filtered_df) == 0:
            self.logger.warning(f"No data in date range for {dataset_key}")
            return None

        # Group by category and sum values
        result = (
            filtered_df.groupby("Category")
            .agg(pl.sum("Value").alias("Total"))
            .sort("Total", descending=True)
        )

        return result


def setup_callbacks(dashboard_instance) -> None:
    """
    Set up the dashboard callbacks to respond to user interactions.

    Args:
        dashboard_instance: The FinanceDashboard instance containing the app and other components
    """

    @dashboard_instance.app.callback(
        [
            Output("summary-cards", "children"),
            Output("main-dashboard", "figure"),
            Output("expense-pie-chart", "figure"),
            Output("income-pie-chart", "figure"),
            Output("stacked-expenses", "figure"),
            Output("stacked-income", "figure"),
            Output("savings-overview", "figure"),
            Output("savings-breakdown", "figure"),
            Output("monthly-savings-rate", "figure"),
            Output("allocation-vs-risparmio", "figure"),
        ],
        [
            Input("start-month-dropdown", "value"),
            Input("end-month-dropdown", "value"),
        ],
    )
    def update_dashboard(start_month: str, end_month: str) -> Tuple[Any, ...]:
        """
        Update all dashboard components based on the selected month range.

        Args:
            start_month: Start month as string (YYYY-MM-DD format)
            end_month: End month as string (YYYY-MM-DD format)

        Returns:
            Tuple containing all dashboard components in the order of the Output callbacks
        """
        try:
            parsed_start_date = datetime.strptime(start_month, "%Y-%m-%d")
            parsed_end_date = datetime.strptime(end_month, "%Y-%m-%d")

            # For end date, we want the last day of the selected month
            last_day = calendar.monthrange(parsed_end_date.year, parsed_end_date.month)[
                1
            ]
            parsed_end_date = datetime(
                parsed_end_date.year, parsed_end_date.month, last_day
            )

        except ValueError as e:
            dashboard_instance.logger.error(f"Error parsing dates: {str(e)}")
            # Use default dates in case of error
            parsed_start_date = dashboard_instance.dataset_loader.min_date
            parsed_end_date = dashboard_instance.dataset_loader.max_date

        # Convert to month format for filtering monthly data
        start_month_str = parsed_start_date.strftime("%Y-%m")
        end_month_str = parsed_end_date.strftime("%Y-%m")

        # Filter datasets by date range
        filtered_monthly_summary = (
            dashboard_instance.dataset_loader.filter_monthly_dataset(
                "monthly_summary_path", start_month_str, end_month_str
            )
        )

        # For the savings metrics, use all data from the beginning up to the selected end date
        filtered_savings_metrics = (
            dashboard_instance.dataset_loader.filter_monthly_dataset(
                "savings_metrics_path",
                dashboard_instance.dataset_loader.min_month,
                end_month_str,
            )
        )

        filtered_expenses_stacked = (
            dashboard_instance.dataset_loader.filter_monthly_dataset(
                "expenses_stacked_path", start_month_str, end_month_str
            )
        )
        filtered_income_stacked = (
            dashboard_instance.dataset_loader.filter_monthly_dataset(
                "income_stacked_path", start_month_str, end_month_str
            )
        )

        # For savings data, include all transactions up to the end date
        filtered_processed_savings = (
            dashboard_instance.dataset_loader.filter_daily_dataset(
                "processed_savings_path",
                dashboard_instance.dataset_loader.min_date,
                parsed_end_date,
            )
        )

        # Calculate category breakdowns based on the filtered date range
        filtered_expenses_by_category = (
            dashboard_instance.dataset_loader.calculate_category_breakdown(
                "processed_expenses",
                parsed_start_date,
                parsed_end_date,
                is_income=False,
            )
        )

        filtered_income_by_category = (
            dashboard_instance.dataset_loader.calculate_category_breakdown(
                "processed_income",
                parsed_start_date,
                parsed_end_date,
                is_income=True,
            )
        )

        # Create unified summary cards with both financial and savings data
        summary_cards = dashboard_instance.card_creator.create_summary_cards(
            filtered_monthly_summary, filtered_savings_metrics
        )

        # Create all other dashboard elements
        fig_main_overview = dashboard_instance.chart_factory.create_monthly_overview(
            filtered_monthly_summary
        )

        # Create category visualizations
        fig_expense_pie_chart = dashboard_instance.chart_factory.create_category_donut(
            filtered_expenses_by_category,
            "Expense Breakdown by Category",
        )
        fig_income_pie_chart = dashboard_instance.chart_factory.create_category_donut(
            filtered_income_by_category,
            "Income Breakdown by Category",
            is_income=True,
        )

        # Create stacked charts for time series
        fig_stacked_expenses = dashboard_instance.chart_factory.create_stacked_bar(
            filtered_expenses_stacked, "Monthly Expense Breakdown", "Expenses"
        )
        fig_stacked_income = dashboard_instance.chart_factory.create_stacked_bar(
            filtered_income_stacked,
            "Monthly Income Breakdown",
            "Income",
            is_income=True,
        )

        # Create savings elements
        fig_savings_overview = (
            dashboard_instance.chart_factory.create_savings_overview_area(
                filtered_savings_metrics, filtered_processed_savings
            )
        )

        # Pass the end date to the savings breakdown method
        fig_savings_breakdown = (
            dashboard_instance.chart_factory.create_category_savings_breakdown(
                filtered_processed_savings, parsed_end_date
            )
        )

        # Create new Monthly Savings Rate chart with detailed savings data
        fig_monthly_savings_rate = (
            dashboard_instance.chart_factory.create_monthly_savings_rate(
                filtered_monthly_summary,
                filtered_savings_metrics,
                filtered_processed_savings,
            )
        )

        # Create new Allocation Breakdown by Category chart
        fig_allocation_vs_risparmio = (
            dashboard_instance.chart_factory.create_allocation_breakdown_by_category(
                filtered_processed_savings, parsed_end_date
            )
        )

        return (
            summary_cards,
            fig_main_overview,
            fig_expense_pie_chart,
            fig_income_pie_chart,
            fig_stacked_expenses,
            fig_stacked_income,
            fig_savings_overview,
            fig_savings_breakdown,
            fig_monthly_savings_rate,
            fig_allocation_vs_risparmio,
        )
