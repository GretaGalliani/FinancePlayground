import polars as pl
from hypothesis import given, strategies as st
from hypothesis.extra.polars import data_frames
from expense_dashboard import (
    ExpenseDashboard,
)  # Assuming your main file is named expense_dashboard.py
import plotly.graph_objects as go

# Define strategies for our data
date_strategy = st.dates(min_value=pl.Date(2020, 1, 1), max_value=pl.Date(2025, 12, 31))
description_strategy = st.text(min_size=1, max_size=100)
category_strategy = st.sampled_from(
    ["Food", "Transport", "Entertainment", "Utilities", "Other"]
)
value_strategy = st.floats(
    min_value=0, max_value=10000, allow_nan=False, allow_infinity=False
)

# Create a strategy for our DataFrame
df_strategy = data_frames(
    [
        ("Date", date_strategy),
        ("Description", description_strategy),
        ("Category", category_strategy),
        ("Value", value_strategy),
    ],
    min_size=1,
    max_size=1000,
)


@given(df=df_strategy)
def test_expense_dashboard_initialization(df):
    dashboard = ExpenseDashboard(df)
    assert isinstance(dashboard.df, pl.DataFrame)
    assert list(dashboard.df.columns) == ["Date", "Description", "Category", "Value"]
    assert dashboard.df["Date"].dtype == pl.Date
    assert dashboard.df["Value"].dtype in (pl.Float32, pl.Float64)


@given(df=df_strategy)
def test_aggregate_expenses(df):
    dashboard = ExpenseDashboard(df)

    monthly_agg = dashboard._aggregate_expenses(frequency="monthly")
    assert isinstance(monthly_agg, pl.DataFrame)
    assert list(monthly_agg.columns) == ["Period", "Total"]
    assert len(monthly_agg) <= len(df["Date"].dt.strftime("%Y-%m").unique())

    weekly_agg = dashboard._aggregate_expenses(frequency="weekly")
    assert isinstance(weekly_agg, pl.DataFrame)
    assert list(weekly_agg.columns) == ["Period", "Total"]
    assert len(weekly_agg) <= len(df["Date"].dt.strftime("%Y-W%W").unique())


@given(df=df_strategy)
def test_create_figures(df):
    dashboard = ExpenseDashboard(df)

    monthly_fig = dashboard.create_figures(df, frequency="monthly")
    assert isinstance(monthly_fig, go.Figure)
    assert len(monthly_fig.data) == 4  # Bar chart, pie chart, table, and line chart

    weekly_fig = dashboard.create_figures(df, frequency="weekly")
    assert isinstance(weekly_fig, go.Figure)
    assert len(weekly_fig.data) == 4


@given(df=df_strategy)
def test_dashboard_layout(df):
    dashboard = ExpenseDashboard(df)

    assert dashboard.app.layout is not None
    assert "Monthly View" in str(dashboard.app.layout)
    assert "Weekly View" in str(dashboard.app.layout)
    assert "date-range" in dashboard.app.layout
    assert "category-filter" in dashboard.app.layout
    assert "dashboard-graph" in dashboard.app.layout


# Test for edge cases
@given(
    df=data_frames(
        [
            ("Date", st.just(pl.Date(2023, 1, 1))),
            ("Description", st.just("Test")),
            ("Category", st.just("Test")),
            ("Value", st.just(100.0)),
        ],
        min_size=1,
        max_size=1,
    )
)
def test_single_row_dataframe(df):
    dashboard = ExpenseDashboard(df)
    monthly_fig = dashboard.create_figures(df, frequency="monthly")
    weekly_fig = dashboard.create_figures(df, frequency="weekly")
    assert isinstance(monthly_fig, go.Figure)
    assert isinstance(weekly_fig, go.Figure)


# Test for invalid frequency
@given(df=df_strategy)
def test_invalid_frequency(df):
    dashboard = ExpenseDashboard(df)
    try:
        dashboard._aggregate_expenses(frequency="invalid")
        assert False, "Should have raised a ValueError"
    except ValueError:
        pass


if __name__ == "__main__":
    import pytest

    pytest.main([__file__])
