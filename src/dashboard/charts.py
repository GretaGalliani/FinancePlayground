"""
Chart components for the financial dashboard.

This module contains classes responsible for creating and styling charts
and visualizations used in the financial dashboard. It provides consistent
styling and chart factory methods for all dashboard visualizations.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import plotly.graph_objects as go
import polars as pl
from dash import dash_table, html

from category_mapper import CategoryMapper


class ChartStyler:
    """
    Handles styling of charts and visualizations.

    This class is responsible for applying consistent styling to
    all charts in the dashboard.
    """

    def __init__(self, config: Any):
        """
        Initialize the chart styler.

        Args:
            config: Dashboard configuration containing styling parameters
        """
        self.config = config

    def apply_styling(self, fig: go.Figure, title: str) -> go.Figure:
        """
        Apply consistent styling with purple titles and dark text for legends.

        Args:
            fig: Plotly figure to style
            title: Title for the figure

        Returns:
            go.Figure: Styled figure
        """
        # Get chart styling parameters from config
        chart_config = self.config.chart_styling

        # Apply consistent styling to the figure
        fig.update_layout(
            title={
                "text": title,
                "font": {
                    "color": self.config.color_theme.get("headline", "#6C3BCE"),
                    "family": self.config.fonts.get("title_font", "Montserrat"),
                    "size": 20,
                    "weight": "bold",  # Make title bold
                },
            },
            plot_bgcolor=self.config.color_theme["background"],
            paper_bgcolor=self.config.color_theme["background"],
            font=dict(
                color=self.config.color_theme.get(
                    "text", "#232323"
                ),  # Dark text for all elements
                family=self.config.fonts.get("body_font", "Open Sans"),
            ),
            yaxis=dict(
                titlefont=dict(color=self.config.color_theme.get("text", "#232323")),
                tickfont=dict(color=self.config.color_theme.get("text", "#232323")),
                gridcolor="rgba(35, 35, 35, 0.05)",  # Lighter gridlines for white background
                # Format y-axis ticks with euro symbol
                tickformat="€%{y:,.2f}",
            ),
            xaxis=dict(
                titlefont=dict(color=self.config.color_theme.get("text", "#232323")),
                tickfont=dict(color=self.config.color_theme.get("text", "#232323")),
                gridcolor="rgba(35, 35, 35, 0.05)",  # Lighter gridlines for white background
            ),
            legend=dict(
                orientation=chart_config.get("legend_orientation", "h"),
                yanchor=chart_config.get("legend_yanchor", "bottom"),
                y=chart_config.get("legend_y", -0.2),
                xanchor=chart_config.get("legend_xanchor", "center"),
                x=chart_config.get("legend_x", 0.5),
                bgcolor=self.config.color_theme.get("background", "#FFFFFF"),
                font=dict(
                    color=self.config.color_theme.get("text", "#232323")
                ),  # Dark text for legend
                bordercolor="#E2E8F0",  # Subtle border
                borderwidth=1,
            ),
            margin=dict(
                l=chart_config.get("margin_left", 50),
                r=chart_config.get("margin_right", 50),
                t=chart_config.get("margin_top", 60),
                b=chart_config.get("margin_bottom", 80),
            ),
            hoverlabel=dict(
                bgcolor="white",
                font_size=chart_config.get("hover_font_size", 14),
                font_family=chart_config.get("hover_font_family", "Open Sans"),
                font=dict(
                    color=self.config.color_theme.get("text", "#232323")
                ),  # Dark text for hover labels
            ),
            # Use unified hover mode to show date only once at the top
            hovermode="x unified",
        )

        return fig


class ChartFactory:
    """
    Factory for creating different types of charts and visualizations.

    This class is responsible for generating all chart components
    for the dashboard using the provided datasets with consistent category colors.
    """

    def __init__(
        self,
        color_theme: Dict[str, Any],
        chart_styler: Any,
        category_mapper: CategoryMapper,
    ):
        """
        Initialize the chart factory.

        Args:
            color_theme: Color theme for the charts
            chart_styler: Chart styler instance for consistent styling
            category_mapper: CategoryMapper for consistent category colors
        """
        self.color_theme = color_theme
        self.chart_styler = chart_styler
        self.category_mapper = category_mapper
        self.logger = logging.getLogger(__name__)

    def create_category_donut(
        self, df_categories: Optional[pl.DataFrame], title: str, is_income: bool = False
    ) -> go.Figure:
        """
        Create a donut chart for expense or income categories with consistent colors.

        Args:
            df_categories: DataFrame with category data
            title: Chart title
            is_income: Whether this is an income chart (affects colors)

        Returns:
            go.Figure: Plotly figure for the dashboard
        """
        if df_categories is None or len(df_categories) == 0:
            fig = go.Figure()
            fig.update_layout(
                title=f"{title} - No Data Available",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text=f"No {title.lower()} data available",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Create a donut chart for categories
        labels = df_categories["Category"].to_list()
        values = df_categories["Total"].to_list()

        # Get consistent colors for the categories
        if is_income:
            colors = self.category_mapper.get_income_colors(labels)
        else:
            colors = self.category_mapper.get_expense_colors(labels)

        # Create custom hover text with formatted values
        hover_texts = []
        for label, value in zip(labels, values):
            percentage = (value / sum(values) * 100) if sum(values) > 0 else 0
            hover_texts.append(f"{label}: {value:,.2f}€ ({percentage:.1f}%)")

        fig = go.Figure(
            data=[
                go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.6,  # Keep large hole for elegant donut
                    textinfo="label",  # Show category labels on chart
                    marker=dict(colors=colors),
                    textposition="outside",
                    textfont=dict(size=12),
                    hovertext=hover_texts,
                    hoverinfo="text",
                )
            ]
        )

        # First apply basic styling
        fig = self.chart_styler.apply_styling(fig, title)

        # Then add specific layout for pie charts with external legend
        fig.update_layout(
            # Set a fixed height for both charts to ensure they're the same size
            height=550,
            # Create plenty of space below for the legend
            margin=dict(t=80, b=200, l=5, r=5),
            # Force the legend outside the plot area
            legend=dict(
                orientation="h",
                y=-0.3,  # Position below the plot area
                yanchor="top",
                x=0.5,
                xanchor="center",
                bordercolor="#E2E8F0",
                borderwidth=1,
            ),
        )

        # Position the text labels to avoid overlap with legend
        fig.update_traces(
            insidetextfont=dict(size=12),
            outsidetextfont=dict(size=12),
        )

        return fig

    def create_stacked_bar(
        self,
        df_stacked: Optional[pl.DataFrame],
        title: str,
        value_column: str,
        is_income: bool = False,
    ) -> go.Figure:
        """
        Create a stacked bar chart with consistent category colors.

        Args:
            df_stacked: DataFrame with stacked data
            title: Chart title
            value_column: Column containing the values
            is_income: Whether this is an income chart (affects colors)

        Returns:
            go.Figure: Plotly figure for the dashboard
        """
        if df_stacked is None or len(df_stacked) == 0:
            fig = go.Figure()
            fig = self.chart_styler.apply_styling(fig, f"{title} - No Data Available")
            fig.update_layout(
                yaxis=dict(title="Amount (€)"),
                annotations=[
                    dict(
                        text="No data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        unique_categories = df_stacked["Category"].unique().to_list()

        # Get consistent colors for the categories
        if is_income:
            color_map = {
                category: self.category_mapper.get_income_category_color(category)
                for category in unique_categories
            }
        else:
            color_map = {
                category: self.category_mapper.get_expense_category_color(category)
                for category in unique_categories
            }

        # Get all months for consistent x-axis
        all_months = sorted(df_stacked["Month"].unique().to_list())

        # Format months for display
        months = []
        for month_str in all_months:
            year, month = month_str.split("-")
            month_name = [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ][int(month) - 1]
            months.append(f"{month_name} {year}")

        # Create monthly data structure and calculate percentages
        monthly_data = {}
        for month_display, month_raw in zip(months, all_months):
            month_df = df_stacked.filter(pl.col("Month") == month_raw)
            total = month_df[value_column].sum()

            # Get category data sorted by value
            cat_data = []
            for category in unique_categories:
                cat_df = month_df.filter(pl.col("Category") == category)
                if len(cat_df) > 0:
                    value = cat_df[value_column].sum()
                    if value > 0:  # Only include categories with values
                        percentage = (value / total * 100) if total > 0 else 0
                        cat_data.append(
                            {
                                "category": category,
                                "value": value,
                                "percentage": percentage,
                                "color": color_map[category],
                            }
                        )

            # Sort by value (largest first)
            cat_data.sort(key=lambda x: x["value"], reverse=True)

            monthly_data[month_display] = {"total": total, "categories": cat_data}

        # Create the figure
        fig = go.Figure()

        # For each category, add a trace to the stacked bar with custom hover text
        for category in unique_categories:
            values = []
            hover_texts = []

            # Create values and hover texts for each month for this category
            for month in months:
                # Get this category's value for the month
                cat_value = 0
                cat_percentage = 0
                month_total = monthly_data[month]["total"]

                for cat in monthly_data[month]["categories"]:
                    if cat["category"] == category:
                        cat_value = cat["value"]
                        cat_percentage = cat["percentage"]
                        break

                values.append(cat_value)

                # Create the hover text for this segment with month total, category value, and percentage
                hover_text = (
                    f"<b>{month}</b><br>"
                    + f"<b>Total:</b> {month_total:,.2f}€<br>"
                    + f"<b>{category}:</b> {cat_value:,.2f}€<br>"
                    + f"<b>Percentage:</b> {cat_percentage:.1f}%"
                )

                hover_texts.append(hover_text)

            # Only add trace if there are values
            if sum(values) > 0:
                fig.add_trace(
                    go.Bar(
                        x=months,
                        y=values,
                        name=category,
                        marker_color=color_map[category],  # Use consistent color
                        hoverinfo="text",
                        hovertext=hover_texts,
                        text=None,
                        textposition="none",  # Don't show text on bars to avoid clutter
                    )
                )

        # Style the figure
        fig = self.chart_styler.apply_styling(fig, title)

        # Set layout properties
        fig.update_layout(
            barmode="stack",
            yaxis=dict(title="Amount (€)"),
            hovermode="closest",  # Use closest mode for best hover behavior
            # Standard legend positioning
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.2,
                xanchor="center",
                x=0.5,
            ),
        )

        return fig

    def create_savings_overview_area(
        self,
        df_savings_metrics: Optional[pl.DataFrame],
        df_savings: Optional[pl.DataFrame],
    ) -> go.Figure:
        """
        Create a savings overview area chart showing each savings category's balance over time.

        Args:
            df_savings_metrics: DataFrame with savings metrics over time
            df_savings: DataFrame with detailed savings transactions

        Returns:
            go.Figure: Area chart showing category balances over time
        """
        if df_savings is None or len(df_savings) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Savings Categories Over Time - No Data Available",
                yaxis=dict(title="Amount (€)"),
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Filter for "Risparmio" categories only
        df_risparmio = df_savings.filter(pl.col("CategoryType") == "Risparmio")

        if len(df_risparmio) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Savings Categories Over Time - No Savings Data Available",
                yaxis=dict(title="Amount (€)"),
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings categorized as 'Risparmio' available",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Ensure we have Month column
        if "Month" not in df_risparmio.columns:
            df_risparmio = df_risparmio.with_columns(
                pl.col("Date").dt.strftime("%Y-%m").alias("Month")
            )

        # Get all months and categories
        months = sorted(df_risparmio["Month"].unique().to_list())
        categories = sorted(df_risparmio["Category"].unique().to_list())

        # Get consistent colors for savings categories
        category_colors = {
            cat: self.category_mapper.get_savings_category_color(cat)
            for cat in categories
        }

        # Calculate end-of-month balance for each category
        monthly_balances = {}
        category_balances = {cat: 0.0 for cat in categories}

        for month in months:
            # Update balances for each category based on this month's transactions
            month_data = df_risparmio.filter(pl.col("Month") == month)

            for category in categories:
                # Calculate net change for this category in this month
                cat_data = month_data.filter(pl.col("Category") == category)
                if len(cat_data) > 0:
                    net_change = cat_data["Value"].sum()
                    category_balances[category] += net_change

            # Store a copy of the current balances for this month
            monthly_balances[month] = category_balances.copy()

        # Create figure with traces for each category
        fig = go.Figure()

        for category in categories:
            x_values = months
            y_values = [monthly_balances[month][category] for month in months]

            fig.add_trace(
                go.Scatter(
                    x=x_values,
                    y=y_values,
                    name=category,
                    mode="lines",
                    line=dict(width=0.5, color=category_colors[category]),
                    fill="tonexty",  # Fill to next y trace
                    stackgroup="one",  # This makes it stack
                    hovertemplate="%{x}<br>%{y:,.2f}€<extra>" + category + "</extra>",
                )
            )

        # Style the figure
        fig = self.chart_styler.apply_styling(
            fig, "Savings Categories Balance Over Time"
        )
        fig.update_layout(
            yaxis=dict(title="Balance Amount (€)"),
            hovermode="x unified",
            legend=dict(
                orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5
            ),
        )

        return fig

    def create_category_savings_breakdown(
        self, df_savings: Optional[pl.DataFrame], end_date: datetime
    ) -> go.Figure:
        """
        Create a pie chart showing the breakdown of savings categories at the latest selected month.

        Args:
            df_savings: DataFrame with savings data
            end_date: End date of the selected period

        Returns:
            go.Figure: Pie chart showing savings categories breakdown
        """
        if df_savings is None or len(df_savings) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Savings Breakdown by Category - No Data Available",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Filter to only include Risparmio categories
        df_risparmio = df_savings.filter(pl.col("CategoryType") == "Risparmio")

        if len(df_risparmio) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Savings Breakdown by Category - No Risparmio Data",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings categorized as 'Risparmio' available",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Ensure we have Month column
        if "Month" not in df_risparmio.columns:
            df_risparmio = df_risparmio.with_columns(
                pl.col("Date").dt.strftime("%Y-%m").alias("Month")
            )

        # Get the end month in "YYYY-MM" format
        end_month = end_date.strftime("%Y-%m")
        self.logger.info("Generating savings breakdown as of: %s", end_month)

        # Get all months up to and including the end month
        all_months = sorted(df_risparmio["Month"].unique().to_list())
        relevant_months = [month for month in all_months if month <= end_month]

        if not relevant_months:
            fig = go.Figure()
            fig.update_layout(
                title=f"Savings Breakdown by Category - No Data Until {end_month}",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text=f"No savings data available up to {end_month}",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Get all savings categories
        categories = df_risparmio["Category"].unique().to_list()

        # Calculate category balances as of the end month
        category_balances = {cat: 0.0 for cat in categories}

        # Process all transactions up to and including the end month
        for category in categories:
            category_data = df_risparmio.filter(
                (pl.col("Month").is_in(relevant_months))
                & (pl.col("Category") == category)
            )
            if len(category_data) > 0:
                net_change = category_data["Value"].sum()
                category_balances[category] += net_change

        # Filter out categories with zero or negative balances for the pie chart
        labels = []
        values = []
        colors = []

        for category, balance in category_balances.items():
            if balance > 0:
                labels.append(category)
                values.append(balance)
                colors.append(self.category_mapper.get_savings_category_color(category))

        # If we have no positive values, display a message
        if not values or sum(values) == 0:
            fig = go.Figure()
            fig.update_layout(
                title=f"Savings Breakdown by Category as of {end_month} - No Positive Savings",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No positive savings found for the selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Create hover text with formatted values and percentages
        hover_texts = []
        for label, value in zip(labels, values):
            percentage = value / sum(values) * 100
            hover_texts.append(f"{label}: {value:,.2f}€ ({percentage:.1f}%)")

        # Create the pie chart
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.4,
                    marker=dict(colors=colors),
                    textinfo="percent",
                    hoverinfo="text",
                    hovertext=hover_texts,
                    textfont=dict(size=14),
                )
            ]
        )

        # Style the figure
        fig = self.chart_styler.apply_styling(
            fig, f"Savings Breakdown as of {end_month}"
        )

        # Update specific pie chart styling
        fig.update_layout(
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.2,
                xanchor="center",
                x=0.5,
            ),
        )

        return fig

    def create_savings_allocation(
        self, df_savings_allocation: Optional[pl.DataFrame]
    ) -> go.Figure:
        """
        Create a grouped bar chart comparing allocated vs spent savings with consistent colors.

        Args:
            df_savings_allocation: DataFrame with savings allocation data

        Returns:
            go.Figure: Plotly figure for the dashboard
        """
        if df_savings_allocation is None or len(df_savings_allocation) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Allocation Status - No Data Available",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Get unique categories and types for plotting
        plot_categories = df_savings_allocation["Category"].unique().to_list()
        plot_types = df_savings_allocation["Type"].unique().to_list()

        # Build a figure with grouped bars
        fig = go.Figure()

        # Colors for different types
        colors = {
            "Saved": self.color_theme["income"],
            "Allocated": self.color_theme["savings"]["allocation"],
            "Spent from Savings": self.color_theme["expense"],
            "Spent from Allocations": "#FFA07A",  # Light salmon color
        }

        for allocation_type in plot_types:
            filtered_data = df_savings_allocation.filter(
                pl.col("Type") == allocation_type
            )

            # Create a dict of values by category
            values_by_category = {}
            for category in plot_categories:
                match = filtered_data.filter(pl.col("Category") == category)
                values_by_category[category] = (
                    match["Value"][0] if len(match) > 0 else 0
                )

            # Add bar trace
            fig.add_trace(
                go.Bar(
                    name=allocation_type,
                    x=list(values_by_category.keys()),
                    y=list(values_by_category.values()),
                    marker_color=colors.get(
                        allocation_type, "#808080"
                    ),  # Default gray if type not found
                )
            )

        fig = self.chart_styler.apply_styling(fig, "Allocation Status by Category")
        fig.update_layout(
            yaxis=dict(title="Amount (€)"),
            barmode="group",
            legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
        )

        return fig

    def create_monthly_overview(
        self, df_monthly_summary: Optional[pl.DataFrame]
    ) -> go.Figure:
        """
        Create a monthly overview figure with larger colored indicators in hover text.

        Args:
            df_monthly_summary: DataFrame with monthly summary data

        Returns:
            go.Figure: Plotly figure for the dashboard
        """
        if df_monthly_summary is None or len(df_monthly_summary) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Monthly Overview - No Data Available",
                yaxis=dict(title="Amount (€)"),
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                        font=dict(color=self.color_theme.get("text", "#232323")),
                    )
                ],
            )
            return fig

        # Format month strings for display (from YYYY-MM to MMM YYYY)
        months = []
        for month_str in df_monthly_summary["Month"]:
            year = month_str.split("-")[0]
            month_num = int(month_str.split("-")[1])
            month_name = [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ][month_num - 1]
            months.append(f"{month_name} {year}")

        # Create combined data for hover
        combined_data = []
        for i, month in enumerate(months):
            combined_data.append(
                {
                    "month": month,
                    "income": df_monthly_summary["Income"][i],
                    "expenses": df_monthly_summary["Expenses"][i],
                    "balance": df_monthly_summary["Balance"][i],
                }
            )

        # Get color values
        income_color = self.color_theme["income"]
        expense_color = self.color_theme["expense"]
        balance_color = self.color_theme["balance"]

        # Create hover template with colored squares
        hover_template = (
            "<b>%{customdata.month}</b><br>"
            f"<span style='color:{income_color}; font-size:22px;'>■</span> Income: %{{customdata.income:,.2f}}€<br>"
            f"<span style='color:{expense_color}; font-size:22px;'>■</span> Expenses: %{{customdata.expenses:,.2f}}€<br>"
            f"<span style='color:{balance_color}; font-size:22px;'>■</span> Balance: %{{customdata.balance:,.2f}}€"
            "<extra></extra>"
        )

        fig = go.Figure()

        # Add income bars
        fig.add_trace(
            go.Bar(
                x=months,
                y=df_monthly_summary["Income"],
                name="Income",
                marker_color=income_color,
                marker_line_color=income_color,
                marker_line_width=1.5,
                opacity=0.9,
                customdata=combined_data,
                hovertemplate=hover_template,
            )
        )

        # Add expense bars
        fig.add_trace(
            go.Bar(
                x=months,
                y=df_monthly_summary["Expenses"],
                name="Expenses",
                marker_color=expense_color,
                marker_line_color=expense_color,
                marker_line_width=1.5,
                opacity=0.9,
                customdata=combined_data,
                hovertemplate=hover_template,
            )
        )

        # Add balance line
        fig.add_trace(
            go.Scatter(
                x=months,
                y=df_monthly_summary["Balance"],
                name="Balance",
                line=dict(color=balance_color, width=3),
                mode="lines+markers",
                marker=dict(size=8, color=balance_color),
                customdata=combined_data,
                hovertemplate=hover_template,
            )
        )

        fig = self.chart_styler.apply_styling(
            fig, "Monthly Income, Expenses, and Balance Overview"
        )

        fig.update_layout(
            barmode="group",
            yaxis=dict(title="Amount (€)"),
            hovermode="closest",
        )

        return fig

    def create_savings_overview(
        self, df_savings_metrics: Optional[pl.DataFrame]
    ) -> go.Figure:
        """
        Create a savings overview line chart.

        Args:
            df_savings_metrics: DataFrame with savings metrics

        Returns:
            go.Figure: Plotly figure for the dashboard
        """
        if df_savings_metrics is None or len(df_savings_metrics) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Savings Overview - No Data Available",
                yaxis=dict(title="Amount (€)"),
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No savings data available for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        fig = go.Figure()

        # Add total savings line
        fig.add_trace(
            go.Scatter(
                x=df_savings_metrics["Month"].to_list(),
                y=df_savings_metrics["TotalSavings"].to_list(),
                name="Total Savings",
                line=dict(color=self.color_theme["savings"]["total"], width=4),
            )
        )

        # Add total allocations line
        fig.add_trace(
            go.Scatter(
                x=df_savings_metrics["Month"].to_list(),
                y=df_savings_metrics["TotalAllocated"].to_list(),
                name="Total Allocations",
                line=dict(
                    color=self.color_theme["savings"]["allocation"],
                    width=3,
                    dash="dash",
                ),
            )
        )

        # Add total spent line
        fig.add_trace(
            go.Scatter(
                x=df_savings_metrics["Month"].to_list(),
                y=df_savings_metrics["TotalSpent"].to_list(),
                name="Total Spent",
                line=dict(
                    color=self.color_theme["savings"]["spent"], width=3, dash="dot"
                ),
            )
        )

        fig = self.chart_styler.apply_styling(fig, "Savings Overview")
        fig.update_layout(
            yaxis=dict(title="Amount (€)"),
            hovermode="x",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )

        return fig

    def create_savings_table(self, df_savings: Optional[pl.DataFrame]) -> Any:
        """
        Create a table of savings transactions.

        Args:
            df_savings: DataFrame with savings transactions

        Returns:
            dash_table.DataTable: DataTable component for the dashboard
        """
        if df_savings is None or len(df_savings) == 0:
            return html.Div(
                "No savings transactions available for the selected period."
            )

        # Create a copy of the dataframe with sorted data
        df_table = df_savings.sort("Date", descending=True)

        # Format date and numeric columns for display
        df_table = df_table.with_columns(
            [
                pl.col("Date").dt.strftime("%d/%m/%Y").alias("Date"),
                pl.col("Value")
                .map_elements(lambda x: f"€{x:.2f}", return_dtype=pl.Utf8)
                .alias("Amount"),
            ]
        )

        # Select and rename columns for display
        df_display = df_table.select(
            ["Date", "Description", "Category", "CategoryType", "Value"]
        )

        # Convert directly to records for Dash without using pandas
        records = df_display.to_dicts()

        # Create the table
        table = dash_table.DataTable(
            data=records,
            columns=[{"name": col, "id": col} for col in df_display.columns],
            style_table={"overflowX": "auto"},
            style_cell={
                "textAlign": "left",
                "padding": "10px",
                "whiteSpace": "normal",
                "height": "auto",
            },
            style_header={
                "backgroundColor": "rgb(230, 230, 230)",
                "fontWeight": "bold",
            },
            style_data_conditional=[
                {
                    "if": {"filter_query": '{CategoryType} = "Accantonamento"'},
                    "backgroundColor": "rgba(7, 128, 128, 0.05)",  # Very light teal
                    "borderLeft": f"3px solid {self.color_theme['income']}",
                },
                {
                    "if": {"filter_query": "{Value} < 0"},
                    "backgroundColor": "rgba(244, 93, 72, 0.05)",  # Very light coral
                    "borderLeft": f"3px solid {self.color_theme['expense']}",
                },
            ],
            page_size=10,
        )

        return table
