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

        # Get the end month in "YYYY-MM" format for filtering
        end_month = end_date.strftime("%Y-%m")
        # Format for display: "Month Year" (e.g., "October 2025")
        end_month_display = end_date.strftime("%B %Y")
        self.logger.info("Generating savings breakdown as of: %s", end_month)

        # Get all months up to and including the end month
        all_months = sorted(df_risparmio["Month"].unique().to_list())
        relevant_months = [month for month in all_months if month <= end_month]

        if not relevant_months:
            fig = go.Figure()
            fig.update_layout(
                title=f"Savings Breakdown as of {end_month_display} - No Data",
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
                title=f"Savings Breakdown as of {end_month_display} - No Positive Savings",
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
                    textinfo="none",  # Don't show text on chart to avoid overlap
                    hoverinfo="text",
                    hovertext=hover_texts,
                    textfont=dict(size=14),
                )
            ]
        )

        # Style the figure with formatted date
        fig = self.chart_styler.apply_styling(
            fig, f"Savings Breakdown as of {end_month_display}"
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

    def create_monthly_savings_rate(
        self,
        df_monthly_summary: Optional[pl.DataFrame],
        df_savings_metrics: Optional[pl.DataFrame],
        df_processed_savings: Optional[pl.DataFrame] = None,
    ) -> go.Figure:
        """
        Create a monthly savings rate chart showing percentage and absolute amounts.

        Args:
            df_monthly_summary: DataFrame with monthly income data
            df_savings_metrics: DataFrame with monthly savings data
            df_processed_savings: DataFrame with detailed savings transactions by category

        Returns:
            go.Figure: Plotly figure for the dashboard
        """
        if (
            df_monthly_summary is None
            or len(df_monthly_summary) == 0
            or df_savings_metrics is None
            or len(df_savings_metrics) == 0
        ):
            fig = go.Figure()
            fig.update_layout(
                title="Monthly Savings Rate - No Data Available",
                plot_bgcolor=self.color_theme["background"],
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

        # Merge income and savings data by month
        df_merged = df_monthly_summary.join(df_savings_metrics, on="Month", how="inner")

        # Filter out months with no income (cannot calculate rate)
        df_merged = df_merged.filter(pl.col("Income") > 0)

        if len(df_merged) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Monthly Savings Rate - No Income Data",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No income data available to calculate savings rate",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Get months and income
        months = df_merged["Month"].to_list()
        total_savings = df_merged["TotalSavings"].to_list()
        income = df_merged["Income"].to_list()

        # Calculate monthly total savings amounts (for percentage calculation)
        monthly_savings_totals = []
        for i, month in enumerate(months):
            if i == 0:
                # First month: Calculate from the beginning of data
                all_savings = df_savings_metrics.filter(pl.col("Month") <= months[0])
                if len(all_savings) > 1:
                    prev_total = all_savings["TotalSavings"][-2]
                    monthly_amount = total_savings[i] - prev_total
                else:
                    monthly_amount = 0
            else:
                # Subsequent months: difference from previous month
                monthly_amount = total_savings[i] - total_savings[i - 1]
            monthly_savings_totals.append(monthly_amount)

        # Calculate savings rate as percentage
        savings_rates = [
            (saved / inc * 100) if inc > 0 else 0
            for saved, inc in zip(monthly_savings_totals, income)
        ]

        # Calculate savings by category for stacked bar
        category_data = {}
        if df_processed_savings is not None and len(df_processed_savings) > 0:
            # Ensure we have Month column
            if "Month" not in df_processed_savings.columns:
                df_processed_savings = df_processed_savings.with_columns(
                    pl.col("Date").dt.strftime("%Y-%m").alias("Month")
                )

            # Filter for Risparmio only (not Accantonamento)
            df_risparmio = df_processed_savings.filter(
                pl.col("CategoryType") == "Risparmio"
            )

            # Get all unique categories
            all_categories = df_risparmio["Category"].unique().to_list()

            # For each category, calculate monthly amounts
            for category in all_categories:
                category_monthly_values = []

                for month in months:
                    # Get savings for this month and category
                    month_cat_savings = df_risparmio.filter(
                        (pl.col("Month") == month) & (pl.col("Category") == category)
                    )

                    if len(month_cat_savings) > 0:
                        monthly_value = month_cat_savings["Value"].sum()
                        category_monthly_values.append(monthly_value)
                    else:
                        category_monthly_values.append(0)

                category_data[category] = category_monthly_values
        else:
            # No detailed savings data, create single category
            category_data["Savings"] = monthly_savings_totals

        # Format months for display
        display_months = []
        for month_str in months:
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
            display_months.append(f"{month_name} {year}")

        # Create combined data for unified hover
        combined_data = []
        rate_color = self.color_theme["savings"]["total"]

        # Build category colors dict and create safe keys for customdata
        category_colors_dict = {}
        category_key_mapping = {}  # Map safe keys to display names
        for idx, category in enumerate(category_data.keys()):
            safe_key = f"cat_{idx}"  # Use safe keys without spaces
            category_key_mapping[safe_key] = category

            if category == "Savings":
                category_colors_dict[safe_key] = self.color_theme["income"]
            else:
                category_colors_dict[
                    safe_key
                ] = self.category_mapper.get_savings_category_color(category)

        # Create reverse mapping: category name -> safe key
        category_to_safe_key = {v: k for k, v in category_key_mapping.items()}

        for i, month in enumerate(display_months):
            month_data = {
                "month": month,
                "rate": savings_rates[i],
            }
            # Add each category's value with safe keys
            for safe_key, category in category_key_mapping.items():
                values = category_data[category]
                month_data[safe_key] = values[i]
            combined_data.append(month_data)

        # Create unified hover template with colored squares
        hover_template_parts = ["<b>%{customdata.month}</b><br>"]
        hover_template_parts.append(
            f"<span style='color:{rate_color}; font-size:22px;'>■</span> Savings Rate: %{{customdata.rate:.1f}}%<br>"
        )
        for safe_key, display_name in category_key_mapping.items():
            color = category_colors_dict[safe_key]
            hover_template_parts.append(
                f"<span style='color:{color}; font-size:22px;'>■</span> {display_name}: %{{customdata.{safe_key}:,.2f}}€<br>"
            )
        # Remove last <br> and add closing tag
        hover_template = "".join(hover_template_parts)[:-4] + "<extra></extra>"

        # Create figure with secondary y-axis
        fig = go.Figure()

        # Add stacked bars for each savings category (primary y-axis) first
        for category, values in category_data.items():
            safe_key = category_to_safe_key[category]  # Get the safe key
            cat_color = category_colors_dict[safe_key]  # Use safe key for color lookup

            fig.add_trace(
                go.Bar(
                    x=display_months,
                    y=values,
                    name=category,
                    marker_color=cat_color,
                    marker_line_color=cat_color,
                    marker_line_width=1.5,
                    opacity=0.9,
                    customdata=combined_data,
                    hovertemplate=hover_template,
                    yaxis="y1",  # Use primary y-axis for bars
                )
            )

        # Add savings rate line (secondary y-axis) last so it renders on top
        fig.add_trace(
            go.Scatter(
                x=display_months,
                y=savings_rates,
                name="Savings Rate (%)",
                line=dict(color=rate_color, width=3),
                mode="lines+markers",
                marker=dict(size=8, color=rate_color),
                customdata=combined_data,
                hovertemplate=hover_template,
                yaxis="y2",  # Use secondary y-axis for line
            )
        )

        # Apply styling
        fig = self.chart_styler.apply_styling(fig, "Monthly Savings Rate")

        # Update layout with dual y-axes and stacked bars
        fig.update_layout(
            barmode="relative",  # Stack positive above 0, negative below 0
            yaxis=dict(
                title="Amount Saved by Category (€)",
                tickformat="€,.0f",
                side="right",  # Amount axis on the right
            ),
            yaxis2=dict(
                title="Savings Rate (%)",
                tickformat=".1f",
                overlaying="y",
                side="left",  # Savings rate axis on the left
            ),
            hovermode="closest",  # Use closest to avoid text repetition
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.2,
                xanchor="center",
                x=0.5,
            ),
        )

        return fig

    def create_allocation_breakdown_by_category(
        self, df_savings: Optional[pl.DataFrame], end_date: datetime
    ) -> go.Figure:
        """
        Create a horizontal bar chart showing allocation amounts by category.

        Args:
            df_savings: DataFrame with savings data
            end_date: End date of the selected period

        Returns:
            go.Figure: Plotly figure for the dashboard
        """
        if df_savings is None or len(df_savings) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Allocation Breakdown - No Data Available",
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

        # Ensure we have Month column
        if "Month" not in df_savings.columns:
            df_savings = df_savings.with_columns(
                pl.col("Date").dt.strftime("%Y-%m").alias("Month")
            )

        # Get the end month in "YYYY-MM" format
        end_month = end_date.strftime("%Y-%m")

        # Get all months up to and including the end month
        all_months = sorted(df_savings["Month"].unique().to_list())
        relevant_months = [month for month in all_months if month <= end_month]

        if not relevant_months:
            fig = go.Figure()
            fig.update_layout(
                title="Allocation Breakdown - No Data",
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

        # Filter only Allocations (Accantonamento)
        df_filtered = df_savings.filter(
            (pl.col("Month").is_in(relevant_months))
            & (pl.col("CategoryType") == "Accantonamento")
        )

        if len(df_filtered) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Allocation Breakdown - No Allocations",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No allocations found for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Group by Category and sum the values
        df_grouped = df_filtered.group_by("Category").agg(
            pl.col("Value").sum().alias("Balance")
        )

        # Filter out categories with zero or negative balance
        df_grouped = df_grouped.filter(pl.col("Balance") > 0)

        if len(df_grouped) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Allocation Breakdown - No Positive Allocations",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No positive allocations for selected period",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Sort by balance descending for better visualization (highest first)
        df_grouped = df_grouped.sort("Balance", descending=True)

        categories = df_grouped["Category"].to_list()
        balances = df_grouped["Balance"].to_list()

        # Get colors for categories using the category mapper
        colors = [
            self.category_mapper.get_savings_category_color(cat) for cat in categories
        ]

        # Create vertical bar chart
        fig = go.Figure()

        # Set bar width based on number of categories
        bar_width = 0.3 if len(categories) == 1 else None

        # Create hover template
        hover_texts = []
        for category, balance in zip(categories, balances):
            hover_text = f"<b>{category}</b><br>" f"Amount: €{balance:,.2f}"
            hover_texts.append(hover_text)

        fig.add_trace(
            go.Bar(
                x=categories,
                y=balances,
                marker=dict(color=colors),
                text=[f"€{bal:,.0f}" for bal in balances],
                textposition="outside",
                textfont=dict(size=11),
                hovertext=hover_texts,
                hoverinfo="text",
                width=bar_width,
            )
        )

        # Apply styling with simple title
        fig = self.chart_styler.apply_styling(fig, "Allocation Breakdown")

        # Update layout for vertical bar chart
        fig.update_layout(
            xaxis=dict(
                title="",
                tickfont=dict(size=11),
            ),
            yaxis=dict(
                title="Amount (€)",
                tickformat="€,.0f",
            ),
            height=350,
            showlegend=False,
            margin=dict(l=50, r=50, t=80, b=100),
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

    def create_statistics_summary_chart(
        self, df_monthly_summary: Optional[pl.DataFrame]
    ) -> go.Figure:
        """
        Create a grouped bar chart showing average and median monthly income, expenses, and balance.

        Args:
            df_monthly_summary: DataFrame with monthly summary data

        Returns:
            go.Figure: Plotly figure showing statistics
        """
        if df_monthly_summary is None or len(df_monthly_summary) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Monthly Statistics - No Data Available",
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
                    )
                ],
            )
            return fig

        # Calculate statistics
        stats = {
            "Income": {
                "mean": df_monthly_summary["Income"].mean(),
                "median": df_monthly_summary["Income"].median(),
                "std": df_monthly_summary["Income"].std(),
            },
            "Expenses": {
                "mean": df_monthly_summary["Expenses"].mean(),
                "median": df_monthly_summary["Expenses"].median(),
                "std": df_monthly_summary["Expenses"].std(),
            },
            "Balance": {
                "mean": df_monthly_summary["Balance"].mean(),
                "median": df_monthly_summary["Balance"].median(),
                "std": df_monthly_summary["Balance"].std(),
            },
        }

        # Create grouped bar chart
        fig = go.Figure()

        categories = ["Income", "Expenses", "Balance"]
        colors = [
            self.color_theme["income"],
            self.color_theme["expense"],
            self.color_theme["balance"],
        ]

        # Add mean bars
        means = [stats[cat]["mean"] for cat in categories]
        medians = [stats[cat]["median"] for cat in categories]

        # Add individual bars for each category with proper colors
        for i, (cat, mean_val, median_val, color) in enumerate(
            zip(categories, means, medians, colors)
        ):
            # Add average bar
            fig.add_trace(
                go.Bar(
                    name="Average" if i == 0 else None,
                    x=[cat],
                    y=[mean_val],
                    marker_color=color,
                    marker_line_color=color,
                    marker_line_width=2,
                    opacity=0.8,
                    text=[f"€{mean_val:,.0f}"],
                    textposition="outside",
                    hovertemplate="Average: €%{y:,.2f}<extra></extra>",
                    legendgroup="average",
                    showlegend=(i == 0),
                )
            )

            # Add median bar
            fig.add_trace(
                go.Bar(
                    name="Median" if i == 0 else None,
                    x=[cat],
                    y=[median_val],
                    marker_color=color,
                    marker_pattern_shape="/",
                    marker_line_color=color,
                    marker_line_width=2,
                    opacity=0.6,
                    text=[f"€{median_val:,.0f}"],
                    textposition="outside",
                    hovertemplate="Median: €%{y:,.2f}<extra></extra>",
                    legendgroup="median",
                    showlegend=(i == 0),
                )
            )

        fig = self.chart_styler.apply_styling(
            fig, "Monthly Statistics: Average vs Median"
        )
        fig.update_layout(
            yaxis=dict(title="Amount (€)"),
            xaxis=dict(title=""),
            barmode="group",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
            ),
            height=450,
            margin=dict(l=50, r=50, t=80, b=120),
        )

        return fig

    def create_category_statistics_breakdown(
        self,
        df_processed: Optional[pl.DataFrame],
        category_type: str = "expense",
        top_n: Optional[int] = None,
    ) -> go.Figure:
        """
        Create a horizontal bar chart showing average, median, and confidence intervals by category.

        Args:
            df_processed: DataFrame with processed expense or income data
            category_type: Type of data ("expense" or "income")
            top_n: Number of top categories to show (None = show all)

        Returns:
            go.Figure: Plotly figure showing category statistics with confidence intervals
        """
        title = f"{category_type.capitalize()} by Category: All Categories Statistics"

        if df_processed is None or len(df_processed) == 0:
            fig = go.Figure()
            fig.update_layout(
                title=f"{title} - No Data Available",
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
                    )
                ],
            )
            return fig

        # Ensure we have Month column
        if "Month" not in df_processed.columns:
            df_processed = df_processed.with_columns(
                pl.col("Date").dt.strftime("%Y-%m").alias("Month")
            )

        # Get all unique months and categories
        all_months = df_processed["Month"].unique().sort()
        all_categories = df_processed["Category"].unique()

        # Get monthly totals per category
        monthly_by_category = df_processed.group_by(["Month", "Category"]).agg(
            pl.col("Value").sum().alias("MonthlyTotal")
        )

        # Create complete grid of all months × all categories
        # This ensures missing month-category combinations are counted as 0
        month_category_grid = []
        for month in all_months:
            for category in all_categories:
                month_category_grid.append({"Month": month, "Category": category})

        complete_grid = pl.DataFrame(month_category_grid)

        # Join with actual data and fill nulls with 0
        monthly_by_category = complete_grid.join(
            monthly_by_category, on=["Month", "Category"], how="left"
        ).with_columns(pl.col("MonthlyTotal").fill_null(0))

        # Calculate statistics per category
        category_stats = (
            monthly_by_category.group_by("Category")
            .agg(
                [
                    pl.col("MonthlyTotal").mean().alias("Mean"),
                    pl.col("MonthlyTotal").median().alias("Median"),
                    pl.col("MonthlyTotal").quantile(0.25).alias("Q1"),
                    pl.col("MonthlyTotal").quantile(0.75).alias("Q3"),
                    pl.col("MonthlyTotal").min().alias("Min"),
                    pl.col("MonthlyTotal").max().alias("Max"),
                ]
            )
            .sort("Mean", descending=True)
        )

        # Apply top_n filter if specified
        if top_n is not None:
            category_stats = category_stats.head(top_n)

        categories = category_stats["Category"].to_list()
        means = category_stats["Mean"].to_list()
        medians = category_stats["Median"].to_list()
        q1s = category_stats["Q1"].to_list()
        q3s = category_stats["Q3"].to_list()

        # Get colors for categories
        if category_type == "income":
            colors = self.category_mapper.get_income_colors(categories)
        else:
            colors = self.category_mapper.get_expense_colors(categories)

        # Create figure with error bars showing confidence intervals (Q1-Q3)
        fig = go.Figure()

        # Add bars for mean values with error bars
        error_y_upper = [q3 - mean for mean, q3 in zip(means, q3s)]
        error_y_lower = [mean - q1 for mean, q1 in zip(means, q1s)]

        # Create custom hover data with all statistics
        customdata = list(zip(medians, q1s, q3s))

        # Horizontal bars with categories on y-axis
        fig.add_trace(
            go.Bar(
                name="Average",
                y=categories,
                x=means,
                orientation="h",
                marker_color=colors,
                marker_line_color=colors,
                marker_line_width=1.5,
                opacity=0.8,
                error_x=dict(
                    type="data",
                    symmetric=False,
                    array=error_y_upper,
                    arrayminus=error_y_lower,
                    color="rgba(100, 100, 100, 0.5)",
                    thickness=2,
                ),
                text=[f"€{val:,.0f}" for val in means],
                textposition="outside",
                customdata=customdata,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Average: €%{x:,.2f}<br>"
                    "Median: €%{customdata[0]:,.2f}<br>"
                    "Q1 (25%%): €%{customdata[1]:,.2f}<br>"
                    "Q3 (75%%): €%{customdata[2]:,.2f}"
                    "<extra></extra>"
                ),
            )
        )

        # Add scatter points for median
        fig.add_trace(
            go.Scatter(
                name="Median",
                y=categories,
                x=medians,
                mode="markers",
                marker=dict(
                    symbol="diamond",
                    size=12,
                    color="white",
                    line=dict(color="black", width=2),
                ),
                customdata=customdata,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Median: €%{x:,.2f}<br>"
                    "Q1 (25%%): €%{customdata[1]:,.2f}<br>"
                    "Q3 (75%%): €%{customdata[2]:,.2f}"
                    "<extra></extra>"
                ),
            )
        )

        fig = self.chart_styler.apply_styling(fig, title)

        # Calculate dynamic height based on number of categories
        height = max(400, len(categories) * 40 + 150)

        fig.update_layout(
            xaxis=dict(title="Monthly Amount (€)"),
            yaxis=dict(title="", autorange="reversed"),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.25,
                xanchor="center",
                x=0.5,
            ),
            height=height,
            margin=dict(l=200, r=50, t=80, b=140),
        )

        return fig

    def create_statistics_summary_table(
        self, df_monthly_summary: Optional[pl.DataFrame]
    ) -> Any:
        """
        Create a table showing comprehensive statistics for income, expenses, and balance.

        Args:
            df_monthly_summary: DataFrame with monthly summary data

        Returns:
            dash_table.DataTable: DataTable component with statistics
        """
        if df_monthly_summary is None or len(df_monthly_summary) == 0:
            return html.Div(
                "No data available to generate statistics for the selected period."
            )

        # Calculate statistics for each metric
        metrics = ["Income", "Expenses", "Balance"]
        stats_data = []

        for metric in metrics:
            values = df_monthly_summary[metric]
            stats_data.append(
                {
                    "Metric": metric,
                    "Average": f"€{values.mean():,.2f}",
                    "Median": f"€{values.median():,.2f}",
                    "Min": f"€{values.min():,.2f}",
                    "Max": f"€{values.max():,.2f}",
                    "Q1 (25%)": f"€{values.quantile(0.25):,.2f}",
                    "Q3 (75%)": f"€{values.quantile(0.75):,.2f}",
                }
            )

        # Create the table
        table = dash_table.DataTable(
            data=stats_data,
            columns=[
                {"name": "Metric", "id": "Metric"},
                {"name": "Average", "id": "Average"},
                {"name": "Median", "id": "Median"},
                {"name": "Min", "id": "Min"},
                {"name": "Max", "id": "Max"},
                {"name": "Q1 (25%)", "id": "Q1 (25%)"},
                {"name": "Q3 (75%)", "id": "Q3 (75%)"},
            ],
            style_table={"overflowX": "auto"},
            style_cell={
                "textAlign": "center",
                "padding": "12px",
                "fontSize": "13px",
            },
            style_header={
                "backgroundColor": self.color_theme.get("headline", "#6C3BCE"),
                "color": "white",
                "fontWeight": "bold",
                "fontSize": "14px",
            },
            style_data_conditional=[
                {
                    "if": {"filter_query": '{Metric} = "Income"'},
                    "backgroundColor": f"rgba({self._hex_to_rgb(self.color_theme['income'])}, 0.1)",
                },
                {
                    "if": {"filter_query": '{Metric} = "Expenses"'},
                    "backgroundColor": f"rgba({self._hex_to_rgb(self.color_theme['expense'])}, 0.1)",
                },
                {
                    "if": {"filter_query": '{Metric} = "Balance"'},
                    "backgroundColor": f"rgba({self._hex_to_rgb(self.color_theme['balance'])}, 0.1)",
                },
            ],
        )

        return table

    def _hex_to_rgb(self, hex_color: str) -> str:
        """
        Convert hex color to RGB string for rgba.

        Args:
            hex_color: Hex color string (e.g., "#FF0000")

        Returns:
            str: RGB values as comma-separated string (e.g., "255, 0, 0")
        """
        hex_color = hex_color.lstrip("#")
        return f"{int(hex_color[0:2], 16)}, {int(hex_color[2:4], 16)}, {int(hex_color[4:6], 16)}"

    def create_prediction_scenarios_chart(
        self, df_monthly_summary: Optional[pl.DataFrame]
    ) -> go.Figure:
        """
        Create a chart showing prediction scenarios based on different statistical measures.

        Args:
            df_monthly_summary: DataFrame with monthly summary data

        Returns:
            go.Figure: Plotly figure showing prediction scenarios
        """
        if df_monthly_summary is None or len(df_monthly_summary) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Prediction Scenarios - No Data Available",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No data available for predictions",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Calculate different scenarios
        scenarios = {
            "Optimistic (Q3)": {
                "income": df_monthly_summary["Income"].quantile(0.75),
                "expenses": df_monthly_summary["Expenses"].quantile(0.25),
            },
            "Median": {
                "income": df_monthly_summary["Income"].median(),
                "expenses": df_monthly_summary["Expenses"].median(),
            },
            "Average": {
                "income": df_monthly_summary["Income"].mean(),
                "expenses": df_monthly_summary["Expenses"].mean(),
            },
            "Pessimistic (Q1)": {
                "income": df_monthly_summary["Income"].quantile(0.25),
                "expenses": df_monthly_summary["Expenses"].quantile(0.75),
            },
        }

        # Calculate balances
        for scenario in scenarios.values():
            scenario["balance"] = scenario["income"] - scenario["expenses"]

        scenario_names = list(scenarios.keys())
        incomes = [scenarios[s]["income"] for s in scenario_names]
        expenses = [scenarios[s]["expenses"] for s in scenario_names]
        balances = [scenarios[s]["balance"] for s in scenario_names]

        # Create grouped bar chart
        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                name="Income",
                x=scenario_names,
                y=incomes,
                marker_color=self.color_theme["income"],
                text=[f"€{val:,.0f}" for val in incomes],
                textposition="outside",
            )
        )

        fig.add_trace(
            go.Bar(
                name="Expenses",
                x=scenario_names,
                y=expenses,
                marker_color=self.color_theme["expense"],
                text=[f"€{val:,.0f}" for val in expenses],
                textposition="outside",
            )
        )

        fig.add_trace(
            go.Bar(
                name="Balance",
                x=scenario_names,
                y=balances,
                marker_color=self.color_theme["balance"],
                text=[f"€{val:,.0f}" for val in balances],
                textposition="outside",
            )
        )

        fig = self.chart_styler.apply_styling(fig, "Prediction Scenarios")
        fig.update_layout(
            yaxis=dict(title="Amount (€)"),
            xaxis=dict(title="", tickangle=-15),
            barmode="group",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
            ),
            height=450,
            margin=dict(l=50, r=50, t=80, b=120),
        )

        return fig

    def create_current_vs_typical_month(
        self, df_monthly_summary: Optional[pl.DataFrame]
    ) -> go.Figure:
        """
        Create a chart comparing the current (last selected) month vs typical month.
        Uses median as the "typical month" reference.

        Args:
            df_monthly_summary: DataFrame with monthly summary data

        Returns:
            go.Figure: Plotly figure comparing current vs typical month
        """
        if df_monthly_summary is None or len(df_monthly_summary) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Current vs Typical Month - No Data Available",
                plot_bgcolor=self.color_theme["background"],
                annotations=[
                    dict(
                        text="No data available",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                    )
                ],
            )
            return fig

        # Get the last month (most recent selected)
        last_month_data = df_monthly_summary.tail(1)

        if len(last_month_data) == 0:
            fig = go.Figure()
            fig.update_layout(
                title="Current vs Typical Month - No Data",
                plot_bgcolor=self.color_theme["background"],
            )
            return fig

        # Current month values
        current_income = last_month_data["Income"][0]
        current_expenses = last_month_data["Expenses"][0]
        current_balance = last_month_data["Balance"][0]

        # Typical month values (median of all months)
        typical_income = df_monthly_summary["Income"].median()
        typical_expenses = df_monthly_summary["Expenses"].median()
        typical_balance = df_monthly_summary["Balance"].median()

        # Get month name for title
        last_month_str = last_month_data["Month"][0]
        month_date = datetime.strptime(f"{last_month_str}-01", "%Y-%m-%d")
        month_name = month_date.strftime("%B %Y")

        # Create grouped bar chart
        fig = go.Figure()

        categories = ["Income", "Expenses", "Balance"]
        current_values = [current_income, current_expenses, current_balance]
        typical_values = [typical_income, typical_expenses, typical_balance]
        colors = [
            self.color_theme["income"],
            self.color_theme["expense"],
            self.color_theme["balance"],
        ]

        # Add typical month bars (median)
        fig.add_trace(
            go.Bar(
                name="Typical (Median)",
                x=categories,
                y=typical_values,
                marker_color=colors,
                marker_pattern_shape="/",
                marker_line_color=colors,
                marker_line_width=1.5,
                opacity=0.6,
                text=[f"€{val:,.0f}" for val in typical_values],
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>Typical: €%{y:,.2f}<extra></extra>",
            )
        )

        # Add current month bars
        fig.add_trace(
            go.Bar(
                name=f"{month_name}",
                x=categories,
                y=current_values,
                marker_color=colors,
                marker_line_color=colors,
                marker_line_width=2,
                opacity=0.9,
                text=[f"€{val:,.0f}" for val in current_values],
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>Current: €%{y:,.2f}<extra></extra>",
            )
        )

        fig = self.chart_styler.apply_styling(fig, f"{month_name} vs Typical Month")
        fig.update_layout(
            yaxis=dict(title="Amount (€)"),
            xaxis=dict(title=""),
            barmode="group",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5,
            ),
            height=450,
            margin=dict(l=50, r=50, t=80, b=120),
        )

        return fig

    def create_category_vs_typical_comparison(
        self, df_processed: Optional[pl.DataFrame], category_type: str = "expense"
    ) -> go.Figure:
        """
        Create a chart comparing current month categories vs typical (median) with attention indicators.

        Args:
            df_processed: DataFrame with processed expense or income data
            category_type: Type of data ("expense" or "income")

        Returns:
            go.Figure: Plotly figure with category comparison and attention markers
        """
        title = f"{category_type.capitalize()} Categories: Current vs Typical"

        if df_processed is None or len(df_processed) == 0:
            fig = go.Figure()
            fig.update_layout(
                title=f"{title} - No Data Available",
                plot_bgcolor=self.color_theme["background"],
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

        # Ensure we have Month column
        if "Month" not in df_processed.columns:
            df_processed = df_processed.with_columns(
                pl.col("Date").dt.strftime("%Y-%m").alias("Month")
            )

        # Get the last month (current)
        months = sorted(df_processed["Month"].unique().to_list())
        if len(months) == 0:
            fig = go.Figure()
            fig.update_layout(
                title=f"{title} - No Data", plot_bgcolor=self.color_theme["background"]
            )
            return fig

        last_month = months[-1]
        current_month_data = df_processed.filter(pl.col("Month") == last_month)

        # Get all unique categories
        all_categories = df_processed["Category"].unique()

        # Get monthly totals per category for all months
        monthly_by_category = df_processed.group_by(["Month", "Category"]).agg(
            pl.col("Value").sum().alias("MonthlyTotal")
        )

        # Create complete grid of all months × all categories
        # This ensures missing month-category combinations are counted as 0
        month_category_grid = []
        for month in months:
            for category in all_categories:
                month_category_grid.append({"Month": month, "Category": category})

        complete_grid = pl.DataFrame(month_category_grid)

        # Join with actual data and fill nulls with 0
        monthly_by_category = complete_grid.join(
            monthly_by_category, on=["Month", "Category"], how="left"
        ).with_columns(pl.col("MonthlyTotal").fill_null(0))

        # Calculate median (typical) for each category
        typical_by_category = monthly_by_category.group_by("Category").agg(
            pl.col("MonthlyTotal").median().alias("TypicalAmount")
        )

        # Get current month totals
        current_by_category = current_month_data.group_by("Category").agg(
            pl.col("Value").sum().alias("CurrentAmount")
        )

        # Merge to compare
        comparison = typical_by_category.join(
            current_by_category, on="Category", how="outer"
        ).fill_null(0)

        # Calculate percentage difference
        comparison = comparison.with_columns(
            [
                (
                    (pl.col("CurrentAmount") - pl.col("TypicalAmount"))
                    / pl.col("TypicalAmount")
                    * 100
                )
                .alias("PercentDiff")
                .fill_null(0)
            ]
        )

        # Sort by absolute difference
        comparison = comparison.sort(
            "PercentDiff", descending=(category_type == "income")
        )

        categories = comparison["Category"].to_list()
        current_amounts = comparison["CurrentAmount"].to_list()
        typical_amounts = comparison["TypicalAmount"].to_list()
        percent_diffs = comparison["PercentDiff"].to_list()

        # Determine attention markers
        # For expenses: attention if current > typical + 20%
        # For income: attention if current < typical - 20%
        attention_threshold = 20
        markers = []
        for pct_diff in percent_diffs:
            if category_type == "expense":
                # Red flag if overspending
                if pct_diff > attention_threshold:
                    markers.append("⚠️")
                else:
                    markers.append("")
            else:  # income
                # Red flag if underearning
                if pct_diff < -attention_threshold:
                    markers.append("⚠️")
                else:
                    markers.append("")

        # Get colors
        if category_type == "income":
            colors = self.category_mapper.get_income_colors(categories)
        else:
            colors = self.category_mapper.get_expense_colors(categories)

        # Create horizontal bar chart
        fig = go.Figure()

        # Add typical bars (no text, since we'll show it on the current bar)
        fig.add_trace(
            go.Bar(
                name="Typical (Median)",
                y=categories,
                x=typical_amounts,
                orientation="h",
                marker_color=colors,
                marker_pattern_shape="/",
                marker_line_width=1,
                opacity=0.5,
                text=[""] * len(typical_amounts),
                textposition="none",
                hovertemplate=("Typical: €%{x:,.2f}" "<extra></extra>"),
            )
        )

        # Add current month bars with text beyond the maximum
        fig.add_trace(
            go.Bar(
                name=f"Current ({months[-1]})",
                y=categories,
                x=current_amounts,
                orientation="h",
                marker_color=colors,
                marker_line_width=2,
                opacity=0.9,
                text=[
                    (
                        f"{marker} €{curr:,.0f} vs €{typ:,.0f} ({diff:+.0f}%)"
                        if marker
                        else f"€{curr:,.0f} vs €{typ:,.0f} ({diff:+.0f}%)"
                    )
                    for curr, typ, diff, marker in zip(
                        current_amounts, typical_amounts, percent_diffs, markers
                    )
                ],
                textposition="outside",
                customdata=list(zip(typical_amounts, percent_diffs, markers)),
                hovertemplate=(
                    "Current: €%{x:,.2f}<br>"
                    "Typical: €%{customdata[0]:,.2f}<br>"
                    "Difference: %{customdata[1]:+.1f}%%"
                    "<extra></extra>"
                ),
            )
        )

        fig = self.chart_styler.apply_styling(fig, title)

        # Calculate height
        height = max(400, len(categories) * 35 + 150)

        fig.update_layout(
            xaxis=dict(title="Amount (€)"),
            yaxis=dict(title="", autorange="reversed"),
            barmode="overlay",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.25,
                xanchor="center",
                x=0.5,
            ),
            height=height,
            margin=dict(l=200, r=150, t=80, b=140),
        )

        return fig
