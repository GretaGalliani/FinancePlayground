#!/filepath: src/category_mapper.py
"""
CategoryMapper module for consistent color mapping in the finance dashboard.

This module ensures categories are always assigned the same colors
in visualizations, providing a consistent user experience.
"""

import logging
from typing import Dict, List, Optional

from config import Config


class CategoryMapper:
    """
    Maps categories to consistent colors based on configuration.

    This class ensures that each category is always represented by the same color
    across all visualizations in the dashboard, making the data more intuitive to read.
    """

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        """
        Initialize the category mapper with configuration.

        Args:
            config: Application configuration containing category color mappings
            logger: Logger instance for logging category mapping events
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._expense_category_colors: Dict[str, str] = {}
        self._income_category_colors: Dict[str, str] = {}
        self._savings_category_colors: Dict[str, str] = {}
        self._load_category_mappings()

    def _load_category_mappings(self) -> None:
        """
        Load category to color mappings from configuration.

        This method initializes the color mappings for expense, income, and savings categories
        based on the configuration. If explicit mappings are not defined, it will create them
        using the category list and color palette from configuration.
        """
        # Try to load explicit mappings first
        self._expense_category_colors = self.config.get("expense_category_colors", {})
        self._income_category_colors = self.config.get("income_category_colors", {})
        self._savings_category_colors = self.config.get("savings_category_colors", {})

        # If explicit mappings are not defined, create them from categories and palette
        if not self._expense_category_colors:
            self._create_expense_category_mapping()

        if not self._income_category_colors:
            self._create_income_category_mapping()

        if not self._savings_category_colors:
            self._create_savings_category_mapping()

        self.logger.info(
            f"Loaded color mappings for {len(self._expense_category_colors)} expense categories"
        )
        self.logger.info(
            f"Loaded color mappings for {len(self._income_category_colors)} income categories"
        )
        self.logger.info(
            f"Loaded color mappings for {len(self._savings_category_colors)} savings categories"
        )

    def _create_expense_category_mapping(self) -> None:
        """Create color mappings for expense categories."""
        categories = self.config.get("valid_expenses_categories", [])
        colors = self.config.get("color_palette", {}).get("categories", [])

        if not categories or not colors:
            self.logger.warning(
                "Could not create expense category mappings: missing categories or colors"
            )
            return

        # Create mappings using the available colors, cycling if needed
        for i, category in enumerate(categories):
            color_index = i % len(colors)
            self._expense_category_colors[category] = colors[color_index]

    def _create_income_category_mapping(self) -> None:
        """Create color mappings for income categories."""
        categories = self.config.get("valid_income_categories", [])
        colors = self.config.get("color_palette", {}).get("categories", [])

        if not categories or not colors:
            self.logger.warning(
                "Could not create income category mappings: missing categories or colors"
            )
            return

        # Use a different starting point in the color palette for income categories
        # to differentiate them from expense categories
        offset = 8  # Start from a different point in the color palette
        for i, category in enumerate(categories):
            color_index = (i + offset) % len(colors)
            self._income_category_colors[category] = colors[color_index]

    def _create_savings_category_mapping(self) -> None:
        """Create color mappings for savings categories."""
        categories = self.config.get("valid_savings_categories", [])
        colors = self.config.get("color_palette", {}).get("categories", [])

        if not categories or not colors:
            self.logger.warning(
                "Could not create savings category mappings: missing categories or colors"
            )
            return

        # Use a different starting point in the color palette for savings
        offset = 4  # Different offset from income categories
        for i, category in enumerate(categories):
            color_index = (i + offset) % len(colors)
            self._savings_category_colors[category] = colors[color_index]

    def get_expense_category_color(self, category: str) -> str:
        """
        Get the color for an expense category.

        Args:
            category: The expense category name

        Returns:
            str: The color code for the category, or a default color if not found
        """
        default_color = self.config.get("color_palette", {}).get("expense", "#F45D48")
        return self._expense_category_colors.get(category, default_color)

    def get_income_category_color(self, category: str) -> str:
        """
        Get the color for an income category.

        Args:
            category: The income category name

        Returns:
            str: The color code for the category, or a default color if not found
        """
        default_color = self.config.get("color_palette", {}).get("income", "#078080")
        return self._income_category_colors.get(category, default_color)

    def get_savings_category_color(self, category: str) -> str:
        """
        Get the color for a savings category.

        Args:
            category: The savings category name

        Returns:
            str: The color code for the category, or a default color if not found
        """
        default_color = (
            self.config.get("color_palette", {})
            .get("savings", {})
            .get("general", "#078080")
        )
        return self._savings_category_colors.get(category, default_color)

    def get_expense_colors(self, categories: List[str]) -> List[str]:
        """
        Get colors for a list of expense categories in the same order.

        Args:
            categories: List of expense category names

        Returns:
            List[str]: List of color codes for the categories
        """
        return [self.get_expense_category_color(category) for category in categories]

    def get_income_colors(self, categories: List[str]) -> List[str]:
        """
        Get colors for a list of income categories in the same order.

        Args:
            categories: List of income category names

        Returns:
            List[str]: List of color codes for the categories
        """
        return [self.get_income_category_color(category) for category in categories]

    def get_savings_colors(self, categories: List[str]) -> List[str]:
        """
        Get colors for a list of savings categories in the same order.

        Args:
            categories: List of savings category names

        Returns:
            List[str]: List of color codes for the categories
        """
        return [self.get_savings_category_color(category) for category in categories]

    def get_category_colors_dict(
        self, category_type: str = "expenses"
    ) -> Dict[str, str]:
        """
        Get the entire dictionary of category to color mappings.

        Args:
            category_type: Type of categories ("expenses", "income", or "savings")

        Returns:
            Dict[str, str]: Dictionary mapping categories to color codes
        """
        if category_type.lower() == "income":
            return self._income_category_colors
        elif category_type.lower() == "savings":
            return self._savings_category_colors
        else:
            return self._expense_category_colors
