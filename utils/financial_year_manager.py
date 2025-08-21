"""
Financial Year Manager
Provides centralized financial year logic and month ordering
Hard-coded to use April 1 of current year to March 31 of next year
"""

from datetime import datetime, date
from typing import List, Dict, Tuple

class FinancialYearManager:
    """
    Centralized management for financial year logic
    Hard-coded to use April 1st of current year to March 31st of next year
    """
    
    def __init__(self):
        self.current_calendar_year = datetime.now().year
        
    def get_financial_year_months_ordered(self) -> List[str]:
        """
        Returns months in financial year order: April to March of next year
        This is the definitive order for all financial year displays
        """
        return [
            "April", "May", "June",           # Q1
            "July", "August", "September",    # Q2  
            "October", "November", "December", # Q3
            "January", "February", "March"     # Q4 (next calendar year)
        ]
    
    def get_financial_year_quarters(self) -> Dict[str, List[str]]:
        """
        Returns quarter mapping for financial year
        Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar
        """
        return {
            "Q1": ["April", "May", "June"],
            "Q2": ["July", "August", "September"], 
            "Q3": ["October", "November", "December"],
            "Q4": ["January", "February", "March"]
        }
    
    def get_current_financial_year(self) -> str:
        """
        Returns current financial year string (e.g., "FY2025")
        Financial year starts April 1st
        """
        current_date = datetime.now()
        if current_date.month >= 4:  # April onwards
            return f"FY{current_date.year}"
        else:  # January to March
            return f"FY{current_date.year - 1}"
    
    def get_financial_year_range(self, fy_year: int) -> Tuple[date, date]:
        """
        Returns start and end dates for given financial year
        Args:
            fy_year: Financial year (e.g., 2025 for FY2025)
        Returns:
            Tuple of (start_date, end_date)
        """
        start_date = date(fy_year, 4, 1)  # April 1st
        end_date = date(fy_year + 1, 3, 31)  # March 31st next year
        return start_date, end_date
    
    def get_month_calendar_year(self, month: str, fy_year: int) -> int:
        """
        Returns the actual calendar year for a given month in a financial year
        Args:
            month: Month name (e.g., "January")
            fy_year: Financial year (e.g., 2025)
        Returns:
            Calendar year for that month
        """
        if month in ["January", "February", "March"]:
            return fy_year + 1  # Next calendar year
        else:
            return fy_year  # Same calendar year
    
    def get_month_number(self, month: str) -> int:
        """Returns calendar month number (1-12)"""
        month_mapping = {
            'January': 1, 'February': 2, 'March': 3,
            'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9,
            'October': 10, 'November': 11, 'December': 12
        }
        return month_mapping.get(month, 1)
    
    def get_financial_year_month_abbreviations(self) -> List[str]:
        """
        Returns month abbreviations in financial year order
        Used for column naming in tables
        """
        return [
            "Apr", "May", "Jun",     # Q1
            "Jul", "Aug", "Sep",     # Q2
            "Oct", "Nov", "Dec",     # Q3
            "Jan", "Feb", "Mar"      # Q4
        ]
    
    def validate_financial_year_data_completeness(self, months_in_data: List[str]) -> Dict[str, any]:
        """
        Validates if data contains all 12 months of financial year
        Returns validation summary
        """
        expected_months = set(self.get_financial_year_months_ordered())
        actual_months = set(months_in_data)
        
        missing_months = expected_months - actual_months
        extra_months = actual_months - expected_months
        
        return {
            'is_complete': len(missing_months) == 0,
            'total_months': len(actual_months),
            'missing_months': list(missing_months),
            'extra_months': list(extra_months),
            'expected_count': 12
        }
    
    def format_financial_year_display(self, fy_year: int) -> str:
        """
        Formats financial year for display
        Returns: "FY 2025 (April 2025 - March 2026)"
        """
        start_date, end_date = self.get_financial_year_range(fy_year)
        return f"FY {fy_year} (April {fy_year} - March {fy_year + 1})"