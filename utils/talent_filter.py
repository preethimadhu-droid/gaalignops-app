"""
Talent Filtering Utility
Provides centralized business logic for filtering talent based on status rules
"""

import pandas as pd
from typing import List, Dict, Any

class TalentFilter:
    """
    Centralized talent filtering logic for demand-supply mapping
    """
    
    # Business rules for talent availability
    AVAILABLE_EMPLOYMENT_STATUSES = ['Active']
    EXCLUDED_ASSIGNMENT_STATUSES = ['Support']
    
    @classmethod
    def filter_available_talent(cls, talent_data: pd.DataFrame) -> pd.DataFrame:
        """
        Filter talent data to show only those available for new assignments
        
        Args:
            talent_data: DataFrame containing talent information
            
        Returns:
            DataFrame with filtered talent based on business rules
        """
        if talent_data.empty:
            return talent_data
            
        # Apply business rules for talent availability in new assignments
        available_talent = talent_data[
            # Employment status filter: Only allow active employment
            (talent_data['employment_status'].isin(cls.AVAILABLE_EMPLOYMENT_STATUSES)) &
            # Assignment status filter: Exclude support-only roles
            (~talent_data['assignment_status'].isin(cls.EXCLUDED_ASSIGNMENT_STATUSES))
        ]
        
        return available_talent
    
    @classmethod
    def filter_by_role(cls, talent_data: pd.DataFrame, role: str) -> pd.DataFrame:
        """
        Filter talent data by specific role
        
        Args:
            talent_data: DataFrame containing talent information
            role: Role to filter by
            
        Returns:
            DataFrame filtered by role
        """
        if talent_data.empty or not role:
            return talent_data
            
        return talent_data[talent_data['role'] == role]
    
    @classmethod
    def get_available_names(cls, talent_data: pd.DataFrame, role: str = None) -> List[str]:
        """
        Get list of available talent names for dropdown
        
        Args:
            talent_data: DataFrame containing talent information
            role: Optional role filter
            
        Returns:
            List of available talent names
        """
        if talent_data.empty:
            return []
            
        # Apply role filter if specified
        if role:
            filtered_talent = cls.filter_by_role(talent_data, role)
        else:
            filtered_talent = talent_data
            
        # Apply availability filters
        available_talent = cls.filter_available_talent(filtered_talent)
        
        return available_talent['name'].unique().tolist()
    
    @classmethod
    def is_talent_available(cls, talent_record: Dict[str, Any]) -> bool:
        """
        Check if a specific talent is available for new assignments
        
        Args:
            talent_record: Dictionary containing talent information
            
        Returns:
            Boolean indicating if talent is available
        """
        employment_status = talent_record.get('employment_status', '')
        assignment_status = talent_record.get('assignment_status', '')
        
        return (employment_status in cls.AVAILABLE_EMPLOYMENT_STATUSES and 
                assignment_status not in cls.EXCLUDED_ASSIGNMENT_STATUSES)
    
    @classmethod
    def get_exclusion_reason(cls, talent_record: Dict[str, Any]) -> str:
        """
        Get reason why talent is excluded from new assignments
        
        Args:
            talent_record: Dictionary containing talent information
            
        Returns:
            String explaining exclusion reason
        """
        employment_status = talent_record.get('employment_status', '')
        assignment_status = talent_record.get('assignment_status', '')
        
        if employment_status not in cls.AVAILABLE_EMPLOYMENT_STATUSES:
            return f"Employment status: {employment_status}"
        
        if assignment_status in cls.EXCLUDED_ASSIGNMENT_STATUSES:
            return f"Assignment status: {assignment_status}"
        
        return "Available"