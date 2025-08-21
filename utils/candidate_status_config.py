"""
Candidate Status Configuration
Centralized management of standardized candidate statuses for consistent application behavior.
"""

class CandidateStatusConfig:
    """
    Centralized configuration for candidate statuses to ensure consistency
    across all forms and interfaces in the application.
    """
    
    # Standard candidate statuses - always available regardless of data
    STANDARD_STATUSES = [
        "Dropped",
        "On Boarded", 
        "On Hold",
        "Rejected",
        "Screening",
        "Selected",
        "Tech Round"
    ]
    
    # Status descriptions for reference
    STATUS_DESCRIPTIONS = {
        "Screening": "Initial candidate evaluation and screening phase",
        "Tech Round": "Technical interview and assessment phase", 
        "Selected": "Candidate has been selected for the position",
        "On Hold": "Candidate process temporarily paused",
        "Rejected": "Candidate application declined",
        "Dropped": "Candidate withdrew or was removed from process",
        "On Boarded": "Candidate successfully hired and onboarded"
    }
    
    # Status categories for workflow management
    ACTIVE_STATUSES = ["Screening", "Tech Round", "Selected", "On Hold"]
    FINAL_STATUSES = ["On Boarded", "Rejected", "Dropped"]
    
    @classmethod
    def get_all_statuses(cls, include_existing_data=True, existing_statuses=None):
        """
        Get complete list of statuses, optionally including any additional
        statuses from existing data.
        
        Args:
            include_existing_data (bool): Whether to include statuses from existing data
            existing_statuses (list): List of existing statuses from database
            
        Returns:
            list: Sorted list of all available statuses
        """
        statuses = cls.STANDARD_STATUSES.copy()
        
        if include_existing_data and existing_statuses:
            # Add any additional statuses from existing data
            for status in existing_statuses:
                if status and status not in statuses:
                    statuses.append(status)
        
        return sorted(statuses)
    
    @classmethod
    def is_valid_status(cls, status):
        """
        Check if a status is valid (either standard or acceptable custom).
        
        Args:
            status (str): Status to validate
            
        Returns:
            bool: True if status is valid
        """
        return status in cls.STANDARD_STATUSES or bool(status and status.strip())
    
    @classmethod
    def get_status_category(cls, status):
        """
        Get the category of a status (active, final, or other).
        
        Args:
            status (str): Status to categorize
            
        Returns:
            str: Category ('active', 'final', or 'other')
        """
        if status in cls.ACTIVE_STATUSES:
            return 'active'
        elif status in cls.FINAL_STATUSES:
            return 'final'
        else:
            return 'other'