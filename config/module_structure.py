"""
Module Structure Configuration for GA AlignOps
Reorganized according to business requirements:
- Demand: Sales Data and Planning
- Supply: Talent Management and Demand-Supply Mapping
- Billing: Planned vs Actual Billing
- Insights & Analytics: Overall reporting and analytics
"""

from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class ModuleConfig:
    """Module configuration data class"""
    name: str
    icon: str
    description: str
    sub_modules: List[str]
    permissions: List[str]
    features: List[str]

class ModuleStructure:
    """
    Restructured module organization for GA AlignOps
    Aligns with business requirements and consolidates related functionality
    """
    
    def __init__(self):
        self.modules = self._initialize_modules()
    
    def _initialize_modules(self) -> Dict[str, ModuleConfig]:
        """Initialize the restructured module configuration"""
        return {
            "Demand Planning": ModuleConfig(
                name="Demand Planning",
                icon="📋",
                description="Sales Data and Planning - Target setting, demand forecasting, and sales planning",
                sub_modules=[
                    "Target Setting",
                    "Demand Tweaking", 
                    "Editable Plan View",
                    "Sales Dashboard",
                    "Forecasting Engine"
                ],
                permissions=[
                    "view_demand_planning",
                    "edit_targets",
                    "adjust_demand",
                    "view_sales_data",
                    "run_forecasts"
                ],
                features=[
                    "ML-powered forecasting",
                    "Interactive demand planning",
                    "Real-time sales metrics",
                    "Scenario planning"
                ]
            ),
            
            "Supply Management": ModuleConfig(
                name="Supply Management", 
                icon="🔄",
                description="Talent Management and Demand-Supply Mapping - Staffing, talent pool, and resource allocation",
                sub_modules=[
                    "Talent Management",
                    "Staffing Planning",
                    "Demand-Supply Mapping",  # Moved from separate module
                    "Resource Allocation",
                    "Skills Matrix"
                ],
                permissions=[
                    "view_supply_management",
                    "manage_talent",
                    "create_staffing_plans",
                    "map_demand_supply",
                    "allocate_resources"
                ],
                features=[
                    "Talent pool management",
                    "Staffing plan creation",
                    "Demand-supply matching",
                    "Skills-based allocation"
                ]
            ),
            
            "Billing Management": ModuleConfig(
                name="Billing Management",
                icon="💰", 
                description="Planned vs Actual Billing - Billing cycles, invoice tracking, and financial reconciliation",
                sub_modules=[
                    "Planned Billing",
                    "Actual Billing",
                    "Billing Reconciliation",
                    "Invoice Management",
                    "Financial Reports"
                ],
                permissions=[
                    "view_billing",
                    "create_billing_plans",
                    "track_actual_billing",
                    "reconcile_billing",
                    "manage_invoices"
                ],
                features=[
                    "Planned vs actual tracking",
                    "Billing cycle management",
                    "Invoice automation",
                    "Financial reconciliation"
                ]
            ),
            
            "Insights & Analytics": ModuleConfig(
                name="Insights & Analytics",
                icon="📊",
                description="Overall reporting and analytics - Performance metrics, trends, and business intelligence",
                sub_modules=[
                    "Analytics Dashboard",
                    "Performance Monitor",
                    "Business Intelligence",
                    "Trend Analysis",
                    "Custom Reports"
                ],
                permissions=[
                    "view_analytics",
                    "access_dashboard",
                    "generate_reports",
                    "analyze_trends",
                    "export_data"
                ],
                features=[
                    "Interactive dashboards",
                    "Real-time metrics",
                    "Trend analysis",
                    "Custom reporting",
                    "Data export"
                ]
            ),
            
            "Settings": ModuleConfig(
                name="Settings",
                icon="⚙️",
                description="System configuration, user management, and environment settings",
                sub_modules=[
                    "User Management",
                    "Role Management", 
                    "Environment Settings",
                    "Google Sheets Config",
                    "OAuth Settings"
                ],
                permissions=[
                    "manage_users",
                    "manage_roles",
                    "configure_system",
                    "manage_integrations",
                    "access_settings"
                ],
                features=[
                    "User role management",
                    "Permission control",
                    "Environment configuration",
                    "Integration settings"
                ]
            )
        }
    
    def get_module_config(self, module_name: str) -> ModuleConfig:
        """Get configuration for a specific module"""
        return self.modules.get(module_name)
    
    def get_all_modules(self) -> Dict[str, ModuleConfig]:
        """Get all module configurations"""
        return self.modules
    
    def get_module_names(self) -> List[str]:
        """Get list of all module names"""
        return list(self.modules.keys())
    
    def get_module_icons(self) -> Dict[str, str]:
        """Get module name to icon mapping"""
        return {name: config.icon for name, config in self.modules.items()}
    
    def get_sub_modules(self, module_name: str) -> List[str]:
        """Get sub-modules for a specific module"""
        config = self.get_module_config(module_name)
        return config.sub_modules if config else []
    
    def get_module_permissions(self, module_name: str) -> List[str]:
        """Get permissions for a specific module"""
        config = self.get_module_config(module_name)
        return config.permissions if config else []
    
    def get_module_features(self, module_name: str) -> List[str]:
        """Get features for a specific module"""
        config = self.get_module_config(module_name)
        return config.features if config else []
    
    def validate_module_access(self, module_name: str, permission: str) -> bool:
        """Validate if a module has a specific permission"""
        config = self.get_module_config(module_name)
        if not config:
            return False
        return permission in config.permissions
    
    def get_navigation_structure(self) -> List[tuple]:
        """Get navigation structure for the sidebar"""
        return [(config.icon, name) for name, config in self.modules.items()]
    
    def get_module_description(self, module_name: str) -> str:
        """Get description for a specific module"""
        config = self.get_module_config(module_name)
        return config.description if config else "Module not found"
    
    def is_valid_module(self, module_name: str) -> bool:
        """Check if a module name is valid"""
        return module_name in self.modules
    
    def get_consolidated_modules(self) -> Dict[str, List[str]]:
        """Get modules with their consolidated sub-modules"""
        return {
            name: config.sub_modules 
            for name, config in self.modules.items()
        }

# Global instance for easy access
module_structure = ModuleStructure()
