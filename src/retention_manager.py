from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Optional

class RetentionType(Enum):
    """Types of retention rules"""
    ACTIVE_PLUS = "active_plus"      # e.g., "active + 7 years"
    CREATION_BASED = "creation_based" # e.g., "7 years from creation"
    EVENT_BASED = "event_based"      # e.g., "5 years after termination"

@dataclass
class RetentionRule:
    """Definition of a retention rule"""
    years: int
    retention_type: RetentionType
    description: str
    # Suggested lookup column types for identifying retention keys in a table schema.
    # e.g., ['creation_date', 'active_flag'] or ['event_date']
    lookup_column_hints: List[str] = None

class RetentionClassCode(Enum):
    """Available Retention Class Codes (RCC)"""
    # Financial & Tax Records
    CFA360 = ("CFA360", RetentionRule(
        years=10,
        retention_type=RetentionType.CREATION_BASED,
        description="Financial statements and reports - 10 years from created date",
        lookup_column_hints=["creation_date", "document_date"]
    ))
    # TAX_DOC = ("TAX_DOC", RetentionRule(
    #     years=7,
    #     retention_type=RetentionType.CREATION_BASED,
    #     description="Tax documents and returns - 7 years from creation",
    #     lookup_column_hints=["creation_date", "document_date"]
    # ))
    BNK460 = ("BNK460", RetentionRule(
        years=10,
        retention_type=RetentionType.CREATION_BASED,
        description="Financial transactions - 10 years from created date",
        lookup_column_hints=["created_date","created_at","settlement_date"]
    ))

    # Legal & Compliance
    LEG460 = ("LEG460", RetentionRule(
        years=10,
        retention_type=RetentionType.ACTIVE_PLUS,
        description="Legal contracts - retain active + 10 years",
        lookup_column_hints=["active_flag", "created_at"]
    ))
    LEG120 = ("LEG120", RetentionRule(
        years=10,
        retention_type=RetentionType.CREATION_BASED,
        description="Compliance documents - 10 years from created date",
        lookup_column_hints=["created_date", "created_at"]
    ))
    ADM150 = ("ADM150", RetentionRule(
        years=1,
        retention_type=RetentionType.CREATION_BASED,
        description="Audit logs - 1 year from creation",
        lookup_column_hints=["created_at"]
    ))

    # Customer & Business
    CFA340 = ("CFA340", RetentionRule(
        years=10,
        retention_type=RetentionType.CREATION_BASED,
        description="Customer Personal Information - 10 years from created date",
        lookup_column_hints=["created_date",  "created_at"]
    ))
    # BUS_TRANS = ("BUS_TRANS", RetentionRule(
    #     years=7,
    #     retention_type=RetentionType.CREATION_BASED,
    #     description="Business transactions - 7 years from transaction date",
    #     lookup_column_hints=["transaction_date", "created_at"]
    # ))
    
    # # HR & Personnel
    # HR_REC = ("HR_REC", RetentionRule(
    #     years=7,
    #     retention_type=RetentionType.EVENT_BASED,
    #     description="HR records - 7 years from employment termination",
    #     lookup_column_hints=["termination_date", "end_date", "employment_end"]
    # ))
    # EMP_DATA = ("EMP_DATA", RetentionRule(
    #     years=5,
    #     retention_type=RetentionType.EVENT_BASED,
    #     description="Employee data - 5 years from employment termination",
    #     lookup_column_hints=["termination_date", "created_at"]
    # ))

    def __init__(self, code: str, rule: RetentionRule):
        self.code = code
        self.rule = rule

class RetentionManager:
    """Manages retention classification and analysis"""
    def __init__(self):
        # Build a quick lookup map of RCC -> RetentionRule
        self._rcc_map = {rcc.code: rcc.rule for rcc in RetentionClassCode}

    def get_lookup_hints(self, rcc_code: str) -> Optional[List[str]]:
        """Return suggested lookup column hint tokens for an RCC"""
        rule = self._rcc_map.get(rcc_code)
        if not rule:
            return None
        return rule.lookup_column_hints or []

    @property
    def available_rccs(self) -> Dict[str, RetentionRule]:
        """Get all available RCCs and their rules"""
        return self._rcc_map