"""Run a mocked analysis by patching LLM calls in GroqLangChainTableAnalyzer.
This script does NOT call any external LLM. It returns a deterministic report
that mimics the structure of the real analyzer.
"""
import json
import sqlite3
from groq_langchain_analyzer import GroqLangChainTableAnalyzer
from retention_manager import RetentionClassCode, RetentionManager


def mock_analyze_archival_columns_with_llm(self, table_name, schema, group):
    # Get RCC classification first
    rcc_result = mock_classify_table_rcc(self, table_name, schema, "")
    assigned_rcc = rcc_result.get("assigned_rcc")
    
    # Get retention analysis based on RCC
    retention_analysis = mock_analyze_retention_columns(self, table_name, schema, assigned_rcc)
    
    # Get retention rule for strategy
    rm = RetentionManager()
    rule = rm.available_rccs.get(assigned_rcc)
    retention_strategy = f"{rule.retention_type.value} - {rule.years} years" if rule else "Unknown"
    retention_recommendation = f"{rule.description}" if rule else "Manual review required"
    
    return {
        "retention_strategy": retention_strategy,
        "retention_recommendation": retention_recommendation,
        "confidence": rcc_result.get("confidence", 8),
        "retention_reasoning": rcc_result.get("reasoning", "Mocked RCC-based analysis"),
        "rcc_classification": rcc_result,
        "retention_analysis": retention_analysis
    }


def mock_classify_table_rcc(self, table_name, schema, content_hint=""):
    # Naive mapping based on table name for mock
    name = table_name.lower()
    if "invoice" in name or "payment" in name:
        assigned = "FIN_TRANS"
    elif "audit" in name or "log" in name:
        assigned = "AUDIT_LOG"
    elif "employee" in name or "hr_" in name:
        assigned = "HR_REC"
    else:
        assigned = "CUST_DATA"

    return {
        "assigned_rcc": assigned,
        "confidence": 9,
        "reasoning": f"Mocked RCC assignment for {table_name} based on table name patterns"
    }


def mock_analyze_retention_columns(self, table_name, schema, rcc_code):
    # Use RetentionManager hints to select plausible columns
    rm = RetentionManager()
    hints = rm.get_lookup_hints(rcc_code) or []

    # Map hint tokens to plausible column names in the schema (naive)
    # Prefer common names if present, otherwise return hint token as-is
    lc = schema.lower()
    selected = []
    for h in hints:
        if h in ["creation_date", "created_at", "created"] and "created" in lc:
            selected.append("created_at")
        elif h in ["transaction_date", "posted_at"] and "invoice" in table_name.lower():
            selected.append("transaction_date")
        elif h in ["active_flag", "is_active", "enabled"] and ("active" in lc or "is_active" in lc):
            selected.append("is_active")
        elif h in ["termination_date", "end_date"] and ("terminated" in lc or "termination" in lc or "end_date" in lc):
            selected.append("termination_date")
        else:
            # fallback to hint token itself
            selected.append(h)

    reasoning = f"Mocked retention lookup for RCC {rcc_code}: selected {selected} based on hints {hints}"
    return {
        "retention_lookup_columns": selected,
        "reasoning": reasoning
    }


def mock_categorize_tables_with_llm(self, table_schemas):
    # Return a simple categorization: put all tables into a single group for mocking
    results = {}
    for table_name in table_schemas.keys():
        results[table_name] = {
            "group": "DEFAULT_GROUP",
            "related_tables": [],
            "relationship_type": "PEER",
            "common_identifiers": [],
            "confidence": 9,
            "reasoning": "Mocked grouping: default single group"
        }
    return results


def mock_determine_priorities_with_llm(self, group_name, group_tables, relationships):
    result = {}
    for t in group_tables:
        result[t] = {
            "intra_group_priority": 2,
            "priority_type": "INDEPENDENT",
            "foreign_keys": [],
            "referenced_by": [],
            "reasoning": "Mocked priority: medium"
        }
    return result


if __name__ == "__main__":
    # Monkeypatch methods
    GroqLangChainTableAnalyzer.analyze_archival_columns_with_llm = mock_analyze_archival_columns_with_llm
    GroqLangChainTableAnalyzer.classify_table_rcc = mock_classify_table_rcc
    GroqLangChainTableAnalyzer.analyze_retention_columns = mock_analyze_retention_columns
    GroqLangChainTableAnalyzer.categorize_tables_with_llm = mock_categorize_tables_with_llm
    GroqLangChainTableAnalyzer.determine_priorities_with_llm = mock_determine_priorities_with_llm

    # Run analyzer on sample DB
    db_path = "table_group_archival_demo.sqlite"
    analyzer = GroqLangChainTableAnalyzer(db_path)
    report = analyzer.create_comprehensive_report()

    # Print a compact report summary
    summary = {
        "timestamp": report.get("analysis_timestamp"),
        "total_tables": report.get("total_tables"),
        "total_groups": report.get("total_groups"),
        "sample_table_analysis": {}
    }

    # Pick up to 5 tables to display
    for i, (tname, info) in enumerate(report.get("table_analysis", {}).items()):
        if i >= 5:
            break
        summary["sample_table_analysis"][tname] = {
            "group": info.get("group"),
            "rcc": info.get("rcc_classification"),
            "retention_lookup": info.get("retention_lookup"),
            "retention_reasoning": info.get("retention_reasoning")
        }

    print(json.dumps(summary, indent=2))
