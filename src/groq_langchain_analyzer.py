
# Fixed LangChain Database Table Analysis Implementation for ChatGroq
import sqlite3
import os
from datetime import datetime
import json
import re
from typing import Dict, List, Tuple, Any

# LangChain imports for ChatGroq
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_classic.chains import LLMChain
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_groq import ChatGroq  # Changed to ChatGroq
from dotenv import load_dotenv

# Local imports
from retention_manager import RetentionManager, RetentionClassCode, RetentionType

load_dotenv()

class GroqLangChainTableAnalyzer:
    """
    LangChain implementation using ChatGroq for database table categorization
    Fixed JSON parsing issues and removed fallback approaches
    """

    def __init__(self, db_path: str, mock_mode: bool = False):
        self.db_path = db_path
        self.mock_mode = mock_mode

        # Initialize LangChain SQLDatabase
        self.db = SQLDatabase.from_uri(f"sqlite:///{db_path}")

        if not mock_mode:
            # Initialize ChatGroq LLM
            self.llm = ChatGroq(
                model="llama-3.3-70b-versatile",  # or use "mixtral-8x7b-32768"
                temperature=0
            )

            # Create SQL toolkit
            self.toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
            self.tools = self.toolkit.get_tools()
        else:
            # Mock mode doesn't need LLM or toolkit initialization
            self.llm = None
            self.toolkit = None
            self.tools = []
            
            # Import mock methods dynamically to avoid circular imports
            from run_mock_analysis import (
                
                mock_classify_table_rcc,
                mock_analyze_retention_columns,
                mock_categorize_tables_with_llm,
                mock_determine_priorities_with_llm
            )
            
            # Apply mock methods when in mock mode
            # self.analyze_archival_columns_with_llm = mock_analyze_archival_columns_with_llm.__get__(self)
            self.classify_table_rcc = mock_classify_table_rcc.__get__(self)
            self.analyze_retention_columns = mock_analyze_retention_columns.__get__(self)
            self.categorize_tables_with_llm = mock_categorize_tables_with_llm.__get__(self)
            self.determine_priorities_with_llm = mock_determine_priorities_with_llm.__get__(self)

        # Group definitions will be dynamically created based on relationships
        self.group_definitions = {}  # Will be populated during analysis
        
        # Initialize retention manager
        self.retention_manager = RetentionManager()

        # Step 1: Relationship-based table categorization prompt
        self.categorization_prompt = PromptTemplate(
            input_variables=["table_schemas", "relationships_data"],
            template="""You are a database analyst. Create groups of related tables that should be purged together.

GROUPING RULES:
1. Tables with direct foreign key relationships MUST be in the same group
2. Tables sharing common business objects (e.g., customer_id in multiple tables) should be in the same group
3. Look for naming patterns indicating relationships (e.g., order_* tables)
4. Keep number of groups minimal (ideally 3-5 groups) by combining related business concepts
5. Each table MUST belong to exactly one group
6. Name groups based on the primary business entity or process they represent

Table Definitions and Relationships:
{table_schemas}

Relationship Data:
{relationships_data}

IMPORTANT: Return ONLY valid JSON in this exact format with no additional text:

{{
  "groups": {{
    "GROUP_NAME": {{
      "description": "Brief description of what this group represents",
      "primary_entity": "The main business entity or process this group revolves around"
    }}
  }},
  "analysis": {{
    "table_name": {{
      "group": "GROUP_NAME",
      "reasoning": "explanation focusing on relationships and why tables must be processed together"
    }}
  }}
}}"""
        )

        # Step 2.1 RCC Classification prompt
        self.rcc_classification_prompt = PromptTemplate(
            input_variables=["table_schema", "table_content", "available_rccs"],
            template="""You are a data retention expert. Classify this database table into the most appropriate Retention Class Code (RCC) based on its schema and content.

Table Schema:
{table_schema}

Table Content Hint: {table_content}

Available RCCs:
{available_rccs}

CLASSIFICATION RULES:
1. Analyze the table name, column names, and data types to determine the business purpose
2. Match the table's purpose to the most appropriate RCC category
3. Consider the data sensitivity and retention requirements
4. Look for key indicators like: financial data, audit logs, customer data, HR records, etc.

Return ONLY valid JSON in this exact format:

{{
    "assigned_rcc": "RCC_CODE",
    "reasoning": "Detailed explanation of why this RCC was chosen based on table characteristics"
}}
"""
        )

        # Step 2.2 Prompt for finding the retention lookup column
#         self.retention_column_prompt = PromptTemplate(
#             input_variables=["table_schema", "rcc_type", "retention_context", "retention_years", "rcc_hints"],
#             template="""Analyze this table schema to find the most appropriate columns to use as retention lookup keys based on the RCC guidance.

# Table Definition:
# {table_schema}

# RCC Type: {rcc_type}
# RCC Hints: {rcc_hints}
# Retention Duration: {retention_years} years
# Context: {retention_context}

# Task: Based on the RCC hints and table schema, return a JSON object with:
# - "retention_lookup_columns": an ordered list of column names (strings) that together can be used to determine retention (for example: ["is_active","created_at"]).
# - "reasoning": explanation why these columns are appropriate and how they map to the RCC requirements.

# Return ONLY valid JSON in this exact format:

# {{
#     "retention_lookup_columns": ["col1", "col2", "col3"],
#     "reasoning": "explain why"
# }}
# """
#         )
        self.retention_column_prompt = PromptTemplate(
    input_variables=["table_schema", "rcc_type", "retention_context", "retention_years", "rcc_hints"],
    template="""You are a data retention expert tasked with identifying the most appropriate columns to use as retention lookup keys for a database table. Use the provided RCC guidance and table schema to make your decision.

Table Details:
- Table Schema: {table_schema}
- RCC Type: {rcc_type}
- Retention Duration: {retention_years} years
- RCC Hints: {rcc_hints}
- Context: {retention_context}

Instructions:
1. Analyze the Table Schema:
   - Look at column names, data types, and any naming conventions that indicate timestamps, activity flags, or event markers.
   - Prioritize columns that align with the RCC hints provided.

2. Retention Lookup Column Selection:
   - For Creation-Based Retention: Focus on columns that indicate when the record was created (e.g., `created_at`, `creation_date`).
   - For Active-Plus Retention: Focus on columns that indicate whether the record is active (e.g., `is_active`, `active_flag`).
   - For Event-Based Retention: Focus on columns that track specific events (e.g., `termination_date`, `last_updated`).

3. Provide a Clear Justification:
   - Explain why the selected columns are appropriate for the retention type and how they map to the RCC requirements.
   - If no suitable columns are found, suggest alternative strategies or highlight gaps in the schema.

Return ONLY valid JSON in this exact format:

{{
    "retention_lookup_columns": ["created_at", "is_active"],
    "reasoning": "The 'created_at' column indicates when the record was created, which aligns with the creation-based retention policy. The 'is_active' column is included as a secondary indicator for active-plus retention."
}}

"""
)

        # Step 3: Relationship analysis and priority assignment prompt
        self.relationship_priority_prompt = PromptTemplate(
            input_variables=["group_name", "tables_with_relationships", "foreign_key_details"],
            template="""You are determining purging priorities for tables in the {group_name} group based on foreign key relationships.

Tables and Relationships:
{tables_with_relationships}

Foreign Key Details:
{foreign_key_details}

PRIORITY ASSIGNMENT RULES:
1. PRIORITY 1 (HIGH - Purge FIRST): 
   - Child tables that have foreign keys but are NOT referenced by other tables
   - Temporary tables, staging tables, log detail tables
   - Example: order_items (has FK to orders), payment_logs (has FK to payments)

2. PRIORITY 2 (MEDIUM - Purge SECOND):
   - Bridge tables that have foreign keys AND are referenced by other tables  
   - Independent tables with no foreign key relationships
   - Junction tables, intermediate processing tables
   - Example: orders (has FK to customers, referenced by order_items)

3. PRIORITY 3 (LOW - Purge LAST):
   - Parent tables that are referenced by other tables but have NO foreign keys
   - Master data tables, lookup tables, configuration tables
   - Example: customers (no FK, referenced by orders), product_catalog (referenced by order_items)

IMPORTANT CONSIDERATIONS:
- Tables with foreign keys should generally be purged before the tables they reference
- Master/reference data should be purged last to maintain referential integrity
- Consider business logic: transaction details before transactions, transactions before customers
- Multi-level hierarchies: grandchild → child → parent → grandparent

Analyze the relationships and assign appropriate priorities within this group.

Return ONLY valid JSON:

{{
  "priority_analysis": {{
    "table_name": {{
      "intra_group_priority": 1,
      "foreign_keys": ["parent_table1", "parent_table2"],
      "referenced_by": ["child_table1", "child_table2"],
      "reasoning": "detailed explanation of priority assignment"
    }}
  }}
}}"""
        )
        

    def get_table_schemas(self):
        """Get table definitions using LangChain SQLDatabase"""
        table_names = self.db.get_usable_table_names()
        schemas = {}

        for table_name in table_names:
            try:
                schema_info = self.db.get_table_info([table_name])
                schemas[table_name] = schema_info
            except Exception as e:
                print(f"Warning: Could not get definition for {table_name}: {e}")

        return schemas

    def analyze_foreign_key_relationships(self):
        """Analyze foreign key relationships using database introspection"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        relationships = {}
        
        for table_name in tables:
            # Get foreign keys for this table
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            
            # Find what tables reference this table
            referenced_by = []
            for other_table in tables:
                if other_table != table_name:
                    cursor.execute(f"PRAGMA foreign_key_list({other_table})")
                    other_fks = cursor.fetchall()
                    for fk in other_fks:
                        if fk[2] == table_name:  # Referenced table
                            referenced_by.append({
                                "child_table": other_table,
                                "child_column": fk[3],
                                "parent_column": fk[4]
                            })
            
            relationships[table_name] = {
                "foreign_keys": [{"parent_table": fk[2], "parent_column": fk[4], "child_column": fk[3]} for fk in foreign_keys],
                "referenced_by": referenced_by,
                "has_foreign_keys": len(foreign_keys) > 0,
                "is_referenced": len(referenced_by) > 0
            }
        
        conn.close()
        return relationships
    
    def parse_json_response(self, response_text: str):
        """Parse JSON from LLM response with comprehensive error handling"""
        try:
            return json.loads(response_text)
        except json.JSONDecodeError:
            # Clean up response
            cleaned = response_text.strip()

            # Remove common LLM prefixes
            prefixes_to_remove = [
                "Here is the analysis in the required JSON format:",
                "Here is the JSON response:",
                "Here's the analysis:",
                "The analysis is:",
                "```json",
                "```",
                "Based on the analysis:"
            ]

            for prefix in prefixes_to_remove:
                if cleaned.startswith(prefix):
                    cleaned = cleaned[len(prefix):].strip()

            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()

            # Extract JSON object
            start_idx = cleaned.find('{')
            if start_idx != -1:
                brace_count = 0
                end_idx = -1
                for i in range(start_idx, len(cleaned)):
                    if cleaned[i] == '{':
                        brace_count += 1
                    elif cleaned[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_idx = i + 1
                            break

                if end_idx != -1:
                    cleaned = cleaned[start_idx:end_idx]

            try:
                return json.loads(cleaned)
            except json.JSONDecodeError as e:
                print(f"ERROR: JSON parsing failed: {e}")
                print(f"Response text: {response_text[:300]}...")
                return {}
    # Step 2.1
    def classify_table_rcc(self, table_name: str, schema: str, content_hint: str = "") -> Dict:
        """Classify a table into a Retention Class Code using LLM"""
        try:
            # Get available RCCs and their rules
            rccs = self.retention_manager.available_rccs
            rcc_descriptions = "\n".join([
                f"{code}: {rule.description} ({rule.retention_type.value}, {rule.years} years)"
                for code, rule in rccs.items()
            ])
            
            # Run LLM classification
            chain = LLMChain(prompt=self.rcc_classification_prompt, llm=self.llm)
            response = chain.run(
                table_schema=schema,
                table_content=content_hint,
                available_rccs=rcc_descriptions
            )
            
            result = self.parse_json_response(response)
            
            # Validate RCC exists
            assigned_rcc = result.get("assigned_rcc")
            if assigned_rcc and assigned_rcc not in rccs:
                # not fatal: return result but note mismatch
                print(f"WARNING: RCC assigned by LLM ({assigned_rcc}) not in available RCCs")
            
            return result
        except Exception as e:
            print(f"ERROR: RCC classification failed for {table_name}: {e}")
            return {}
    # Step 2.2
    def analyze_retention_columns(self, table_name: str, schema: str, rcc_code: str) -> Dict:
        """Find the appropriate retention lookup column based on RCC type"""
        try:
            # Get retention rule for this RCC
            rule = self.retention_manager.available_rccs.get(rcc_code)
            rcc_hints = self.retention_manager.get_lookup_hints(rcc_code) or []
            if not rule:
                return {"error": "Unknown RCC"}

            # Prepare context based on RCC type
            if rule.retention_type == RetentionType.ACTIVE_PLUS:
                context = "Find the column that indicates if the record is still active/current"
            elif rule.retention_type == RetentionType.CREATION_BASED:
                context = "Find the column that records when this record was created"
            else:  # EVENT_BASED
                context = f"Find the column that tracks the timing of: {rule.description}"
            # "table_schema", "rcc_type", "retention_context", "retention_years", "rcc_hints"
            # Run LLM analysis to find the retention lookup column
            chain = LLMChain(prompt=self.retention_column_prompt, llm=self.llm)
            response = chain.run(
                table_schema=schema,
                rcc_type=rule.retention_type.value,
                retention_context=context,
                retention_years=rule.years,
                rcc_hints=rcc_hints
            )
            
            return self.parse_json_response(response)
        except Exception as e:
            print(f"ERROR: Retention column analysis failed for {table_name}: {e}")
            return {"error": str(e)}
    # Step 1
    def categorize_tables_with_llm(self, table_schemas):
        """Step 1: Pure LLM table categorization based on relationships"""
        print("Step 1: Analyzing table relationships and creating dynamic groups...")

        try:
            # Analyze relationships first
            relationships = self.analyze_foreign_key_relationships()
            
            # Format relationship data for LLM
            relationship_text = ""
            for table_name, rel_info in relationships.items():
                relationship_text += f"\nTable: {table_name}\n"
                if rel_info["foreign_keys"]:
                    fk_list = [f"{fk['parent_table']} (via {fk['child_column']})" 
                              for fk in rel_info["foreign_keys"]]
                    relationship_text += f"  References: {', '.join(fk_list)}\n"
                if rel_info["referenced_by"]:
                    ref_list = [f"{ref['child_table']} (via {ref['child_column']})" 
                              for ref in rel_info["referenced_by"]]
                    relationship_text += f"  Referenced by: {', '.join(ref_list)}\n"

            # Format schema text with column info for identifying common identifiers
            schema_text = ""
            for table_name, schema in list(table_schemas.items()):
                schema_text += f"\nTable: {table_name}\n{schema[:400]}\n"

            # Run LLM analysis
            categorization_chain = LLMChain(
                llm=self.llm, 
                prompt=self.categorization_prompt
            )

            response = categorization_chain.run(
                table_schemas=schema_text,
                relationships_data=relationship_text
            )

            result = self.parse_json_response(response)
            
            # Update group definitions with dynamically created groups
            self.group_definitions = result.get("groups", {})
            
            return result.get("analysis", {})

        except Exception as e:
            print(f"ERROR: LLM categorization failed: {e}")
            raise Exception("Pure LLM approach failed - no fallback available")
    # Step 2
    def analyze_archival_columns_with_llm(self, table_name, schema, group):
        """Step 2: RCC-based archival column analysis"""
        print(f"Step 2: Analyzing archival columns for {table_name}...")

        try:
            # First classify the table into an RCC
            rcc_result = self.classify_table_rcc(table_name, schema, "")
            assigned_rcc = rcc_result.get("assigned_rcc")
            
            if not assigned_rcc:
                return {
                    # "retention_strategy": "unknown",
                    # "retention_recommendation": "Manual review required",
                    # "confidence": 1,
                    "retention_reasoning": "Could not classify table into RCC",
                    "rcc_classification": rcc_result
                }

            # Get retention analysis based on the assigned RCC
            retention_analysis = self.analyze_retention_columns(table_name, schema, assigned_rcc)
            
            # Get retention rule for strategy
            # rule = self.retention_manager.available_rccs.get(assigned_rcc)
            # retention_strategy = f"{rule.retention_type.value} - {rule.years} years" if rule else "Unknown"
            # retention_recommendation = f"{rule.description}" if rule else "Manual review required"

            return {
                # "retention_strategy": retention_strategy,
                # "retention_recommendation": retention_recommendation,
                # "confidence": rcc_result.get("confidence", 5),
                "retention_reasoning": rcc_result.get("reasoning", "RCC classification based analysis"),
                "rcc_classification": rcc_result,
                "retention_analysis": retention_analysis
            }

        except Exception as e:
            print(f"ERROR: LLM archival analysis failed for {table_name}: {e}")
            raise Exception("Pure LLM approach failed - no fallback available")
    # Step 3
    def determine_priorities_with_llm(self, group_name, group_tables, relationships):
        """Step 3: Pure LLM relationship-based priority assignment"""
        print(f"Step 3: Determining priorities for {group_name} group...")

        try:
            # Format relationship data for LLM
            tables_info = ""
            fk_details = ""

            for table_name in group_tables:
                if table_name in relationships:
                    rel = relationships[table_name]
                    tables_info += f"\nTable: {table_name}\n"
                    tables_info += f"  Has Foreign Keys: {rel['has_foreign_keys']}\n"
                    tables_info += f"  Is Referenced: {rel['is_referenced']}\n"

                    if rel['foreign_keys']:
                        fk_list = [f"{fk['parent_table']}({fk['parent_column']})" for fk in rel['foreign_keys']]
                        fk_details += f"{table_name} references: {', '.join(fk_list)}\n"

                    if rel['referenced_by']:
                        ref_list = [f"{ref['child_table']}({ref['child_column']})" for ref in rel['referenced_by']]
                        fk_details += f"{table_name} referenced by: {', '.join(ref_list)}\n"

            priority_chain = LLMChain(
                llm=self.llm,
                prompt=self.relationship_priority_prompt
            )

            response = priority_chain.run(
                group_name=group_name,
                tables_with_relationships=tables_info,
                foreign_key_details=fk_details
            )

            result = self.parse_json_response(response)
            return result.get("priority_analysis", {})

        except Exception as e:
            print(f"ERROR: LLM priority analysis failed for {group_name}: {e}")
            raise Exception("Pure LLM approach failed - no fallback available")

    
    def analyze_database_pure_llm(self):
        """Main analysis using ONLY LLM - NO fallback approaches"""
        print("Starting PURE LLM ChatGroq analysis...")
        print("WARNING: No fallback approaches - LLM must succeed or analysis fails")

        # Get table definitions
        print("Extracting table definitions...")
        table_schemas = self.get_table_schemas()
        if not table_schemas:
            raise Exception("Could not extract table definitions")

        # Analyze foreign key relationships
        print("Analyzing foreign key relationships...")
        relationships = self.analyze_foreign_key_relationships()
        # Step 1: LLM categorization
        categorization_results = self.categorize_tables_with_llm(table_schemas)
        if not categorization_results:
            raise Exception("LLM categorization failed")

        print(f"SUCCESS: Categorized {len(categorization_results)} tables")

        # Step 2: LLM archival column analysis for each table
        final_results = {}
        for table_name, cat_info in categorization_results.items():
            if table_name in table_schemas:
                print(f"Analyzing {table_name}...")

                # Get archival columns with RCC-based analysis
                archival_info = self.analyze_archival_columns_with_llm(
                    table_name, 
                    table_schemas[table_name], 
                    cat_info["group"]
                )

                # Combine categorization and archival info (RCC classification is already included in archival_info)
                combined = {**cat_info, **archival_info}

                final_results[table_name] = combined

        # Step 3: Group tables and determine priorities with LLM
        grouped_tables = {}
        for table_name, info in final_results.items():
            group = info["group"]
            if group not in grouped_tables:
                grouped_tables[group] = []
            grouped_tables[group].append(table_name)

        # LLM priority analysis for each group
        for group_name, group_table_list in grouped_tables.items():
            priority_results = self.determine_priorities_with_llm(
                group_name, group_table_list, relationships
            )

            # Apply priority results
            for table_name in group_table_list:
                if table_name in priority_results:
                    priority_info = priority_results[table_name]
                    final_results[table_name].update({
                        "intra_group_priority": priority_info.get("intra_group_priority", 2),
                        "priority_type": priority_info.get("priority_type", "UNKNOWN"),
                        "priority_reasoning": priority_info.get("reasoning", "LLM analysis"),
                        "relationship_info": relationships.get(table_name, {})
                    })

        return final_results

    def create_comprehensive_report(self):
        """Generate comprehensive pure LLM analysis report"""

        try:
            # Perform pure LLM analysis
            analysis_results = self.analyze_database_pure_llm()

            # Group results for display
            grouped_results = {}
            for table_name, info in analysis_results.items():
                group = info["group"]
                if group not in grouped_results:
                    grouped_results[group] = []
                grouped_results[group].append({
                    "table_name": table_name,
                    "intra_group_priority": info.get("intra_group_priority", 2),
                    "priority_type": info.get("priority_type", "UNKNOWN"),
                    "rcc_classification": info.get("rcc_classification"),
                    "retention_analysis": info.get("retention_analysis"),
                    "retention_strategy": info.get("archival_strategy", ""),
                    "confidence": info.get("confidence", 0),
                    "priority_reasoning": info.get("priority_reasoning", ""),
                    "retention_reasoning": info.get("archival_reasoning", "")
                })

            # Sort by priority within groups
            for group_name in grouped_results:
                grouped_results[group_name].sort(key=lambda x: x["intra_group_priority"])

            return {
                "analysis_timestamp": datetime.now().isoformat(),
                "total_tables": len(analysis_results),
                "total_groups": len(grouped_results),
                "llm_used": "ChatGroq (Pure LLM - No Fallbacks)",
                "analysis_type": "Pure LLM: Categorization + Archival Analysis + Relationship-Based Priorities",
                "table_analysis": analysis_results,
                "grouped_by_priority": grouped_results,
                "group_definitions": self.group_definitions
            }

        except Exception as e:
            return {
                "error": f"Pure LLM analysis failed: {str(e)}",
                "analysis_timestamp": datetime.now().isoformat(),
                "llm_used": "ChatGroq (Failed)",
                "note": "No fallback approaches available - LLM must succeed"
            }

# Example usage with ChatGroq
def demonstrate_groq_langchain(mock_mode: bool = False):
    """Demonstrate ChatGroq LangChain implementation

    Args:
        mock_mode (bool): If True, runs analysis with mock data without LLM calls
    """
    # Use existing sample database
    db_path = "table_group_archival_demo.sqlite"

    if not os.path.exists(db_path):
        print("ERROR: Sample database not found")
        return

    if not mock_mode:
        # Initialize with Groq API key in real mode
        groq_api_key = os.getenv("GROQ_API_KEY")
        if not groq_api_key:
            print("ERROR: GROQ_API_KEY environment variable not set")
            print("Set it with: export GROQ_API_KEY='your-groq-api-key'")
            print("Or run with --mock flag for testing without API key")
            return
    
    # Initialize analyzer with appropriate mode
    analyzer = GroqLangChainTableAnalyzer(db_path, mock_mode=mock_mode)

    # Generate report using ChatGroq
    report = analyzer.create_comprehensive_report()

    if "error" in report:
        print(f"ERROR: {report['error']}")
        return

    print("\n" + "="*60)
    print("CHATGROQ LANGCHAIN DATABASE ANALYSIS")
    print("="*60)
    print(f"LLM Used: {report.get('llm_used', 'ChatGroq')}")
    print(f"Total Tables: {report.get('total_tables', 0)}")
    print(f"Total Groups: {report.get('total_groups', 0)}")

    # Display results
    print("\nTABLE ANALYSIS:")
    for table_name, info in report.get("table_analysis", {}).items():
        lookup = info.get("retention_lookup_column") or info.get("retention_lookup") or {}
        cols = lookup.get("retention_lookup_columns") if isinstance(lookup, dict) else lookup
        cols_str = ", ".join(cols) if cols else ""
        print(f"\nTABLE: {table_name}")
        print(f"   Group: {info['group']}")
        print(f"   Retention Lookup Columns: {cols_str}")
        print(f"   Priority: {info['intra_group_priority']}")
        print(f"   Confidence: {info.get('confidence', 0)}/10")

    # Display intra-group priorities
    print("\nINTRA-GROUP PRIORITIES:")
    for group_name, tables in report.get("grouped_by_priority", {}).items():
        print(f"\nGROUP: {group_name}")
        for table_info in tables:
            priority_desc = {1: "HIGH", 2: "MEDIUM", 3: "LOW"}[table_info["intra_group_priority"]]
            print(f"   Priority {table_info['intra_group_priority']} ({priority_desc}): {table_info['table_name']}")

    return report

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run database analysis with GroqLangChain")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode without LLM calls")
    args = parser.parse_args()
    
    # Run with appropriate mode
    report = demonstrate_groq_langchain(mock_mode=args.mock)
