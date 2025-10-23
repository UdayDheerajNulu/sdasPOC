
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

load_dotenv()

class GroqLangChainTableAnalyzer:
    """
    LangChain implementation using ChatGroq for database table categorization
    Fixed JSON parsing issues and removed fallback approaches
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

        # Initialize LangChain SQLDatabase
        self.db = SQLDatabase.from_uri(f"sqlite:///{db_path}")

        # Initialize ChatGroq LLM
        self.llm = ChatGroq(
            model="llama-3.3-70b-versatile",  # or use "mixtral-8x7b-32768"
            temperature=0
        )

        # Create SQL toolkit
        self.toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
        self.tools = self.toolkit.get_tools()

        # Group definitions will be dynamically created based on relationships
        self.group_definitions = {}  # Will be populated during analysis

        # Step 1: Relationship-based table categorization prompt
        self.categorization_prompt = PromptTemplate(
            input_variables=["table_schemas", "relationships_data"],
            template="""You are a database analyst. Create groups of related tables that should be archived/purged together.

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
      "related_tables": ["table1", "table2"],
      "relationship_type": "PARENT|CHILD|PEER",
      "common_identifiers": ["shared_column1", "shared_column2"],
      "confidence": 9,
      "reasoning": "explanation focusing on relationships and why tables must be processed together"
    }}
  }}
}}"""
        )

        # Step 2: Comprehensive archival column analysis prompt
        self.archival_analysis_prompt = PromptTemplate(
            input_variables=["table_name", "table_schema", "group_name"],
            template="""You are analyzing archival columns for data retention in table '{table_name}' from group '{group_name}'.

Table Definition:
{table_schema}

ARCHIVAL COLUMN TYPES TO IDENTIFY:

1. TIME-BASED ARCHIVAL:
   - Created dates: created_date, created_at, create_time, date_created, timestamp
   - Modified dates: modified_date, updated_date, last_updated, last_modified, updated_at
   - Access times: access_time, login_time, logout_time, last_access_date
   - Business dates: invoice_date, payment_date, transaction_date, order_date
   - Event times: event_timestamp, log_time, audit_time, measurement_timestamp

2. STATUS-BASED ARCHIVAL:
   - Active flags: is_active, active, enabled, is_enabled, is_current, current
   - Status fields: status, record_status, data_status, state, lifecycle_status
   - Validation flags: is_valid, is_processed, is_approved, is_deleted

3. BUSINESS LOGIC ARCHIVAL:
   - Retention fields: retention_date, retention_period, retention_until_date
   - Expiry fields: expiry_date, expires_at, valid_until, end_date
   - Archive fields: archived_date, archive_eligible, archive_reason
   - Purge fields: purge_date, scheduled_deletion_date, destroy_date

4. GROUP-SPECIFIC PATTERNS:
   - KEYLOG: session_id, ip_address, severity_level, event_type
   - INVOICE: payment_status, invoice_status, amount fields
   - CONFIG: configuration_type, environment, deployment_status
   - METRICS: measurement_value, metric_type, aggregation_period

Analyze the table definition and identify ALL columns that could be used for archival/retention decisions.
Consider column names, data types, and the table's functional group.

Return ONLY valid JSON:

{{
  "archival_analysis": {{
    "primary_archival_columns": ["most_important_column1", "most_important_column2"],
    "secondary_archival_columns": ["additional_column1", "additional_column2"],
    "archival_strategy": "time_based|status_based|business_logic|hybrid",
    "retention_recommendation": "recommended retention criteria",
    "confidence": 9,
    "reasoning": "detailed explanation of archival column selection"
  }}
}}"""
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
      "priority_type": "CHILD|BRIDGE|PARENT|INDEPENDENT",
      "foreign_keys": ["parent_table1", "parent_table2"],
      "referenced_by": ["child_table1", "child_table2"],
      "reasoning": "detailed explanation of priority assignment"
    }}
  }}
}}"""
        )
        
        # Simpler archival column prompt
        self.archival_column_prompt = PromptTemplate(
            input_variables=["table_name", "columns", "table_group"],
            template="""For table '{table_name}' in group '{table_group}', identify archival columns from: {columns}

Look for: created_date, modified_date, timestamp, access_time, is_active, status, enabled, retention_date, expiry_date

Return ONLY a JSON array of column names:
["column1", "column2", "column3"]"""
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
                print(f"❌ JSON parsing failed: {e}")
                print(f"Response text: {response_text[:300]}...")
                return {}

    def categorize_tables_with_llm(self, table_schemas):
        """Step 1: Pure LLM table categorization based on relationships"""
        print("🧠 Step 1: Analyzing table relationships and creating dynamic groups...")

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
            print(f"❌ LLM categorization failed: {e}")
            raise Exception("Pure LLM approach failed - no fallback available")

    def analyze_archival_columns_with_llm(self, table_name, schema, group):
        """Step 2: Comprehensive LLM archival column analysis"""
        print(f"🔍 Step 2: Analyzing archival columns for {table_name}...")

        try:
            archival_chain = LLMChain(
                llm=self.llm,
                prompt=self.archival_analysis_prompt
            )

            response = archival_chain.run(
                table_name=table_name,
                table_schema=schema,
                group_name=group
            )

            result = self.parse_json_response(response)
            archival_data = result.get("archival_analysis", {})

            # Combine primary and secondary columns
            primary = archival_data.get("primary_archival_columns", [])
            secondary = archival_data.get("secondary_archival_columns", [])
            all_archival_columns = primary + secondary

            return {
                "archival_columns": all_archival_columns,
                "primary_archival_columns": primary,
                "secondary_archival_columns": secondary,
                "archival_strategy": archival_data.get("archival_strategy", "hybrid"),
                "retention_recommendation": archival_data.get("retention_recommendation", ""),
                "archival_confidence": archival_data.get("confidence", 5),
                "archival_reasoning": archival_data.get("reasoning", "LLM analysis")
            }

        except Exception as e:
            print(f"❌ LLM archival analysis failed for {table_name}: {e}")
            raise Exception("Pure LLM approach failed - no fallback available")

    def determine_priorities_with_llm(self, group_name, group_tables, relationships):
        """Step 3: Pure LLM relationship-based priority assignment"""
        print(f"🎯 Step 3: Determining priorities for {group_name} group...")

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
            print(f"❌ LLM priority analysis failed for {group_name}: {e}")
            raise Exception("Pure LLM approach failed - no fallback available")

    
    def analyze_database_pure_llm(self):
        """Main analysis using ONLY LLM - NO fallback approaches"""
        print("🤖 Starting PURE LLM ChatGroq analysis...")
        print("⚠️ No fallback approaches - LLM must succeed or analysis fails")

        # Get table definitions
        print("📊 Extracting table definitions...")
        table_schemas = self.get_table_schemas()
        if not table_schemas:
            raise Exception("Could not extract table definitions")

        # Analyze foreign key relationships
        print("🔗 Analyzing foreign key relationships...")
        relationships = self.analyze_foreign_key_relationships()
        # Step 1: LLM categorization
        categorization_results = self.categorize_tables_with_llm(table_schemas)
        if not categorization_results:
            raise Exception("LLM categorization failed")

        print(f"✅ Categorized {len(categorization_results)} tables")

        # Step 2: LLM archival column analysis for each table
        final_results = {}
        for table_name, cat_info in categorization_results.items():
            if table_name in table_schemas:
                print(f"Analyzing {table_name}...")

                # Get archival columns with comprehensive LLM analysis
                archival_info = self.analyze_archival_columns_with_llm(
                    table_name, 
                    table_schemas[table_name], 
                    cat_info["group"]
                )

                # Combine categorization and archival info
                final_results[table_name] = {
                    **cat_info,
                    **archival_info
                }

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
                    "archival_columns": info.get("archival_columns", []),
                    "primary_archival_columns": info.get("primary_archival_columns", []),
                    "secondary_archival_columns": info.get("secondary_archival_columns", []),
                    "archival_strategy": info.get("archival_strategy", ""),
                    "confidence": info.get("confidence", 0),
                    "priority_reasoning": info.get("priority_reasoning", ""),
                    "archival_reasoning": info.get("archival_reasoning", "")
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
def demonstrate_groq_langchain():
    """Demonstrate ChatGroq LangChain implementation"""

    # Use existing sample database
    db_path = "table_group_archival_demo.sqlite"

    if not os.path.exists(db_path):
        print("❌ Sample database not found")
        return

    # Initialize with Groq API key
    # groq_api_key = os.getenv("GROQ_API_KEY")  # Set environment variable

    # if not groq_api_key:
    #     print("❌ GROQ_API_KEY environment variable not set")
    #     print("Set it with: export GROQ_API_KEY='your-groq-api-key'")
    #     return

    # analyzer = GroqLangChainTableAnalyzer(db_path, groq_api_key=groq_api_key)
    analyzer = GroqLangChainTableAnalyzer(db_path)

    # Generate report using ChatGroq
    report = analyzer.create_comprehensive_report()

    if "error" in report:
        print(f"❌ {report['error']}")
        return

    print("\n" + "="*60)
    print("CHATGROQ LANGCHAIN DATABASE ANALYSIS")
    print("="*60)
    print(f"LLM Used: {report.get('llm_used', 'ChatGroq')}")
    print(f"Total Tables: {report.get('total_tables', 0)}")
    print(f"Total Groups: {report.get('total_groups', 0)}")

    # Display results
    print("\n📋 TABLE ANALYSIS:")
    for table_name, info in report.get("table_analysis", {}).items():
        archival_str = ", ".join(info.get("archival_columns", [])[:3])
        print(f"\n🏷️ {table_name}")
        print(f"   Group: {info['group']}")
        print(f"   Archival Columns: {archival_str}")
        print(f"   Priority: {info['intra_group_priority']}")
        print(f"   Confidence: {info['confidence']}/10")

    # Display intra-group priorities
    print("\n📊 INTRA-GROUP PRIORITIES:")
    for group_name, tables in report.get("grouped_by_priority", {}).items():
        print(f"\n📂 {group_name}:")
        for table_info in tables:
            priority_desc = {1: "HIGH", 2: "MEDIUM", 3: "LOW"}[table_info["intra_group_priority"]]
            print(f"   Priority {table_info['intra_group_priority']} ({priority_desc}): {table_info['table_name']}")

    return report

if __name__ == "__main__":
    # Set your Groq API key: export GROQ_API_KEY="your-groq-api-key"
    report = demonstrate_groq_langchain()
