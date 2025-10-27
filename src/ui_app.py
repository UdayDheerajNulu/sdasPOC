import os
import json
import traceback
import streamlit as st
from dotenv import load_dotenv

# Local import
from groq_langchain_analyzer import GroqLangChainTableAnalyzer
import sqlite3

load_dotenv()

st.set_page_config(page_title="ChatGroq DB Analyzer", layout="wide")

# Simple styles for badges
st.markdown(
	"""
	<style>
	.chip {display:inline-block; padding:4px 8px; margin:2px; border-radius:12px; font-size:12px;}
	.chip-primary {background-color:#e6f4ea; color:#137333; border:1px solid #a8d5b9;}
	.chip-secondary {background-color:#e8f0fe; color:#174ea6; border:1px solid #aecbfa;}
	</style>
	""",
	unsafe_allow_html=True,
)

st.title("Database Archival Analysis")
st.caption("Categorize tables, detect archival columns, and assign purge priorities")

with st.sidebar:
	st.header("Configuration")
	default_db = "table_group_archival_demo.sqlite"
	db_path = st.text_input("SQLite DB path", value=default_db, help="Path to a .sqlite/.db file")
	
	mock_mode = st.checkbox("Mock Mode (No API key needed)", value=False, 
						   help="Run with mock data without making LLM API calls")
	
	if not mock_mode:
		groq_env = os.getenv("GROQ_API_KEY", "")
		api_key = st.text_input("GROQ_API_KEY", value=groq_env, type="password")
	else:
		api_key = "mock"  # Placeholder value when in mock mode
		st.info("Running in mock mode - using sample data without LLM calls")
	
	st.write("")
	run_btn = st.button("Run Analysis", type="primary")

if run_btn:
	if not db_path or not os.path.exists(db_path):
		st.error(f"Database not found at: {db_path}")
		st.stop()
	
	if not mock_mode and not api_key:
		st.warning("Set GROQ_API_KEY to continue or use Mock Mode")
		st.stop()

	if not mock_mode:
		# Pass key to environment for langchain_groq
		os.environ["GROQ_API_KEY"] = api_key

	with st.status("Running analysis...", expanded=True) as status:
		try:
			st.write("Initializing analyzer" + (" (Mock Mode)" if mock_mode else ""))
			analyzer = GroqLangChainTableAnalyzer(db_path, mock_mode=mock_mode)

			st.write("Creating comprehensive report")
			report = analyzer.create_comprehensive_report()

			if "error" in report:
				st.error(report["error"])
				status.update(label="Analysis failed", state="error")
				st.stop()

			status.update(label="Analysis completed", state="complete")
		except Exception as e:
			status.update(label="Analysis failed", state="error")
			st.exception(e)
			st.text(traceback.format_exc())
			st.stop()

	# Build relationship graph (Graphviz) and show at top
	st.subheader("Table Relationship Diagram")
	table_analysis = report.get("table_analysis", {})
	# Build directed edges from relationship_info
	edges = []
	nodes = set()
	node_labels = {}
	# We'll fetch actual table columns from the SQLite file to ensure accurate column lists
	conn = None
	try:
		conn = sqlite3.connect(db_path)
		cursor = conn.cursor()

		for table_name, info in table_analysis.items():
			rel = info.get("relationship_info", {}) or {}
			col_lines = []
			try:
				cursor.execute(f"PRAGMA table_info({table_name})")
				cols = cursor.fetchall()  # cid, name, type, notnull, dflt_value, pk
			except Exception:
				cols = []

			pk_set = set()
			fk_set = set()
			# Collect pk columns from PRAGMA
			for c in cols:
				if len(c) >= 6 and c[5]:
					pk_set.add(c[1])

			# Collect foreign key child columns from relationship info
			for fk in rel.get("foreign_keys", []):
				child_col = fk.get("child_column")
				if child_col:
					fk_set.add(child_col)

			# Only show primary keys and foreign keys per user's request
			if cols:
				for c in cols:
					col_name = c[1]
					if col_name in pk_set:
						col_lines.append(f'<FONT COLOR="green"><B>{col_name}</B></FONT>')
					elif col_name in fk_set:
						col_lines.append(f'<FONT COLOR="blue"><I>{col_name}</I></FONT>')
				if not col_lines:
					col_lines.append('<I><FONT COLOR="gray">(no keys)</FONT></I>')
			else:
				# Fallback: use analyzer-provided primary_keys and relationship info
				pk_list = set(info.get("primary_keys", []) or [])
				fk_list = set(fk.get("child_column") for fk in rel.get("foreign_keys", []) if fk.get("child_column"))
				for name in sorted(pk_list):
					col_lines.append(f'<FONT COLOR="green"><B>{name}</B></FONT>')
				for name in sorted(fk_list - pk_list):
					col_lines.append(f'<FONT COLOR="blue"><I>{name}</I></FONT>')
				if not col_lines:
					col_lines.append('<I><FONT COLOR="gray">(no keys)</FONT></I>')

			label = '<' + f'<B>{table_name}</B><BR/>' + '<BR/>'.join(col_lines) + '>'
			node_labels[table_name] = label
			# Build edges and nodes
			for fk in rel.get("foreign_keys", []):
				parent = fk.get("parent_table")
				child = table_name
				if parent and child:
					edges.append((child, parent))
					nodes.add(child)
					nodes.add(parent)
			for ref in rel.get("referenced_by", []):
				child = ref.get("child_table")
				parent = table_name
				if parent and child:
					nodes.add(child)
					nodes.add(parent)

	finally:
		if conn:
			conn.close()
		for fk in rel.get("foreign_keys", []):
			parent = fk.get("parent_table")
			child = table_name
			if parent and child:
				edges.append((child, parent))
				nodes.add(child)
				nodes.add(parent)
		for ref in rel.get("referenced_by", []):
			child = ref.get("child_table")
			parent = table_name
			if parent and child:
				nodes.add(child)
				nodes.add(parent)

	# Render Graphviz
	try:
		from graphviz import Digraph
		g = Digraph("tables", format="svg")
		g.attr(rankdir="LR", fontsize="10")
		for n in sorted(nodes):
			g.node(n, label=node_labels.get(n, n), shape="box")
		for c, p in edges:
			g.edge(c, p, label="FK")
		st.graphviz_chart(g.source, use_container_width=True)
	except Exception:
		st.info("Install 'graphviz' Python package and system binaries to view the relationship diagram.")

	st.divider()

	# Metrics
	col1, col2 = st.columns(2)
	with col1:
		st.metric("Total Tables", report.get("total_tables", 0))
	with col2:
		st.metric("Total Groups", report.get("total_groups", 0))

	# Grouped by priority (with nested table expanders + relationships)
	st.subheader("Grouped by Priority")
	grouped = report.get("grouped_by_priority", {})
	for group_name, tables in grouped.items():
		with st.expander(f"Group: {group_name} ({len(tables)} tables)", expanded=False):
			for t in tables:
				priority = t.get("intra_group_priority", 2)
				priority_desc = {1: "HIGH", 2: "MEDIUM", 3: "LOW"}.get(priority, "?")
				with st.expander(f"{t['table_name']} - Priority {priority} ({priority_desc})", expanded=False):
					# Archival columns colored
					prim = t.get("primary_archival_columns", []) or []
					sec = t.get("secondary_archival_columns", []) or []
					if prim or sec:
						st.markdown("**Archival Columns**")
						chips_html = "".join([f"<span class='chip chip-primary'>{c}</span>" for c in prim])
						chips_html += "".join([f"<span class='chip chip-secondary'>{c}</span>" for c in sec])
						st.markdown(chips_html, unsafe_allow_html=True)

					# Relationships under each table
					rel = table_analysis.get(t["table_name"], {}).get("relationship_info", {})
					if rel:
						st.markdown("**Relationships**")
						st.json(rel)

					# RCC classification (from LLM)
					rcc = t.get("rcc_classification")
					if rcc:
						st.markdown("**Retention Class (RCC)**")
						st.markdown(f"- Assigned RCC: {rcc.get('assigned_rcc', 'N/A')}")
						if rcc.get('reasoning'):
							with st.expander("RCC reasoning"):
								st.write(rcc.get('reasoning'))

					# Retention lookup columns (from LLM)
					ret_lookup = t.get("retention_lookup") or t.get("retention_analysis") or {}
					if ret_lookup:
						cols = ret_lookup.get("retention_lookup_columns") or ret_lookup.get("retention_lookup_column") or []
						if cols:
							st.markdown("**Retention Lookup Columns**")
							chips_html = "".join([f"<span class='chip chip-primary'>{c}</span>" for c in cols])
							st.markdown(chips_html, unsafe_allow_html=True)
						reason = ret_lookup.get("reasoning") or t.get("retention_reasoning")
						if reason:
							with st.expander("Retention reasoning"):
								st.write(reason)

					# Strategy and reasoning
					if t.get("retention_strategy"):
						st.caption(f"Strategy: {t['retention_strategy']}")
					if t.get("priority_reasoning"):
						with st.expander("Priority reasoning"):
							st.write(t["priority_reasoning"]) 
					if t.get("retention_reasoning"):
						with st.expander("Retention reasoning"):
							st.write(t["retention_reasoning"]) 

	st.divider()
	st.caption(f"Completed at {report.get('analysis_timestamp', '')}")

else:
	st.info("Configure the DB path and API key in the sidebar, then click Run Analysis.")