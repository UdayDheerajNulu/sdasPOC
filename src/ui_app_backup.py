import os
import json
import traceback
import streamlit as st
from dotenv import load_dotenv

# Local imports
from groq_langchain_analyzer import GroqLangChainTableAnalyzer
from db_visualizer import DatabaseVisualizer

load_dotenv()

st.set_page_config(page_title="ChatGroq DB Analyzer", layout="wide")

# Simple styles for badges
st.markdown(
    """
    <style>
    .chip {display:inline-block; padding:4px 8px; margin:2px; border-radius:12px; font-size:12px;}
    .chip-primary {background-color:#e6f4ea; color:#137333; border:1px solid #a8d5b9;}
    .chip-secondary {background-color:#e8f0fe; color:#174ea6; border:1px solid #aecbfa;}
    .faded {opacity: 0.15; transition: opacity 0.25s ease-in-out;}
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
    groq_env = os.getenv("GROQ_API_KEY", "")
    api_key = st.text_input("GROQ_API_KEY", value=groq_env, type="password")
    st.write("")
    run_btn = st.button("Run Analysis", type="primary")

if run_btn:
    if not db_path or not os.path.exists(db_path):
        st.error(f"Database not found at: {db_path}")
        st.stop()
    if not api_key:
        st.warning("Set GROQ_API_KEY to continue.")
        st.stop()

    # Pass key to environment for langchain_groq
    os.environ["GROQ_API_KEY"] = api_key

    with st.status("Running analysis...", expanded=True) as status:
        try:
            st.write("Initializing analyzer")
            analyzer = GroqLangChainTableAnalyzer(db_path)

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

    # Create interactive relationship diagram using Cytoscape
    st.subheader("Table Relationship Diagram")
    
    # Add diagram help in an expander
    with st.expander("?? Diagram Help", expanded=False):
        st.markdown("""
        #### Table Information
        - ?? Table names are shown at the top of each box
        - ?? Primary keys are listed with bullet points
        - Tables with foreign keys have a green border
        - Only tables with relationships are shown

        #### Relationship Information
        - ?? Foreign key relationships are shown as arrows
        - Arrow direction: Child table ? Parent table
        - Hover over arrows to see the exact columns involved

        #### Navigation
        - ??? Click a table to zoom to it
        - ?? Double-click anywhere to reset the view
        - ??? Scroll to zoom in/out
        - ? Drag to pan and move around
        """)

    # Initialize visualizer and create diagram
    visualizer = DatabaseVisualizer(db_path)
    
    # Generate visualization data
    data = visualizer.generate_cytoscape_elements()
    styles = visualizer.generate_cytoscape_style()
    
    # Generate HTML for Cytoscape visualization
    cytoscape_html = f"""
    <div style="width:100%; height:500px; border:1px solid #ddd; border-radius:5px; margin:10px 0;">
        <div id="cy" style="width:100%; height:100%;"></div>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.28.1/cytoscape.min.js"></script>
        <script>
            var cy = cytoscape({{
                container: document.getElementById('cy'),
                elements: {json.dumps(data)},
                style: {json.dumps(styles)},
                layout: {{
                    name: 'cose',
                    idealEdgeLength: 150,
                    nodeOverlap: 20,
                    refresh: 20,
                    fit: true,
                    padding: 50,
                    randomize: false,
                    componentSpacing: 150,
                    nodeRepulsion: 500000,
                    edgeElasticity: 100,
                    nestingFactor: 5,
                    gravity: 80,
                    numIter: 1500,
                    initialTemp: 200,
                    coolingFactor: 0.95,
                    minTemp: 1.0
                }},
                wheelSensitivity: 0.2,
                minZoom: 0.2,
                maxZoom: 3
            }});

            // Add zoom controls
            cy.on('tap', 'node', function(evt) {{
                var node = evt.target;
                cy.animate({{
                    fit: {{
                        eles: node,
                        padding: 50
                    }},
                    duration: 500
                }});
            }});

            // Add double-click to reset view
            var tappedBefore;
            var tappedTimeout;
            cy.on('tap', function(event) {{
                var tappedNow = event.target;
                if (tappedTimeout && tappedBefore) {{
                    clearTimeout(tappedTimeout);
                }}
                if (tappedBefore === tappedNow) {{
                    cy.animate({{
                        fit: {{
                            padding: 50
                        }},
                        duration: 500
                    }});
                    tappedBefore = null;
                }} else {{
                    tappedTimeout = setTimeout(function(){{ tappedBefore = null; }}, 300);
                    tappedBefore = tappedNow;
                }}
            }});

            // Highlight connected elements on hover
            cy.on('mouseover', 'node', function(e) {{
                var node = e.target;
                var neighborhood = node.neighborhood().add(node);
                cy.elements().not(neighborhood).addClass('faded');
                neighborhood.removeClass('faded');
            }});
            
            cy.on('mouseout', 'node', function(e) {{
                cy.elements().removeClass('faded');
            }});
        </script>
    </div>
    """
    
    # Show diagram
    import streamlit.components.v1 as components
    components.html(cytoscape_html, height=520)

    # Add relationship stats
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Related Tables", len(data["nodes"]))
    with col2:
        st.metric("Total Relationships", len(data["edges"]))

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
                with st.expander(f"{t['table_name']} — Priority {priority} ({priority_desc})", expanded=False):
                    # Archival columns colored
                    prim = t.get("primary_archival_columns", []) or []
                    sec = t.get("secondary_archival_columns", []) or []
                    if prim or sec:
                        st.markdown("**Archival Columns**")
                        chips_html = "".join([f"<span class='chip chip-primary'>{c}</span>" for c in prim])
                        chips_html += "".join([f"<span class='chip chip-secondary'>{c}</span>" for c in sec])
                        st.markdown(chips_html, unsafe_allow_html=True)

                    # Relationships under each table
                    rel = report.get("table_analysis", {}).get(t["table_name"], {}).get("relationship_info", {})
                    if rel:
                        st.markdown("**Relationships**")
                        st.json(rel)

                    # Strategy and reasoning
                    if t.get("archival_strategy"):
                        st.caption(f"Strategy: {t['archival_strategy']}")
                    if t.get("priority_reasoning"):
                        with st.expander("Priority reasoning"):
                            st.write(t["priority_reasoning"]) 
                    if t.get("archival_reasoning"):
                        with st.expander("Archival reasoning"):
                            st.write(t["archival_reasoning"]) 

    st.divider()
    st.caption(f"Completed at {report.get('analysis_timestamp', '')}")

else:
    st.info("Configure the DB path and API key in the sidebar, then click Run Analysis.")
