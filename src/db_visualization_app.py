import streamlit as st
import streamlit.components.v1 as components
import json
from db_visualizer import DatabaseVisualizer

def render_cytoscape_html(elements, styles):
    """Render Cytoscape visualization using HTML component"""
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.28.1/cytoscape.min.js"></script>
        <style>
            #cy {{
                width: 100%;
                height: 800px;
                position: relative;
                border: 1px solid #ccc;
                border-radius: 5px;
            }}
        </style>
    </head>
    <body>
        <div id="cy"></div>
        <script>
            var cy = cytoscape({{
                container: document.getElementById('cy'),
                elements: {json.dumps(elements)},
                style: {json.dumps(styles)},
                layout: {{
                    name: 'cose',
                    idealEdgeLength: 100,
                    nodeOverlap: 20,
                    refresh: 20,
                    fit: true,
                    padding: 30,
                    randomize: false,
                    componentSpacing: 100,
                    nodeRepulsion: 400000,
                    edgeElasticity: 100,
                    nestingFactor: 5,
                    gravity: 80,
                    numIter: 1000,
                    initialTemp: 200,
                    coolingFactor: 0.95,
                    minTemp: 1.0
                }},
                wheelSensitivity: 0.2
            }});

            // Add zoom controls
            cy.on('tap', 'node', function(evt){{
                var node = evt.target;
                cy.fit(node, 50);
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
                    tappedNow.trigger('doubleTap');
                    tappedBefore = null;
                }} else {{
                    tappedTimeout = setTimeout(function(){{ tappedBefore = null; }}, 300);
                    tappedBefore = tappedNow;
                }}
            }});

            cy.on('doubleTap', function(){{
                cy.fit();
                cy.center();
            }});
        </script>
    </body>
    </html>
    """
    return html

def main():
    st.title("Database Schema Relationship Visualization")
    
    st.markdown("""
    ### Interactive Database Relationship Diagram
    
    #### Table Information
    - 📋 Table names are shown at the top of each box
    - 🔑 Primary keys are listed with bullet points
    - Tables with foreign keys have a green border
    - Tables without relationships are not shown
    
    #### Relationship Information
    - 🔗 Foreign key relationships are shown as arrows
    - Arrow direction: Child table → Parent table
    - Hover over arrows to see the exact columns involved
    
    #### Navigation
    - 🖱️ Click a table to zoom to it
    - 👆 Double-click anywhere to reset the view
    - 🖲️ Scroll to zoom in/out
    - ✋ Drag to pan and move around
    """)

    # Initialize database visualizer
    db_path = "table_group_archival_demo.sqlite"  # Update with your database path
    visualizer = DatabaseVisualizer(db_path)
    
    # Generate visualization data
    data = visualizer.generate_cytoscape_elements()
    styles = visualizer.generate_cytoscape_style()
    
    # Render visualization
    html = render_cytoscape_html(data, styles)
    components.html(html, height=850)

    # Display statistics
    st.subheader("Database Statistics")
    stats = {
        "Total Tables": len(data["nodes"]),
        "Total Relationships": len(data["edges"])
    }
    st.json(stats)

if __name__ == "__main__":
    main()