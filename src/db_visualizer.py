import json
from typing import Dict, List
import sqlite3

class DatabaseVisualizer:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_table_info(self) -> Dict:
        """Get detailed table information including primary and foreign keys"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        table_info = {}
        
        for table_name in tables:
            # Get column information
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Get foreign key information
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            
            # Get indices to identify primary keys
            cursor.execute(f"PRAGMA index_list({table_name})")
            indices = cursor.fetchall()
            
            primary_keys = []
            for idx in indices:
                if idx[2] == 1:  # is_primary
                    cursor.execute(f"PRAGMA index_info({idx[1]})")
                    pk_cols = cursor.fetchall()
                    primary_keys.extend(col[2] for col in pk_cols)
            
            table_info[table_name] = {
                "columns": [
                    {
                        "name": col[1],
                        "type": col[2],
                        "is_primary": col[1] in primary_keys,
                        "is_nullable": not col[3]
                    }
                    for col in columns
                ],
                "foreign_keys": [
                    {
                        "from_column": fk[3],
                        "to_table": fk[2],
                        "to_column": fk[4]
                    }
                    for fk in foreign_keys
                ],
                "primary_keys": primary_keys
            }
        
        conn.close()
        return table_info

    def generate_cytoscape_elements(self) -> Dict:
        """Generate elements for Cytoscape visualization with only related tables"""
        table_info = self.get_table_info()
        elements = {"nodes": [], "edges": []}
        
        # Track tables with relationships
        related_tables = set()
        
        # First pass: identify tables with relationships
        for table_name, info in table_info.items():
            if info["foreign_keys"]:
                related_tables.add(table_name)
                for fk in info["foreign_keys"]:
                    related_tables.add(fk["to_table"])
        
        # Add nodes (only for related tables)
        for table_name, info in table_info.items():
            if table_name in related_tables:
                # Create detailed label with table name and primary keys
                pk_cols = [col["name"] for col in info["columns"] if col["is_primary"]]
                
                # Format label with clear primary key section
                label_parts = [
                    f"📋 {table_name}",  # Table name with icon
                    "🔑 Primary Keys:",   # Primary key section header
                    *[f"  • {pk}" for pk in pk_cols]  # Bulleted list of primary keys
                ]
                
                label = "\\n".join(label_parts)
                
                elements["nodes"].append({
                    "data": {
                        "id": table_name,
                        "label": label,
                        "primary_keys": pk_cols,
                        "has_foreign_keys": bool(info["foreign_keys"])
                    }
                })
        
        # Add edges (relationships)
        for table_name, info in table_info.items():
            if table_name in related_tables:
                for fk in info["foreign_keys"]:
                    edge_id = f"{table_name}_{fk['to_table']}_{fk['from_column']}"
                    elements["edges"].append({
                        "data": {
                            "id": edge_id,
                            "source": table_name,
                            "target": fk["to_table"],
                            "label": f"🔗 {fk['from_column']} → {fk['to_column']}"
                        }
                    })
        
        return elements

    def generate_cytoscape_style(self) -> List[Dict]:
        """Generate Cytoscape style configuration with enhanced visibility"""
        return [
            {
                "selector": "node",
                "style": {
                    "content": "data(label)",
                    "text-valign": "center",
                    "text-halign": "center",
                    "shape": "round-rectangle",
                    "background-color": "#ffffff",
                    "border-width": 2,
                    "border-color": "#0366d6",
                    "width": "label",
                    "height": "label",
                    "padding": "15px",
                    "font-size": "14px",
                    "text-wrap": "wrap",
                    "text-max-width": "200px",
                    "font-family": "system-ui",
                    "color": "#24292e",
                    "text-margin-y": "5px"
                }
            },
            {
                "selector": "edge",
                "style": {
                    "content": "data(label)",
                    "curve-style": "bezier",
                    "target-arrow-shape": "vee",
                    "line-color": "#0366d6",
                    "target-arrow-color": "#0366d6",
                    "font-size": "12px",
                    "text-rotation": "autorotate",
                    "text-margin-y": "-10px",
                    "width": 2,
                    "font-weight": "bold",
                    "text-outline-color": "#ffffff",
                    "text-outline-width": 2,
                    "line-style": "solid"
                }
            },
            {
                "selector": "node[?has_foreign_keys]",
                "style": {
                    "border-color": "#28a745",
                    "border-width": 3
                }
            }
        ]

    def export_visualization_data(self, output_file: str = "db_visualization.json"):
        """Export visualization data to a JSON file"""
        data = {
            "elements": self.generate_cytoscape_elements(),
            "style": self.generate_cytoscape_style()
        }
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        return data