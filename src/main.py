import pymysql
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans

# DB connection setup
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='dheeraJ@0502',
    database='sample_archival'
)

# Step 1: Get all table names
with conn.cursor() as cursor:
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]

# Step 2: Get table definitions as strings
def get_table_schema(table):
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table}")
        schema = cursor.fetchall()
    schema_str = f"{table}: " + ", ".join(f"{col[0]} {col[1]}" for col in schema)
    return schema_str

table_descriptions = [get_table_schema(tbl) for tbl in tables]

# Step 3: Embed definition descriptions
model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = model.encode(table_descriptions)

# Step 4: Cluster tables (change n_clusters as needed)
kmeans = KMeans(n_clusters=2, random_state=42).fit(embeddings)

# Step 5: Show results
for i, table in enumerate(tables):
    print(f"Table: {table} → Cluster: {kmeans.labels_[i]}")

# Optional: Identify datetime columns (archival candidate)
def find_datetime_columns(table):
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table}")
        schema = cursor.fetchall()
    return [col[0] for col in schema if 'datetime' in col[1]]

print("\nSuggested archival columns:")
for table in tables:
    dt_cols = find_datetime_columns(table)
    print(f"{table}: {dt_cols}")
