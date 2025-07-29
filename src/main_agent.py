from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_groq import ChatGroq
from langgraph.prebuilt import create_react_agent
from urllib.parse import quote
from dotenv import load_dotenv
import os

load_dotenv()

password = os.getenv("DB_PASSWORD")
encoded_password = quote(password)

# 1. Connect to MySQL database
db = SQLDatabase.from_uri(f"mysql+pymysql://root:{encoded_password}@localhost:3306/sample_db")

# 2. Initialize Groq chat model
llm = ChatGroq(temperature=0, model_name="llama3-8b-8192")

# 3. Create SQL toolkit for database interaction
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools()

system_prompt = """
You are an expert database analyst. Given a list of table names and their schemas, classify each table into one of the following groups:
archive, config, dataretention, invoice, keylog, mediadestruction, metrics, scorecard, system versioning.

For each table, identify possible archival columns such as createddate, inserteddate, isactive, validfrom, loaddate, validto, or similar columns that indicate archival or retention information.

Return the results in JSON format with table name, group, and archival columns.
"""
agent = create_react_agent(
    llm,
    tools,
    prompt=system_prompt,
)

question = "List all tables and their schemas in the database."

for step in agent.stream(
    {"messages": [{"role": "user", "content": question}]},
    stream_mode="values",
):
    step["messages"][-1].pretty_print()