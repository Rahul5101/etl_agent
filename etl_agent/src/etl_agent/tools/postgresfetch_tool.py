from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field
import psycopg2
import pandas as pd
import os
import json
from dotenv import load_dotenv
load_dotenv()



class postgresInput(BaseModel):
    """Input schema for fetecing the data from postgres."""
    query: str = Field(..., description="sql query to fetch the data from postgres")

class PostgresFetchTool(BaseTool):
    name: str = "Postgres Fetch Tool"
    description: str = (
        "Runs a SQL query against Postgres and returns results as JSON (records orient)."
    )
    args_schema: Type[BaseModel] = postgresInput

    def _run(self, query: str) -> str:
        """
        Connects to Postgres using env vars PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD.
        Returns: JSON string of rows (list of dicts).

        """
        host = os.getenv("PG_HOST")
        port = os.getenv("PG_PORT")
        database = os.getenv("PG_DATABASE")
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASSWORD")

        conn = psycopg2.connect(host =host,port=port ,database=database,user=user,password=password)
        df = pd.read_sql(query, conn)
        conn.close()

        try:
            return df.to_json(orient="records", date_format="iso")
        except Exception as e:
            try:
                rows = df.where(pd.notnull(df), None).to_dict(orient="records")
                return json.dumps(rows, default=str)
            except Exception as e2:
                return f"Failed to serialize result to JSON: {str(e2)}"



obj1 = PostgresFetchTool()
ans = obj1._run("SELECT * FROM customers")
print(ans)
