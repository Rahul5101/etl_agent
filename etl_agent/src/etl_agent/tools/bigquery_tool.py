from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import io
from dotenv import load_dotenv

load_dotenv()
credentials_path = os.getenv("CREDENTIALS_PATH")

class BigQueryLoadInput(BaseModel):
    data: str = Field(..., description="JSON formatted data.")
    table_id: str = Field(..., description="BigQuery table in format: project.dataset.table")
    credentials_path: str = Field(..., description="Path to service account JSON key")

class BigQueryLoadTool(BaseTool):
    name: str = "BigQueryLoadTool"
    description: str = "Loads JSON data into BigQuery using Load Job (free tier compatible)."
    args_schema: Type[BaseModel] = BigQueryLoadInput

    def _run(self, data: str, table_id: str,credentials_path:str) -> str:
        # Validate JSON
        try:
            # rows= json.loads(data).get("rows", [])
            rows = json.loads(data)
        except Exception as e:
            return f"Invalid JSON provided: {e}"

        # Load credentials from JSON key file
        
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
        except Exception as e:
            return f"Credential error: {str(e)}"

        # Initialize BigQuery client
        try:
            client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        except Exception as e:
            return f"BigQuery client initialization error: {str(e)}"

        # Extract project, dataset, table
        try:
            project, dataset, table = table_id.split(".")
        except:
            return "Invalid table_id format — expected project.dataset.table"

        dataset_ref = client.dataset(dataset)
        table_ref = dataset_ref.table(table)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # Convert rows → NDJSON lines
        ndjson_data = "\n".join([json.dumps(row) for row in rows])
        json_bytes = ndjson_data.encode("utf-8")

        # Load Job (works on free tier)
        try:
            load_job = client.load_table_from_file(
                file_obj=io.BytesIO(json_bytes),
                destination=table_ref,
                job_config=job_config,
            )
            load_job.result()  # Wait for completion
        except Exception as e:
            return f"BigQuery Load Error: {e}"

        return f"Loaded {len(rows)} rows into {table_id} using load job."


ans = '{"rows": [{"id": 1, "name": "Alice", "email": "alice@example.com"}, {"id": 2, "name": "rahul", "email": "rahul@example.com"}, {"id": 3, "name": "rahulG", "email": "rahulG@example.com"}, {"id": 4, "name": "boss", "email": "boss@example.com"}, {"id": 5, "name": "badmass", "email": "badmass@example.com"}, {"id": 6, "name": "kali", "email": "kali@example.com"}, {"id": 7, "name": "dogesh", "email": "dogesh@example.com"}, {"id": 8, "name": "billu", "email": "billu@example.com"}, {"id": 9, "name": "jivi", "email": "jivi@example.com"}, {"id": 10, "name": "muska", "email": "muska@example.com"}, {"id": 11, "name": "neha", "email": "neha@example.com"}, {"id": 12, "name": "priya", "email": "priya@example.com"}, {"id": 13, "name": "neha", "email": "neha@example.com"}, {"id": 14, "name": "ridhi", "email": "ridhi@example.com"}], "schema": {"id": "INT64", "name": "STRING", "email": "STRING"}}'
obj = BigQueryLoadTool()
result = obj._run(ans, "postgres-bigquery-agent.cusitomerDB.friends_detail",credentials_path)
print(result)  # replace with actual table_id