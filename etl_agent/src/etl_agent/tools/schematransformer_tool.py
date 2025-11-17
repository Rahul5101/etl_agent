from pydantic import BaseModel, Field
from typing import Type, List, Dict
import pandas as pd 
import json
from crewai.tools import BaseTool

class SchemaTransformerInput(BaseModel):
    """Input schema for transforming the schema of a dataset."""
    raw_json: str = Field(..., description="Raw json array from postgresfetchtool.")
    required_columns:List[str]= Field(..., description="ordered list of required bigquery columns.")
    timezone : str =Field("UTC",description="Timezone to normalize timestamps to (e.g., UTC).")


class SchemaTransformerTool(BaseTool):
    name : str = "Schema Transformer Tool"
    description : str = "convert raw postgres json data to bigquery-ready json rows and infer simple mapping. "
    args_schema : Type[BaseModel] = SchemaTransformerInput

    def _run(self, raw_json:str, required_columns:List[str], timezone:str="UTC") ->str:

        """
        - raw_json: JSON string list of rows returned by PostgresFetchTool
        - required_columns: list of columns expected by BigQuery table
        - returns: JSON string of transformed rows with only required columns and simple type normalization
                  Also returns a small metadata header encoded as {"rows": [...], "schema": {...}}
        """

        try:
            rows = json.loads(raw_json)
        except Exception as e:
            return f" Invalid JSON input: {str(e)}"

        try:
            df = pd.DataFrame(rows)
        except Exception as e:
            return f" Failed to load JSON into DataFrame: {str(e)}"

        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                
                dt_series = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S", errors="coerce")
                if dt_series.notna().any():
                    df[col] = dt_series.dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")
    
        # keeping only required columns aand adding missing columns as none
        transformed_rows = []
        for _, row in df.iterrows():
            out = {}
            for col in required_columns:
                val = row.get(col, None) if col in df.columns else None
                # turning numpy types and nan to None
                if pd.isna(val):
                    val = None
                if isinstance(val, (list, dict)):
                    val = json.dumps(val)
                out[col] = val
            transformed_rows.append(out)

        # doing schema mapping (col -> BQ type)
        schema_map = {}
        for col in required_columns:
            if col not in df.columns:
                schema_map[col] = "STRING"
                continue
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                schema_map[col] = "INT64"
            elif pd.api.types.is_float_dtype(dtype):
                schema_map[col] = "FLOAT64"
            elif pd.api.types.is_bool_dtype(dtype):
                schema_map[col] = "BOOL"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                schema_map[col] = "TIMESTAMP"
            else:
                sample = df[col].dropna().astype(str).head(10).tolist()
                is_json = True
                for s in sample:
                    try:
                        json.loads(s)
                    except Exception:
                        is_json = False
                        break
                schema_map[col] = "JSON" if is_json and len(sample) > 0 else "STRING"

        payload = {"rows": transformed_rows, "schema": schema_map}
        try:
            return json.dumps(payload, default=str)
        except Exception as e:
            return f" Failed to serialize transformed payload: {str(e)}"
        

rows = '[{"id": 1, "name": "Alice", "email": "alice@example.com"}, {"id": 2, "name": "rahul", "email": "rahul@example.com"}, {"id": 3, "name": "rahulG", "email": "rahulG@example.com"}, {"id": 4, "name": "boss", "email": "boss@example.com"}, {"id": 5, "name": "badmass", "email": "badmass@example.com"}, {"id": 6, "name": "kali", "email": "kali@example.com"}, {"id": 7, "name": "dogesh", "email": "dogesh@example.com"}, {"id": 8, "name": "billu", "email": "billu@example.com"}, {"id": 9, "name": "jivi", "email": "jivi@example.com"}, {"id": 10, "name": "muska", "email": "muska@example.com"}, {"id": 11, "name": "neha", "email": "neha@example.com"}, {"id": 12, "name": "priya", "email": "priya@example.com"}, {"id": 13, "name": "neha", "email": "neha@example.com"}, {"id": 14, "name": "ridhi", "email": "ridhi@example.com"}]'

obj = SchemaTransformerTool()
ans = obj._run(rows,['id', 'name', 'email'],"UTC")
print(ans)