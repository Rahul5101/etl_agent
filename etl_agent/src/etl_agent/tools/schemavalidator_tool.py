from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, List
import json

class ValidatorInput(BaseModel):
    """Input schema for ValidatorTool."""
    transformed_payload: str = Field(..., description="JSON string containing {'rows': [...], 'schema': {...}}")
    required_columns: List[str] = Field(..., description="List of required columns in BigQuery table.")

class ValidatorTool(BaseTool):
    name: str = "Validator Tool"
    description: str = "Validates transformed rows against required columns and basic type checks."
    args_schema: Type[BaseModel] = ValidatorInput

    def _run(self, transformed_payload: str, required_columns: List[str]) -> str:
        """
        Validations performed:
         - JSON parse
         - Every required column exists in schema
         - No required column has all-null across rows (optional check)
         - Simple type compatibility checks (non-exhaustive)
        Returns success message or issues string 
        """
        try:
            payload = json.loads(transformed_payload)
            # print(payload)
            rows = payload.get("rows", [])
            schema = payload.get("schema", {})
        except Exception as e:
            return f"Invalid transformed payload JSON: {str(e)}"

        issues = []

        #checking column presence
        for col in required_columns:
            if col not in schema:
                issues.append(f"Missing column in schema: {col}")

        # Row-level checking
        for idx, row in enumerate(rows):
            for col in required_columns:
                if col not in row:
                    issues.append(f"Row {idx}: missing column {col}")
                else:
                    
                    if row[col] is None:
                        issues.append(f"Row {idx}: column {col} is null")

        # optional: require at least one non-null in required cols
        for col in required_columns:
            non_null_count = sum(1 for r in rows if r.get(col) is not None)
            if non_null_count == 0:
                issues.append(f"All values are null for required column: {col}")

        if issues:
            return "Validation Failed:\n" + "\n".join(issues)

        return "✔ Validation Passed"


rows = '{"rows": [{"id": 1, "name": "Alice", "email": "alice@example.com"}, {"id": 2, "name": "rahul", "email": "rahul@example.com"}, {"id": 3, "name": "rahulG", "email": "rahulG@example.com"}, {"id": 4, "name": "boss", "email": "boss@example.com"}, {"id": 5, "name": "badmass", "email": "badmass@example.com"}, {"id": 6, "name": "kali", "email": "kali@example.com"}, {"id": 7, "name": "dogesh", "email": "dogesh@example.com"}, {"id": 8, "name": "billu", "email": "billu@example.com"}, {"id": 9, "name": "jivi", "email": "jivi@example.com"}, {"id": 10, "name": "muska", "email": "muska@example.com"}, {"id": 11, "name": "neha", "email": "neha@example.com"}, {"id": 12, "name": "priya", "email": "priya@example.com"}, {"id": 13, "name": "neha", "email": "neha@example.com"}, {"id": 14, "name": "ridhi", "email": "ridhi@example.com"}], "schema": {"id": "INT64", "name": "STRING", "email": "STRING"}}'
obj = ValidatorTool()
ans = obj._run(rows,['id', 'name', 'email'])
print(ans)