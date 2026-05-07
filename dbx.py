"""Databricks connection wrapper.

Auto-detects auth mode:
  - If env var DATABRICKS_TOKEN is set -> PAT auth (used in CI)
  - Otherwise -> OAuth browser flow (used locally)

Override hostname/path with DATABRICKS_HOST and DATABRICKS_HTTP_PATH if needed.
"""

import os

from databricks import sql
import pandas as pd

DEFAULT_HOST = "bolt-incentives.cloud.databricks.com"
DEFAULT_HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"


class DBX:
    def __init__(self, http_path: str | None = None):
        host = os.environ.get("DATABRICKS_HOST", DEFAULT_HOST)
        path = http_path or os.environ.get("DATABRICKS_HTTP_PATH", DEFAULT_HTTP_PATH)
        token = os.environ.get("DATABRICKS_TOKEN")

        if token:
            self.conn = sql.connect(
                server_hostname=host,
                http_path=path,
                access_token=token,
            )
            self.auth_mode = "pat"
        else:
            self.conn = sql.connect(
                server_hostname=host,
                http_path=path,
                auth_type="databricks-oauth",
            )
            self.auth_mode = "oauth"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query(self, q: str) -> pd.DataFrame:
        with self.conn.cursor() as cur:
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    print("Testing connection...")
    with DBX() as dbx:
        print(f"Auth mode: {dbx.auth_mode}")
        df = dbx.query("SELECT 1 AS test")
        print("OK" if len(df) else "FAIL")
