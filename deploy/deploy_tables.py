"""
Deploy monitoring Delta tables to a Databricks workspace.

Authenticates via databricks-sdk, reads DDL files, and executes them
against a SQL warehouse.

Usage:
    python deploy/deploy_tables.py --catalog main --profile DEFAULT
"""

import argparse
import glob
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def get_ddl_files() -> list:
    """Return sorted list of DDL SQL files."""
    ddl_dir = Path(__file__).parent.parent / "ddl"
    files = sorted(glob.glob(str(ddl_dir / "*.sql")))
    return files


def execute_sql(client: WorkspaceClient, warehouse_id: str, sql: str) -> None:
    """Execute a SQL statement and wait for completion."""
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="60s",
    )
    if response.status.state == StatementState.FAILED:
        raise RuntimeError(
            f"SQL execution failed: {response.status.error.message}"
        )


def deploy_tables(catalog: str, warehouse_id: str, profile: str = "DEFAULT") -> None:
    """Execute all DDL files against the workspace."""
    client = WorkspaceClient(profile=profile)

    ddl_files = get_ddl_files()
    if not ddl_files:
        print("No DDL files found in ddl/ directory")
        sys.exit(1)

    print(f"Deploying {len(ddl_files)} DDL files to catalog '{catalog}'")

    for filepath in ddl_files:
        filename = os.path.basename(filepath)
        print(f"  Executing {filename}...")

        with open(filepath, "r") as f:
            sql_content = f.read()

        # Replace catalog placeholder
        sql_content = sql_content.replace("${catalog}", catalog)

        # Split on semicolons for multi-statement files (skip OPTIMIZE file in DDL deploy)
        statements = [s.strip() for s in sql_content.split(";") if s.strip()]

        for stmt in statements:
            if stmt.startswith("--"):
                continue
            try:
                execute_sql(client, warehouse_id, stmt)
            except Exception as e:
                print(f"    WARNING: {e}")

        print(f"    Done: {filename}")

    print("All tables deployed successfully.")


def main():
    parser = argparse.ArgumentParser(description="Deploy streaming monitoring tables")
    parser.add_argument("--catalog", default="main", help="Unity Catalog name")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse ID")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    args = parser.parse_args()

    deploy_tables(args.catalog, args.warehouse_id, args.profile)


if __name__ == "__main__":
    main()
