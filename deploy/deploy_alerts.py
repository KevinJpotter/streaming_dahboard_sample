"""
Deploy SQL alerts to a Databricks workspace.

Creates DBSQL alerts from alert_queries.sql, configured per alert_config.py.

Usage:
    python deploy/deploy_alerts.py --warehouse-id <id> --catalog main --environment prod
"""

import argparse
import re
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

sys.path.insert(0, str(Path(__file__).parent.parent))
from alerts.alert_config import ALERTS


def load_alert_queries(catalog: str, environment: str) -> dict:
    """Parse alert_queries.sql into named query blocks."""
    sql_path = Path(__file__).parent.parent / "alerts" / "alert_queries.sql"
    content = sql_path.read_text()
    content = content.replace("${catalog}", catalog)
    content = content.replace("${environment}", environment)

    queries = {}
    # Split on "-- alert_" markers
    blocks = re.split(r"-- (alert_\w+)", content)
    for i in range(1, len(blocks), 2):
        name = blocks[i].strip()
        sql = blocks[i + 1].strip().rstrip(";")
        if sql:
            queries[name] = sql

    return queries


def deploy_alerts(
    warehouse_id: str,
    catalog: str,
    environment: str,
    profile: str = "DEFAULT",
) -> None:
    """Create SQL alerts in the workspace."""
    client = WorkspaceClient(profile=profile)
    queries = load_alert_queries(catalog, environment)

    print(f"Deploying {len(ALERTS)} alerts...")

    for alert_def in ALERTS:
        query_name = alert_def["query_name"]
        sql = queries.get(query_name)

        if not sql:
            print(f"  WARNING: No query found for {query_name}, skipping")
            continue

        alert_name = alert_def["display_name"]
        print(f"  Creating alert: {alert_name}")

        try:
            # First create the query
            query_resp = client.api_client.do(
                "POST",
                "/api/2.0/sql/queries",
                body={
                    "name": f"[Monitor] {alert_name}",
                    "warehouse_id": warehouse_id,
                    "query": sql,
                    "description": alert_def["description"],
                },
            )
            query_id = query_resp.get("id")

            # Then create the alert referencing the query
            alert_body = {
                "name": alert_name,
                "query_id": query_id,
                "condition": {
                    "op": alert_def["condition"]["op"],
                    "operand": alert_def["condition"]["operand"],
                    "threshold": {
                        "value": {
                            "double_value": alert_def["condition"]["threshold"]
                        }
                    },
                },
                "rearm": alert_def["rearm_seconds"],
                "custom_body": (
                    f"Alert: {alert_name}\n"
                    f"Severity: {alert_def['severity']}\n"
                    f"Environment: {environment}\n"
                    f"Description: {alert_def['description']}"
                ),
            }

            client.api_client.do("POST", "/api/2.0/sql/alerts", body=alert_body)
            print(f"    Created: {alert_name}")

        except Exception as e:
            print(f"    ERROR creating {alert_name}: {e}")

    print("Alert deployment complete.")


def main():
    parser = argparse.ArgumentParser(description="Deploy streaming monitoring alerts")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse ID")
    parser.add_argument("--catalog", default="main", help="Unity Catalog name")
    parser.add_argument("--environment", default="prod", help="Environment tag")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    args = parser.parse_args()

    deploy_alerts(args.warehouse_id, args.catalog, args.environment, args.profile)


if __name__ == "__main__":
    main()
