"""
Deploy Lakeview dashboard to a Databricks workspace.

Reads the Lakeview JSON definition, injects SQL queries from the
dashboard/queries/ directory, and creates or updates the dashboard
via the Lakeview REST API.

Usage:
    python deploy/deploy_dashboard.py --warehouse-id <id> --profile DEFAULT
"""

import argparse
import json
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient


QUERY_FILE_MAP = {
    "ds_stream_overview": "01_stream_overview.sql",
    "ds_throughput_trends": "02_throughput_trends.sql",
    "ds_latency_sla": "03_latency_sla.sql",
    "ds_backpressure": "04_backpressure.sql",
    "ds_state_store": "05_state_store_health.sql",
    "ds_data_quality": "06_data_quality.sql",
    "ds_infrastructure": "07_infrastructure_cost.sql",
}


def load_query(filename: str, catalog: str) -> str:
    """Load a SQL query file and substitute the catalog placeholder."""
    queries_dir = Path(__file__).parent.parent / "dashboard" / "queries"
    filepath = queries_dir / filename
    sql = filepath.read_text()
    return sql.replace("${catalog}", catalog)


def build_dashboard_payload(
    warehouse_id: str,
    catalog: str,
) -> dict:
    """Build the full Lakeview dashboard payload with embedded queries."""
    definition_path = (
        Path(__file__).parent.parent / "dashboard" / "lakeview_definition.json"
    )
    with open(definition_path, "r") as f:
        dashboard = json.load(f)

    dashboard["warehouse_id"] = warehouse_id

    # Inject actual SQL into dataset definitions
    serialized = dashboard.get("serialized_dashboard", dashboard)
    for dataset in serialized.get("datasets", []):
        ds_name = dataset["name"]
        if ds_name in QUERY_FILE_MAP:
            dataset["query"] = load_query(QUERY_FILE_MAP[ds_name], catalog)

    # Serialize the inner dashboard to a JSON string (Lakeview API expects this)
    dashboard["serialized_dashboard"] = json.dumps(serialized)

    return dashboard


def deploy_dashboard(
    warehouse_id: str,
    catalog: str,
    profile: str = "DEFAULT",
    dashboard_id: str = None,
) -> str:
    """Create or update a Lakeview dashboard. Returns the dashboard ID."""
    client = WorkspaceClient(profile=profile)
    payload = build_dashboard_payload(warehouse_id, catalog)

    if dashboard_id:
        # Update existing dashboard
        print(f"Updating dashboard {dashboard_id}...")
        resp = client.api_client.do(
            "PATCH",
            f"/api/2.0/lakeview/dashboards/{dashboard_id}",
            body=payload,
        )
    else:
        # Create new dashboard
        print("Creating new Lakeview dashboard...")
        resp = client.api_client.do(
            "POST",
            "/api/2.0/lakeview/dashboards",
            body=payload,
        )

    result = resp or {}
    dash_id = result.get("dashboard_id", dashboard_id or "unknown")
    print(f"Dashboard deployed: {dash_id}")

    # Publish the dashboard
    print("Publishing dashboard...")
    client.api_client.do(
        "POST",
        f"/api/2.0/lakeview/dashboards/{dash_id}/published",
        body={
            "embed_credentials": False,
            "warehouse_id": warehouse_id,
        },
    )
    print("Dashboard published successfully.")
    return dash_id


def main():
    parser = argparse.ArgumentParser(description="Deploy Lakeview dashboard")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse ID")
    parser.add_argument("--catalog", default="main", help="Unity Catalog name")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    parser.add_argument("--dashboard-id", default=None,
                        help="Existing dashboard ID to update (omit to create new)")
    args = parser.parse_args()

    deploy_dashboard(args.warehouse_id, args.catalog, args.profile, args.dashboard_id)


if __name__ == "__main__":
    main()
