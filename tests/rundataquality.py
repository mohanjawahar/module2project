#!/usr/bin/env python3
"""
Production-ready BigQuery data quality runner.

Environment variables:
  GCP_PROJECT_ID=pilot-488720
  ANALYTICS_DATASET=ecommerce_marts
  GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json   # optional if ADC/oauth already works

"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import List, Optional

import requests
from google.cloud import bigquery


@dataclass
class CheckResult:
    test_name: str
    failed_rows: int


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def getenv_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def build_query(project_id: str, dataset: str) -> str:
    fq_dataset = f"{project_id}.{dataset}"

    return f"""
with checks as (

    select 'null_total_sale_amount' as test_name, count(*) as failed_rows
    from `{fq_dataset}.fct_sales`
    where total_sale_amount is null

    union all

    select 'null_customer_key', count(*)
    from `{fq_dataset}.fct_sales`
    where customer_key is null

    union all

    select 'null_product_key', count(*)
    from `{fq_dataset}.fct_sales`
    where product_key is null

    union all

    select 'null_seller_key', count(*)
    from `{fq_dataset}.fct_sales`
    where seller_key is null

    union all

    select 'duplicate_fct_sales_grain', count(*)
    from (
        select order_id, order_item_id
        from `{fq_dataset}.fct_sales`
        group by order_id, order_item_id
        having count(*) > 1
    )

    union all

    select 'missing_dim_customer', count(*)
    from `{fq_dataset}.fct_sales` f
    left join `{fq_dataset}.dim_customers` c
        on f.customer_key = c.customer_key
    where c.customer_key is null

    union all

    select 'missing_dim_product', count(*)
    from `{fq_dataset}.fct_sales` f
    left join `{fq_dataset}.dim_products` p
        on f.product_key = p.product_key
    where p.product_key is null

    union all

    select 'missing_dim_seller', count(*)
    from `{fq_dataset}.fct_sales` f
    left join `{fq_dataset}.dim_sellers` s
        on f.seller_key = s.seller_key
    where s.seller_key is null

    union all

    select 'missing_dim_date', count(*)
    from `{fq_dataset}.fct_sales` f
    left join `{fq_dataset}.dim_dates` d
        on f.order_date_key = d.date_key
    where d.date_key is null

    union all

    select 'negative_sales_amount', count(*)
    from `{fq_dataset}.fct_sales`
    where total_sale_amount < 0

    union all

    select 'delivery_days_negative_when_delivered', count(*)
    from `{fq_dataset}.fct_sales`
    where delivered_flag = true
      and delivery_days < 0
)

select
    test_name,
    cast(failed_rows as int64) as failed_rows
from checks
where failed_rows > 0
order by failed_rows desc, test_name
""".strip()


def run_query(project_id: str, query: str, location: Optional[str]) -> List[CheckResult]:
    client = bigquery.Client(project=project_id, location=location)
    job = client.query(query)
    rows = job.result()

    results: List[CheckResult] = []
    for row in rows:
        results.append(CheckResult(
            test_name=row["test_name"], failed_rows=int(row["failed_rows"])))
    return results


def write_csv_report(results: List[CheckResult], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = output_dir / f"dataquality_report_{timestamp}.csv"

    with report_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["test_name", "failed_rows"])
        for result in results:
            writer.writerow([result.test_name, result.failed_rows])

    return report_path


def format_summary(project_id: str, dataset: str, results: List[CheckResult]) -> str:
    header = f"Data quality results for {project_id}.{dataset}"
    divider = "-" * len(header)

    if not results:
        return f"{header}\n{divider}\nPASS: all checks returned 0 failing rows."

    lines = [header, divider,
             "FAIL: one or more checks returned failing rows.", ""]
    total_failed_rows = sum(r.failed_rows for r in results)
    lines.append(f"Checks failed: {len(results)}")
    lines.append(f"Total failing rows across checks: {total_failed_rows}")
    lines.append("")
    lines.append("Failed checks:")
    for result in results:
        lines.append(f"  - {result.test_name}: {result.failed_rows}")
    return "\n".join(lines)


def send_slack_alert(webhook_url: str, summary: str) -> None:
    payload = {"text": summary}
    response = requests.post(webhook_url, json=payload, timeout=15)
    response.raise_for_status()


def send_email_alert(summary: str, subject: str) -> None:
    import smtplib

    smtp_host = getenv_required("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_use_tls = getenv_bool("SMTP_USE_TLS", True)

    email_to = getenv_required("ALERT_EMAIL_TO")
    email_from = getenv_required("ALERT_EMAIL_FROM")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.set_content(summary)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
        if smtp_use_tls:
            server.starttls()
        if smtp_username and smtp_password:
            server.login(smtp_username, smtp_password)
        server.send_message(msg)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BigQuery data quality checks.")
    parser.add_argument("--project-id", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument(
        "--dataset", default=os.getenv("ANALYTICS_DATASET", "ecommerce_marts"))
    parser.add_argument("--location", default=os.getenv("BIGQUERY_LOCATION"))
    parser.add_argument(
        "--output-dir", default=os.getenv("DQ_OUTPUT_DIR", "reports"))
    parser.add_argument("--print-sql", action="store_true",
                        help="Print the generated SQL and exit.")
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()

        if not args.project_id:
            raise ValueError(
                "Missing project id. Set --project-id or GCP_PROJECT_ID.")

        query = build_query(args.project_id, args.dataset)

        if args.print_sql:
            print(query)
            return 0

        results = run_query(args.project_id, query, args.location)
        summary = format_summary(args.project_id, args.dataset, results)
        report_path = write_csv_report(results, Path(args.output_dir))

        print(summary)
        print(f"\nCSV report: {report_path}")

        slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
        if slack_webhook and results:
            try:
                send_slack_alert(
                    slack_webhook, f"{summary}\n\nCSV report: {report_path}")
                print("Slack alert sent.")
            except Exception as exc:
                print(f"Slack alert failed: {exc}", file=sys.stderr)

        if os.getenv("ALERT_EMAIL_TO") and results:
            try:
                send_email_alert(
                    summary=f"{summary}\n\nCSV report: {report_path}",
                    subject=f"FAIL: Data quality check for {args.project_id}.{args.dataset}",
                )
                print("Email alert sent.")
            except Exception as exc:
                print(f"Email alert failed: {exc}", file=sys.stderr)

        return 1 if results else 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())


# import os
# from pathlib import Path

# import pandas as pd
# from dotenv import load_dotenv
# from sqlalchemy import create_engine


# def get_engine():
#     load_dotenv()
#     project_id = os.getenv("GCP_PROJECT_ID")
#     # print(f"In get engine: Value of project id: {project_id}")
#     analytics_dataset = os.getenv(
#         "BQ_DATASET_ANALYTICS", "ecommerce_marts")
#     # print(f"Value of Analytics dataset: {analytics_dataset}")
#     if not project_id:
#         raise ValueError("GCP_PROJECT_ID is required")
#     return create_engine(f"bigquery://{project_id}/{analytics_dataset}")


# def split_sql_statements(sql_text: str):
#     return [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]


# def main():
#     load_dotenv()
#     analytics_dataset = os.getenv(
#         "BQ_DATASET_ANALYTICS", "ecommerce_marts")
#     # print(f"In Main - Value of Analytics dataset: {analytics_dataset}")
#     sql_file = Path(__file__).with_name("dataquality.sql")
#     # print(f"In Main Value of sql file: {sql_file}")
#     sql_text = sql_file.read_text(
#         encoding="utf-8").replace("{{analytics_dataset}}", analytics_dataset)
#     # print(f"In Main Value of sql text: {sql_text}")
#     engine = get_engine()

#     failures = []
#     for statement in split_sql_statements(sql_text):
#         df = pd.read_sql(statement, engine)
#         # print(f"In For loop print df: {df}")
#         if "failed_rows" in df.columns:
#             current = df[df["failed_rows"] > 0]
#         elif "failed_groups" in df.columns:
#             current = df[df["failed_groups"] > 0]
#         else:
#             current = pd.DataFrame()

#         if not current.empty:
#             failures.append(current)

#     if failures:
#         result = pd.concat(failures, ignore_index=True)
#         print("DATA QUALITY CHECKS FAILED")
#         print(result.to_string(index=False))
#         raise SystemExit(1)

#     print("All data quality checks passed.")


# if __name__ == "__main__":
#     main()
