# import os
# from pathlib import Path

# import matplotlib.pyplot as plt
# import pandas as pd
# from dotenv import load_dotenv
# from sqlalchemy import create_engine


# def get_engine():
#     load_dotenv()
#     project_id = os.getenv("GCP_PROJECT_ID")
#     analytics_dataset = os.getenv(
#         "BQ_DATASET_ANALYTICS", "ecommerce_marts")
#     if not project_id:
#         raise ValueError("GCP_PROJECT_ID is required")
#     return create_engine(f"bigquery://{project_id}/{analytics_dataset}")


# def analytics_dataset() -> str:
#     load_dotenv()
#     return os.getenv("BQ_DATASET_ANALYTICS", "ecommerce_marts")


# def save_monthly_sales(engine, out_dir: Path):
#     dataset = analytics_dataset()
#     query = f"""
#     SELECT d.year_month, SUM(f.total_sale_amount) AS monthly_sales
#     FROM `{dataset}.fct_sales` f
#     JOIN `{dataset}.dim_dates` d ON f.order_date_key = d.date_key
#     GROUP BY d.year_month
#     ORDER BY d.year_month
#     """
#     df = pd.read_sql(query, engine)
#     df.to_csv(out_dir / "monthly_sales.csv", index=False)

#     plt.figure(figsize=(10, 5))
#     plt.plot(df["year_month"], df["monthly_sales"], marker="o")
#     plt.xticks(rotation=45, ha="right")
#     plt.title("Monthly Sales Trend")
#     plt.xlabel("Month")
#     plt.ylabel("Sales Amount")
#     plt.tight_layout()
#     plt.savefig(out_dir / "monthly_sales.png")
#     plt.close()


# def save_top_products(engine, out_dir: Path):
#     dataset = analytics_dataset()
#     query = f"""
#     SELECT p.product_category_name_english, SUM(f.total_sale_amount) AS revenue
#     FROM `{dataset}.fct_sales` f
#     JOIN `{dataset}.dim_products` p ON f.product_key = p.product_key
#     GROUP BY p.product_category_name_english
#     ORDER BY revenue DESC
#     LIMIT 10
#     """
#     df = pd.read_sql(query, engine)
#     df.to_csv(out_dir / "top_products.csv", index=False)

#     plt.figure(figsize=(10, 6))
#     plt.barh(df["product_category_name_english"], df["revenue"])
#     plt.gca().invert_yaxis()
#     plt.title("Top 10 Product Categories by Revenue")
#     plt.xlabel("Revenue")
#     plt.tight_layout()
#     plt.savefig(out_dir / "top_products.png")
#     plt.close()


# def save_customer_segments(engine, out_dir: Path):
#     dataset = analytics_dataset()
#     query = f"""
#     WITH clv AS (
#         SELECT c.customer_unique_id,
#                SUM(f.total_sale_amount) AS clv
#         FROM `{dataset}.fct_sales` f
#         JOIN `{dataset}.dim_customers` c ON f.customer_key = c.customer_key
#         GROUP BY c.customer_unique_id
#     )
#     SELECT CASE
#                WHEN clv >= 1000 THEN 'High Value'
#                WHEN clv >= 300 THEN 'Medium Value'
#                ELSE 'Low Value'
#            END AS segment,
#            COUNT(*) AS customers,
#            SUM(clv) AS segment_revenue
#     FROM clv
#     GROUP BY 1
#     ORDER BY segment_revenue DESC
#     """
#     df = pd.read_sql(query, engine)
#     df.to_csv(out_dir / "customer_segments.csv", index=False)

#     plt.figure(figsize=(7, 7))
#     plt.pie(df["customers"], labels=df["segment"], autopct="%1.1f%%")
#     plt.title("Customer Segmentation by Purchase Behavior")
#     plt.tight_layout()
#     plt.savefig(out_dir / "customer_segments.png")
#     plt.close()


# def main():
#     engine = get_engine()
#     out_dir = Path("analysis/output")
#     out_dir.mkdir(parents=True, exist_ok=True)

#     save_monthly_sales(engine, out_dir)
#     save_top_products(engine, out_dir)
#     save_customer_segments(engine, out_dir)
#     print(f"Analysis outputs saved to {out_dir.resolve()}")


# if __name__ == "__main__":
#     main()

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def get_project_id() -> str:
    load_dotenv()
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID is required")
    return project_id


def get_analytics_dataset() -> str:
    load_dotenv()
    return os.getenv("BQ_DATASET_ANALYTICS", "ecommerce_marts")


def get_engine():
    project_id = get_project_id()
    dataset = get_analytics_dataset()
    return create_engine(f"bigquery://{project_id}/{dataset}")


def full_table_name(table_name: str) -> str:
    project_id = get_project_id()
    dataset = get_analytics_dataset()
    return f"`{project_id}.{dataset}.{table_name}`"


def save_monthly_sales(engine, out_dir: Path):
    fct_sales = full_table_name("fct_sales")
    dim_dates = full_table_name("dim_dates")

    query = f"""
    SELECT
        d.year_month,
        SUM(f.total_sale_amount) AS monthly_sales
    FROM {fct_sales} f
    JOIN {dim_dates} d
        ON f.order_date_key = d.date_key
    GROUP BY d.year_month
    ORDER BY d.year_month
    """

    df = pd.read_sql(query, engine)
    df.to_csv(out_dir / "monthly_sales.csv", index=False)

    plt.figure(figsize=(10, 5))
    plt.plot(df["year_month"], df["monthly_sales"], marker="o")
    plt.xticks(rotation=45, ha="right")
    plt.title("Monthly Sales Trend")
    plt.xlabel("Month")
    plt.ylabel("Sales Amount")
    plt.tight_layout()
    plt.savefig(out_dir / "monthly_sales.png")
    plt.close()


def save_top_products(engine, out_dir: Path):
    fct_sales = full_table_name("fct_sales")
    dim_products = full_table_name("dim_products")

    query = f"""
    SELECT
        p.product_category_name_english,
        SUM(f.total_sale_amount) AS revenue
    FROM {fct_sales} f
    JOIN {dim_products} p
        ON f.product_key = p.product_key
    GROUP BY p.product_category_name_english
    ORDER BY revenue DESC
    LIMIT 10
    """

    df = pd.read_sql(query, engine)
    df.to_csv(out_dir / "top_products.csv", index=False)

    plt.figure(figsize=(10, 6))
    plt.barh(df["product_category_name_english"], df["revenue"])
    plt.gca().invert_yaxis()
    plt.title("Top 10 Product Categories by Revenue")
    plt.xlabel("Revenue")
    plt.tight_layout()
    plt.savefig(out_dir / "top_products.png")
    plt.close()


def save_customer_segments(engine, out_dir: Path):
    fct_sales = full_table_name("fct_sales")
    dim_customers = full_table_name("dim_customers")

    query = f"""
    WITH customer_clv AS (
        SELECT
            c.customer_unique_id,
            SUM(f.total_sale_amount) AS customer_lifetime_value
        FROM {fct_sales} f
        JOIN {dim_customers} c
            ON f.customer_key = c.customer_key
        GROUP BY c.customer_unique_id
    )
    SELECT
        CASE
            WHEN customer_lifetime_value >= 1000 THEN 'High Value'
            WHEN customer_lifetime_value >= 300 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS segment,
        COUNT(*) AS customers,
        SUM(customer_lifetime_value) AS segment_revenue
    FROM customer_clv
    GROUP BY 1
    ORDER BY segment_revenue DESC
    """

    df = pd.read_sql(query, engine)
    df.to_csv(out_dir / "customer_segments.csv", index=False)

    plt.figure(figsize=(7, 7))
    plt.pie(df["customers"], labels=df["segment"], autopct="%1.1f%%")
    plt.title("Customer Segmentation by Purchase Behavior")
    plt.tight_layout()
    plt.savefig(out_dir / "customer_segments.png")
    plt.close()


def save_high_value_product_sellers(engine, out_dir: Path):
    tbl = full_table_name("fct_high_value_product_sellers")

    # Top 10 categories by total revenue across high-value product sellers
    query_cat = f"""
    SELECT
        product_category,
        SUM(seller_product_revenue) AS total_revenue,
        COUNT(DISTINCT seller_id)   AS seller_count
    FROM {tbl}
    GROUP BY product_category
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    df_cat = pd.read_sql(query_cat, engine)
    df_cat.to_csv(out_dir / "high_value_categories.csv", index=False)

    plt.figure(figsize=(10, 6))
    plt.barh(df_cat["product_category"], df_cat["total_revenue"])
    plt.gca().invert_yaxis()
    plt.title("Top 10 High-Value Product Categories by Revenue")
    plt.xlabel("Total Revenue")
    plt.tight_layout()
    plt.savefig(out_dir / "high_value_categories.png")
    plt.close()

    # Top 10 sellers by revenue in high-value products
    query_sellers = f"""
    SELECT
        seller_id,
        seller_city,
        seller_state,
        SUM(seller_product_revenue)  AS total_revenue,
        SUM(seller_product_orders)   AS total_orders,
        ROUND(AVG(seller_product_market_share_pct), 2) AS avg_market_share_pct
    FROM {tbl}
    GROUP BY seller_id, seller_city, seller_state
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    df_sellers = pd.read_sql(query_sellers, engine)
    df_sellers.to_csv(out_dir / "high_value_top_sellers.csv", index=False)

    df_sellers["seller_label"] = df_sellers["seller_city"] + ", " + df_sellers["seller_state"] + " (" + df_sellers["seller_id"].str[:8] + ")"

    plt.figure(figsize=(10, 6))
    plt.barh(df_sellers["seller_label"], df_sellers["total_revenue"])
    plt.gca().invert_yaxis()
    plt.title("Top 10 Sellers by Revenue in High-Value Products")
    plt.xlabel("Total Revenue")
    plt.tight_layout()
    plt.savefig(out_dir / "high_value_top_sellers.png")
    plt.close()


def save_seller_performance(engine, out_dir: Path):
    tbl = full_table_name("fct_seller_performance")
    tbl_hvps = full_table_name("fct_high_value_product_sellers")

    # Top sellers' product category breakdown
    query_products = f"""
    WITH top_sellers AS (
        SELECT seller_id
        FROM {tbl}
        ORDER BY total_revenue DESC
        LIMIT 10
    )
    SELECT
        sp.seller_id,
        sp.seller_city,
        sp.seller_state,
        sp.product_category,
        ROUND(SUM(sp.seller_product_revenue), 2) AS category_revenue
    FROM {tbl_hvps} sp
    INNER JOIN top_sellers ts ON sp.seller_id = ts.seller_id
    GROUP BY sp.seller_id, sp.seller_city, sp.seller_state, sp.product_category
    ORDER BY sp.seller_id, category_revenue DESC
    """
    df_products = pd.read_sql(query_products, engine)
    df_products["seller_label"] = (
        df_products["seller_city"] + ", " + df_products["seller_state"]
        + " (" + df_products["seller_id"].str[:8] + ")"
    )
    df_products.to_csv(out_dir / "seller_top_products.csv", index=False)

    pivot = df_products.pivot_table(
        index="seller_label", columns="product_category",
        values="category_revenue", aggfunc="sum", fill_value=0
    )
    pivot.plot(kind="barh", stacked=True, figsize=(12, 7), colormap="tab20")
    plt.gca().invert_yaxis()
    plt.title("Top 10 Sellers — Revenue by Product Category")
    plt.xlabel("Revenue")
    plt.ylabel("")
    plt.legend(title="Category", bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    plt.savefig(out_dir / "seller_top_products.png")
    plt.close()

    # Top 10 sellers by total revenue
    query_top = f"""
    SELECT
        seller_id,
        seller_city,
        seller_state,
        total_orders,
        total_items_sold,
        total_revenue,
        avg_review_score,
        distinct_categories
    FROM {tbl}
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    df_top = pd.read_sql(query_top, engine)
    df_top.to_csv(out_dir / "seller_performance_top10.csv", index=False)

    df_top["seller_label"] = df_top["seller_city"] + ", " + df_top["seller_state"] + " (" + df_top["seller_id"].str[:8] + ")"

    plt.figure(figsize=(10, 6))
    plt.barh(df_top["seller_label"], df_top["total_revenue"])
    plt.gca().invert_yaxis()
    plt.title("Top 10 Sellers by Total Revenue")
    plt.xlabel("Total Revenue")
    plt.tight_layout()
    plt.savefig(out_dir / "seller_performance_top10.png")
    plt.close()


def main():
    engine = get_engine()
    out_dir = Path("analysis/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    save_monthly_sales(engine, out_dir)
    save_top_products(engine, out_dir)
    save_customer_segments(engine, out_dir)
    save_high_value_product_sellers(engine, out_dir)
    save_seller_performance(engine, out_dir)

    print(f"Analysis outputs saved to {out_dir.resolve()}")


if __name__ == "__main__":
    main()
