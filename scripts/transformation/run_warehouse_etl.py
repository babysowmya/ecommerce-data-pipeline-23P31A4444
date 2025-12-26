import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from load_warehouse import (
    load_dim_date,
    load_dim_payment_method,
    load_dim_customers,
    load_dim_products,
    load_fact_sales,
    load_aggregates,
    close_connection
)


def main():
    # ---------- READ TRANSFORMED DATA ----------
    customers_df = pd.read_csv("data/raw/customers.csv")
    products_df = pd.read_csv("data/raw/products.csv")
    transactions_df = pd.read_csv("data/raw/transactions.csv")
    transaction_items_df = pd.read_csv("data/raw/transaction_items.csv")

    # ---------- LOAD DIMENSIONS ----------
    load_dim_date()
    load_dim_payment_method()
    load_dim_customers(customers_df)
    load_dim_products(products_df)

    # ---------- LOAD FACT ----------
    load_fact_sales(
        transactions_df,
        transaction_items_df,
        products_df   #  THIS WAS MISSING
    )

    # ---------- AGGREGATES ----------
    load_aggregates()

    close_connection()


if __name__ == "__main__":
    main()