import os
import pandas as pd
import re
import pytest
from scripts.data_generation.generate_data import (
    generate_customers,
    generate_products,
    generate_transactions,
    generate_transaction_items
)

@pytest.fixture
def customers():
    return generate_customers(10)

@pytest.fixture
def products():
    return generate_products(5)

@pytest.fixture
def transactions(customers):
    return generate_transactions(10, customers["customer_id"].tolist())

@pytest.fixture
def items(transactions, products):
    items, transactions = generate_transaction_items(transactions, products)
    return items

def test_customers_generation(customers):
    assert len(customers) == 10
    assert "customer_id" in customers.columns
    assert customers["customer_id"].isnull().sum() == 0
    assert "email" in customers.columns
    assert customers["email"].str.contains("@").all()

def test_customer_id_format(customers):
    pattern = re.compile(r"CUST\d{4}")
    assert customers["customer_id"].apply(lambda x: bool(pattern.match(x))).all()

def test_products_generation(products):
    assert (products["price"] > 0).all()
    assert (products["cost"] > 0).all()
    assert "product_id" in products.columns

def test_transactions_and_items_referential_integrity(transactions, items, customers, products):
    assert items["transaction_id"].isin(transactions["transaction_id"]).all()
    assert items["product_id"].isin(products["product_id"]).all()
    # Referential integrity: customer IDs
    assert transactions["customer_id"].isin(customers["customer_id"]).all()

def test_line_total_calculation(transactions, products):
    items, _ = generate_transaction_items(transactions, products)
    row = items.iloc[0]
    expected = round(
        row["quantity"] * row["unit_price"] * (1 - row["discount_percentage"] / 100),
        2
    )
    assert round(row["line_total"], 2) == expected

def test_discount_percentage_range(items):
    assert items["discount_percentage"].between(0, 100).all()

def test_quantity_positive(items):
    assert (items["quantity"] > 0).all()
