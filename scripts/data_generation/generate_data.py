import pandas as pd
import random
import json
from faker import Faker
from datetime import datetime, timedelta
import yaml
import os
from dotenv import load_dotenv
load_dotenv()

fake = Faker()

# ---------- LOAD CONFIG ----------
with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

GEN_CFG = config["data_generation"]

RAW_PATH = "data/raw"
os.makedirs(RAW_PATH, exist_ok=True)

# ---------- CUSTOMERS ----------
def generate_customers(n):
    customers = []
    emails = set()

    for i in range(1, n + 1):
        email = fake.email()
        while email in emails:
            email = fake.email()
        emails.add(email)

        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "age_group": random.choice(["18-25", "26-35", "36-45", "46-60", "60+"])
        })

    return pd.DataFrame(customers)

# ---------- PRODUCTS ----------
def generate_products(n):
    categories = {
        "Electronics": ["Mobile", "Laptop", "Headphones"],
        "Clothing": ["Shirt", "Jeans", "Dress"],
        "Home & Kitchen": ["Mixer", "Pan", "Vacuum"],
        "Books": ["Novel", "Comics", "Education"],
        "Sports": ["Bat", "Ball", "Shoes"],
        "Beauty": ["Cream", "Perfume", "Makeup"]
    }

    products = []
    for i in range(1, n + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        price = round(random.uniform(200, 5000), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": f"{fake.word().capitalize()} {sub_category}",
            "category": category,
            "sub_category": sub_category,
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1,50):03d}"
        })

    return pd.DataFrame(products)

# ---------- TRANSACTIONS ----------
def generate_transactions(n, customer_ids):
    transactions = []

    start = datetime(2023, 1, 1)
    for i in range(1, n + 1):
        tx_date = start + timedelta(days=random.randint(0, 365))
        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customer_ids),
            "transaction_date": tx_date.date(),
            "transaction_time": tx_date.time(),
            "payment_method": random.choice(
                ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
            ),
            "shipping_address": fake.address(),
            "total_amount": 0.0  # calculated later
        })

    return pd.DataFrame(transactions)

# ---------- TRANSACTION ITEMS ----------
def generate_transaction_items(transactions, products):
    items = []
    item_id = 1

    product_map = products.set_index("product_id").to_dict("index")

    for _, tx in transactions.iterrows():
        item_count = random.randint(1, 5)
        chosen_products = random.sample(list(product_map.keys()), item_count)

        tx_total = 0
        for pid in chosen_products:
            qty = random.randint(1, 3)
            price = product_map[pid]["price"]
            discount = random.choice([0, 5, 10, 15])

            line_total = round(qty * price * (1 - discount / 100), 2)
            tx_total += line_total

            items.append({
                "item_id": f"ITEM{item_id:05d}",
                "transaction_id": tx["transaction_id"],
                "product_id": pid,
                "quantity": qty,
                "unit_price": price,
                "discount_percentage": discount,
                "line_total": line_total
            })
            item_id += 1

        transactions.loc[
            transactions["transaction_id"] == tx["transaction_id"],
            "total_amount"
        ] = round(tx_total, 2)

    return pd.DataFrame(items), transactions

# ---------- VALIDATION ----------
def validate_referential_integrity(customers, products, transactions, items):
    orphan_tx = items[~items["transaction_id"].isin(transactions["transaction_id"])]
    orphan_prod = items[~items["product_id"].isin(products["product_id"])]

    return {
        "orphan_transaction_items": len(orphan_tx),
        "orphan_products": len(orphan_prod),
        "data_quality_score": 100 if orphan_tx.empty and orphan_prod.empty else 90
    }

# ---------- MAIN ----------
def main():
    customers = generate_customers(GEN_CFG["customers"])
    products = generate_products(GEN_CFG["products"])
    transactions = generate_transactions(GEN_CFG["orders"], customers["customer_id"].tolist())
    items, transactions = generate_transaction_items(transactions, products)

    customers.to_csv(f"{RAW_PATH}/customers.csv", index=False)
    products.to_csv(f"{RAW_PATH}/products.csv", index=False)
    transactions.to_csv(f"{RAW_PATH}/transactions.csv", index=False)
    items.to_csv(f"{RAW_PATH}/transaction_items.csv", index=False)

    metadata = {
        "generated_at": datetime.now().isoformat(),
        "record_counts": {
            "customers": len(customers),
            "products": len(products),
            "transactions": len(transactions),
            "transaction_items": len(items)
        },
        "validation": validate_referential_integrity(
            customers, products, transactions, items
        )
    }

    with open(f"{RAW_PATH}/generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

if __name__ == "__main__":
    main()