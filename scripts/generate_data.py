import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

OUT_DIR = "data"

N_CUSTOMERS = 200
N_PRODUCTS = 50
N_ORDERS = 1000

def rand_dt(days_back=90):
    return datetime.now() - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def main():
    # customers
    customers = []
    customer_ids = []
    for i in range(1, N_CUSTOMERS + 1):
        cid = f"C{i:04d}"
        customer_ids.append(cid)
        customers.append({
            "customer_id": cid,
            "name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "created_at": rand_dt(365).strftime("%Y-%m-%d %H:%M:%S")
        })

    # bad customer (missing name)
    customers.append({
        "customer_id": "C9999",
        "name": "",
        "email": "broken@example.com",
        "city": "Seattle",
        "created_at": rand_dt(365).strftime("%Y-%m-%d %H:%M:%S")
       })

    write_csv(f"{OUT_DIR}/customers.csv",
              ["customer_id", "name", "email", "city", "created_at"],
              customers)

    # products
    categories = ["electronics", "home", "beauty", "sports", "books"]
    products = []
    product_ids = []
    for i in range(1, N_PRODUCTS + 1):
        pid = f"P{i:04d}"
        product_ids.append(pid)
        products.append({
            "product_id": pid,
            "product_name": fake.word().title() + " " + fake.word().title(),
            "category": random.choice(categories),
            "price": round(random.uniform(5, 500), 2)
        })

    write_csv(f"{OUT_DIR}/products.csv",
              ["product_id", "product_name", "category", "price"],
              products)

    # orders
    orders = []
    order_ids = []
    for i in range(1, N_ORDERS + 1):
        oid = f"O{i:06d}"
        order_ids.append(oid)

        cid = random.choice(customer_ids)
        k = random.randint(1, 3)
        chosen = random.sample(products, k)
        total = sum(float(p["price"]) for p in chosen)

        orders.append({
            "order_id": oid,
            "customer_id": cid,
            "order_date": rand_dt(90).strftime("%Y-%m-%d %H:%M:%S"),
            "order_total": round(total, 2)
        })

    # bad orders
    orders.append({**orders[10]})  # duplicate
    orders.append({
        "order_id": "O999998",
        "customer_id": "",
        "order_date": rand_dt(90).strftime("%Y-%m-%d %H:%M:%S"),
        "order_total": 25.00
    })
    orders.append({
        "order_id": "O999999",
        "customer_id": random.choice(customer_ids),
        "order_date": rand_dt(90).strftime("%Y-%m-%d %H:%M:%S"),
        "order_total": -10.00
    })

    write_csv(f"{OUT_DIR}/orders.csv",
              ["order_id", "customer_id", "order_date", "order_total"],
              orders)

    # payments
    statuses = ["paid", "failed", "refunded", "pending"]
    payments = []
    for i, oid in enumerate(order_ids, start=1):
        status = random.choices(statuses, weights=[0.85, 0.07, 0.03, 0.05])[0]
        o = next(x for x in orders if x["order_id"] == oid)
        amount = float(o["order_total"])

        if random.random() < 0.02:
            amount = round(amount + random.uniform(-5, 5), 2)
        if status == "failed":
            amount = 0.00

        payments.append({
            "payment_id": f"PAY{i:06d}",
            "order_id": oid,
            "payment_status": status,
            "payment_amount": round(amount, 2)
        })

    payments.append({
        "payment_id": "PAY999999",
        "order_id": "",
        "payment_status": "paid",
        "payment_amount": 19.99
    })

    write_csv(f"{OUT_DIR}/payments.csv",
              ["payment_id", "order_id", "payment_status", "payment_amount"],
              payments)

    print("Generated: data/customers.csv, data/products.csv, data/orders.csv, data/payments.csv")

if __name__ == "__main__":
    main()


