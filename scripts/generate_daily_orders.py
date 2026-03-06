import csv, random
from datetime import datetime, timedelta

random.seed(43)

N_NEW_ORDERS = 50
OUT_ORDERS = "data/orders_daily.csv"
OUT_PAYMENTS = "data/payments_daily.csv"

def rand_dt(days_back=7):
    return datetime.now() - timedelta(days=random.randint(0, days_back),
                                      hours=random.randint(0, 23),
                                      minutes=random.randint(0, 59))

def main():
    # customers
    customer_ids = []
    with open("data/customers.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["customer_id"]:
                customer_ids.append(row["customer_id"])

    orders = []
    payments = []

    for i in range(N_NEW_ORDERS):
        oid = f"OD{int(datetime.now().timestamp())}{i:02d}"   # unique enough for learning
        cid = random.choice(customer_ids)
        total = round(random.uniform(10, 400), 2)

        orders.append({
            "order_id": oid,
            "customer_id": cid,
            "order_date": rand_dt(7).strftime("%Y-%m-%d %H:%M:%S"),
            "order_total": total
        })

        status = random.choices(["paid", "failed", "pending"], weights=[0.85, 0.1, 0.05])[0]
        amount = total if status == "paid" else 0.0

        payments.append({
            "payment_id": f"PD{int(datetime.now().timestamp())}{i:02d}",
            "order_id": oid,
            "payment_status": status,
            "payment_amount": round(amount, 2)
        })

    with open(OUT_ORDERS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["order_id","customer_id","order_date","order_total"])
        w.writeheader()
        w.writerows(orders)

    with open(OUT_PAYMENTS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["payment_id","order_id","payment_status","payment_amount"])
        w.writeheader()
        w.writerows(payments)

    print("Generated:", OUT_ORDERS, "and", OUT_PAYMENTS)

if __name__ == "__main__":
    main()
