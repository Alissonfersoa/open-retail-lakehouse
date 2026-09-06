import argparse
import csv
import random

from datetime import datetime, timedelta
from pathlib import Path


LOCATIONS = [
    ("São José dos Campos", "SP"),
    ("São Paulo", "SP"),
    ("Campinas", "SP"),
    ("Rio de Janeiro", "RJ"),
    ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"),
    ("Porto Alegre", "RS"),
    ("Salvador", "BA"),
    ("Recife", "PE"),
    ("Brasília", "DF"),
]

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "pix",
    "bank_slip",
]


def random_datetime(start: datetime, end: datetime) -> datetime:
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--start-id", type=int, default=1)
    parser.add_argument(
        "--output",
        default="data/landing/transactions.csv",
    )
    parser.add_argument(
        "--bad-rate",
        type=float,
        default=0.002,
    )
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    random.seed(args.seed)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start = datetime(2025, 1, 1)
    end = datetime(2026, 8, 31, 23, 59, 59)

    columns = [
        "transaction_id",
        "customer_id",
        "product_id",
        "store_id",
        "transaction_date",
        "quantity",
        "unit_price",
        "discount_pct",
        "payment_method",
        "city",
        "state",
        "created_at",
    ]

    with output_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(file, fieldnames=columns)

        writer.writeheader()

        for offset in range(args.rows):

            transaction_id = args.start_id + offset

            customer_id = random.randint(1, 50_000)
            product_id = random.randint(1, 2_000)
            store_id = random.randint(1, 100)

            transaction_datetime = random_datetime(start, end)

            quantity = random.randint(1, 5)

            unit_price = round(
                random.uniform(5, 500),
                2,
            )

            discount_pct = random.choice([
                0,
                0,
                0,
                0.05,
                0.10,
                0.15,
                0.20,
            ])

            payment_method = random.choice(
                PAYMENT_METHODS
            )

            city, state = random.choice(LOCATIONS)

            # Introduz propositalmente alguns dados ruins.
            if random.random() < args.bad_rate:

                defect = random.choice([
                    "zero_quantity",
                    "null_customer",
                    "duplicate_id",
                    "negative_price",
                ])

                if defect == "zero_quantity":
                    quantity = 0

                elif defect == "null_customer":
                    customer_id = ""

                elif defect == "duplicate_id" and offset > 0:
                    transaction_id -= 1

                elif defect == "negative_price":
                    unit_price = -unit_price

            writer.writerow({
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "store_id": store_id,
                "transaction_date": transaction_datetime.date().isoformat(),
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_pct": discount_pct,
                "payment_method": payment_method,
                "city": city,
                "state": state,
                "created_at": transaction_datetime.isoformat(),
            })

    print(
        f"Generated {args.rows:,} records "
        f"at {output_path}"
    )


if __name__ == "__main__":
    main()