#!/usr/bin/env python3
"""
Generate dummy data for transactions, orders, and chargebacks.
Includes intentional inconsistencies for validation testing.
"""

import json
import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path


def generate_id(prefix: str, length: int = 8) -> str:
    """Generate a random ID with a prefix."""
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"{prefix}_{suffix}"


def random_timestamp(start_days_ago: int = 90, end_days_ago: int = 0) -> str:
    """Generate a random ISO timestamp within a date range."""
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    random_date = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
    return random_date.isoformat()


def generate_orders(count: int) -> list[dict]:
    """Generate order records."""
    orders = []
    currencies = ["USD", "EUR", "GBP"]
    payment_statuses = ["paid", "failed", "refunded"]

    for i in range(count):
        order_id = generate_id("ORD")
        num_items = random.randint(1, 5)
        items = []

        for _ in range(num_items):
            unit_price = round(random.uniform(5.0, 200.0), 2)
            quantity = random.randint(1, 3)
            items.append({
                "product_id": generate_id("PROD"),
                "quantity": quantity,
                "unit_price": unit_price
            })

        total_amount = round(sum(item["unit_price"] * item["quantity"] for item in items), 2)

        # Intentional inconsistency #1: Negative amount for one order
        if i == 15:
            total_amount = -50.00

        order = {
            "order_id": order_id,
            "customer_id": generate_id("CUST"),
            "timestamp": random_timestamp(),
            "total_amount": total_amount,
            "currency": random.choice(currencies),
            "items": items,
            "payment_status": random.choice(payment_statuses)
        }
        orders.append(order)

    return orders


def generate_transactions(count: int, orders: list[dict]) -> list[dict]:
    """Generate transaction records with some linked to orders."""
    transactions = []
    statuses = ["completed", "failed", "pending"]
    payment_types = ["credit_card", "debit_card", "wallet"]
    providers = {
        "credit_card": ["Visa", "Mastercard", "Amex"],
        "debit_card": ["Visa Debit", "Mastercard Debit"],
        "wallet": ["PayPal", "Apple Pay", "Google Pay"]
    }
    error_codes = ["INSUFFICIENT_FUNDS", "CARD_DECLINED", "EXPIRED_CARD", "INVALID_CVV"]
    currencies = ["USD", "EUR", "GBP"]

    # Use most order IDs but leave some transactions orphaned
    available_order_ids = [o["order_id"] for o in orders]

    for i in range(count):
        payment_type = random.choice(payment_types)
        status = random.choice(statuses)

        # Determine order_id - some will be orphaned (no matching order)
        if i < 85 and i < len(available_order_ids):
            order_id = available_order_ids[i]
            matching_order = orders[i]
            amount = matching_order["total_amount"]
            currency = matching_order["currency"]
        else:
            # Intentional inconsistency #2: Orphaned transactions (no matching order)
            order_id = generate_id("ORD")  # Non-existent order
            amount = round(random.uniform(10.0, 500.0), 2)
            currency = random.choice(currencies)

        # Intentional inconsistency #3: Mismatched currency for some transactions
        if i in [25, 45, 67]:
            # Pick a different currency than the order
            other_currencies = [c for c in currencies if c != currency]
            currency = random.choice(other_currencies)

        # Intentional inconsistency #4: Negative amount for some transactions
        if i == 30:
            amount = -100.50

        transaction = {
            "transaction_id": generate_id("TXN"),
            "order_id": order_id,
            "timestamp": random_timestamp(),
            "amount": amount,
            "currency": currency,
            "status": status,
            "payment_method": {
                "type": payment_type,
                "provider": random.choice(providers[payment_type])
            },
            "error_code": random.choice(error_codes) if status == "failed" else None
        }
        transactions.append(transaction)

    return transactions


def generate_chargebacks(count: int, transactions: list[dict]) -> list[dict]:
    """Generate chargeback records linked to transactions."""
    chargebacks = []
    reason_codes = ["4837", "4853", "4863", "10.4", "13.1", "75"]
    statuses = ["open", "won", "lost", "pending_response"]

    # Select some completed transactions for chargebacks
    completed_txns = [t for t in transactions if t["status"] == "completed"]
    selected_txns = random.sample(completed_txns, min(count - 2, len(completed_txns)))

    for i, txn in enumerate(selected_txns):
        dispute_date = random_timestamp(60, 10)
        resolution_date = ""
        status = random.choice(statuses)

        if status in ["won", "lost"]:
            resolution_date = random_timestamp(9, 0)

        chargeback = {
            "transaction_id": txn["transaction_id"],
            "dispute_date": dispute_date,
            "amount": txn["amount"],
            "reason_code": random.choice(reason_codes),
            "status": status,
            "resolution_date": resolution_date
        }

        # Intentional inconsistency #5: Chargeback amount differs from transaction
        if i == 5:
            chargeback["amount"] = round(txn["amount"] * 1.5, 2)

        chargebacks.append(chargeback)

    # Intentional inconsistency #6: Chargeback for non-existent transaction
    chargebacks.append({
        "transaction_id": generate_id("TXN"),  # Non-existent
        "dispute_date": random_timestamp(30, 5),
        "amount": 150.00,
        "reason_code": "4837",
        "status": "open",
        "resolution_date": ""
    })

    # Intentional inconsistency #7: Chargeback with negative amount
    chargebacks.append({
        "transaction_id": random.choice(completed_txns)["transaction_id"],
        "dispute_date": random_timestamp(30, 5),
        "amount": -75.00,
        "reason_code": "13.1",
        "status": "pending_response",
        "resolution_date": ""
    })

    return chargebacks


def save_json(data: list[dict], filepath: Path) -> None:
    """Save data as JSON file."""
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Generated {filepath} with {len(data)} records")


def save_csv(data: list[dict], filepath: Path) -> None:
    """Save data as CSV file."""
    if not data:
        return

    fieldnames = ["transaction_id", "dispute_date", "amount", "reason_code", "status", "resolution_date"]
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Generated {filepath} with {len(data)} records")


def main():
    # Set seed for reproducibility (optional - comment out for different data each run)
    random.seed(42)

    # Paths
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)

    # Generate data
    print("Generating dummy data with intentional inconsistencies...\n")

    orders = generate_orders(100)
    transactions = generate_transactions(100, orders)
    chargebacks = generate_chargebacks(20, transactions)

    # Save files
    save_json(orders, data_dir / "orders.json")
    save_json(transactions, data_dir / "transactions.json")
    save_csv(chargebacks, data_dir / "chargebacks.csv")

    # Print summary of intentional inconsistencies
    print("\n--- Intentional Inconsistencies for Validation Testing ---")
    print("1. Order #15: Negative total_amount (-50.00)")
    print("2. Transactions #85-99: Orphaned (no matching order)")
    print("3. Transactions #25, #45, #67: Currency mismatch with linked order")
    print("4. Transaction #30: Negative amount (-100.50)")
    print("5. Chargeback #5: Amount differs from original transaction")
    print("6. One chargeback: References non-existent transaction")
    print("7. One chargeback: Has negative amount (-75.00)")


if __name__ == "__main__":
    main()
