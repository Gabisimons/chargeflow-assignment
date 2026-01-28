"""
Data pipeline for ingesting, validating, transforming, and analyzing
transaction, order, and chargeback data.
"""

import csv
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generic, List, Tuple, Type, TypeVar

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.models import (
    Chargeback,
    Order,
    Transaction,
    TransactionStatus,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


@dataclass
class ValidationResult(Generic[T]):
    """Container for validation results."""
    valid_records: List[T] = field(default_factory=list)
    failed_records: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        return len(self.valid_records) + len(self.failed_records)

    @property
    def success_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        return len(self.valid_records) / self.total_processed * 100


class DataPipeline:
    """
    Pipeline for processing transaction, order, and chargeback data.

    Handles ingestion, validation, transformation, and analysis.
    """

    def __init__(self, data_dir: Path):
        """
        Initialize the pipeline.

        Args:
            data_dir: Path to the directory containing data files.
        """
        self.data_dir = Path(data_dir)

        # Validation results
        self.transactions_result: ValidationResult[Transaction] = ValidationResult()
        self.orders_result: ValidationResult[Order] = ValidationResult()
        self.chargebacks_result: ValidationResult[Chargeback] = ValidationResult()

        # DataFrames
        self.transactions_df: pd.DataFrame = pd.DataFrame()
        self.orders_df: pd.DataFrame = pd.DataFrame()
        self.chargebacks_df: pd.DataFrame = pd.DataFrame()
        self.merged_df: pd.DataFrame = pd.DataFrame()

    def _validate_records(
        self,
        raw_records: List[Dict[str, Any]],
        model_class: Type[T],
        source_name: str
    ) -> ValidationResult[T]:
        """
        Validate records against a Pydantic model.

        Args:
            raw_records: List of raw dictionaries to validate.
            model_class: Pydantic model class for validation.
            source_name: Name of the data source for logging.

        Returns:
            ValidationResult containing valid and failed records.
        """
        result: ValidationResult[T] = ValidationResult()

        for idx, record in enumerate(raw_records):
            try:
                validated = model_class.model_validate(record)
                result.valid_records.append(validated)
            except ValidationError as e:
                error_details = {
                    "index": idx,
                    "record": record,
                    "errors": e.errors()
                }
                result.failed_records.append(error_details)
                logger.warning(
                    f"{source_name} validation failed at index {idx}: "
                    f"{e.errors()[0]['msg'] if e.errors() else 'Unknown error'}"
                )

        logger.info(
            f"{source_name}: {len(result.valid_records)} valid, "
            f"{len(result.failed_records)} failed "
            f"({result.success_rate:.1f}% success rate)"
        )

        return result

    def _load_json(self, filename: str) -> List[Dict[str, Any]]:
        """Load data from a JSON file."""
        filepath = self.data_dir / filename
        logger.info(f"Loading {filepath}")

        with open(filepath, "r") as f:
            return json.load(f)

    def _load_csv(self, filename: str) -> List[Dict[str, Any]]:
        """Load data from a CSV file."""
        filepath = self.data_dir / filename
        logger.info(f"Loading {filepath}")

        records = []
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert amount to float for CSV
                if "amount" in row:
                    try:
                        row["amount"] = float(row["amount"])
                    except (ValueError, TypeError):
                        pass  # Let validation catch this
                records.append(row)

        return records

    def ingest_and_validate(self) -> "DataPipeline":
        """
        Load all data files and validate against Pydantic models.

        Returns:
            Self for method chaining.
        """
        logger.info("=" * 50)
        logger.info("Starting data ingestion and validation")
        logger.info("=" * 50)

        # Load and validate transactions
        raw_transactions = self._load_json("transactions.json")
        self.transactions_result = self._validate_records(
            raw_transactions, Transaction, "Transactions"
        )

        # Load and validate orders
        raw_orders = self._load_json("orders.json")
        self.orders_result = self._validate_records(
            raw_orders, Order, "Orders"
        )

        # Load and validate chargebacks
        raw_chargebacks = self._load_csv("chargebacks.csv")
        self.chargebacks_result = self._validate_records(
            raw_chargebacks, Chargeback, "Chargebacks"
        )

        logger.info("Ingestion and validation complete")
        return self

    def _transaction_to_dict(self, txn: Transaction) -> Dict[str, Any]:
        """Convert Transaction model to flat dictionary for DataFrame."""
        return {
            "transaction_id": txn.transaction_id,
            "order_id": txn.order_id,
            "txn_timestamp": txn.timestamp,
            "txn_amount": float(txn.amount),
            "txn_currency": txn.currency.value,
            "txn_status": txn.status.value,
            "payment_type": txn.payment_method.type.value,
            "payment_provider": txn.payment_method.provider,
            "error_code": txn.error_code,
        }

    def _order_to_dict(self, order: Order) -> Dict[str, Any]:
        """Convert Order model to flat dictionary for DataFrame."""
        return {
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "order_timestamp": order.timestamp,
            "order_total_amount": float(order.total_amount),
            "order_currency": order.currency.value,
            "order_item_count": len(order.items),
            "payment_status": order.payment_status.value,
        }

    def _chargeback_to_dict(self, cb: Chargeback) -> Dict[str, Any]:
        """Convert Chargeback model to flat dictionary for DataFrame."""
        return {
            "transaction_id": cb.transaction_id,
            "cb_dispute_date": cb.dispute_date,
            "cb_amount": float(cb.amount),
            "cb_reason_code": cb.reason_code,
            "cb_status": cb.status.value,
            "cb_resolution_date": cb.resolution_date,
        }

    def transform(self) -> "DataPipeline":
        """
        Transform valid Pydantic objects into Pandas DataFrames and merge.

        Returns:
            Self for method chaining.
        """
        logger.info("=" * 50)
        logger.info("Starting data transformation")
        logger.info("=" * 50)

        # Convert to DataFrames
        if self.transactions_result.valid_records:
            self.transactions_df = pd.DataFrame([
                self._transaction_to_dict(t) for t in self.transactions_result.valid_records
            ])
        else:
            self.transactions_df = pd.DataFrame(columns=[
                "transaction_id", "order_id", "txn_timestamp", "txn_amount",
                "txn_currency", "txn_status", "payment_type", "payment_provider",
                "error_code"
            ])
        logger.info(f"Transactions DataFrame: {len(self.transactions_df)} rows")

        if self.orders_result.valid_records:
            self.orders_df = pd.DataFrame([
                self._order_to_dict(o) for o in self.orders_result.valid_records
            ])
        else:
            self.orders_df = pd.DataFrame(columns=[
                "order_id", "customer_id", "order_timestamp", "order_total_amount",
                "order_currency", "order_item_count", "payment_status"
            ])
        logger.info(f"Orders DataFrame: {len(self.orders_df)} rows")

        if self.chargebacks_result.valid_records:
            self.chargebacks_df = pd.DataFrame([
                self._chargeback_to_dict(cb) for cb in self.chargebacks_result.valid_records
            ])
        else:
            self.chargebacks_df = pd.DataFrame(columns=[
                "transaction_id", "cb_dispute_date", "cb_amount",
                "cb_reason_code", "cb_status", "cb_resolution_date"
            ])
        logger.info(f"Chargebacks DataFrame: {len(self.chargebacks_df)} rows")

        # Merge transactions with orders
        self.merged_df = self.transactions_df.merge(
            self.orders_df,
            on="order_id",
            how="left",
            suffixes=("", "_order")
        )
        logger.info(f"After merging with orders: {len(self.merged_df)} rows")

        # Merge with chargebacks (left join)
        if not self.chargebacks_df.empty:
            self.merged_df = self.merged_df.merge(
                self.chargebacks_df,
                on="transaction_id",
                how="left",
                suffixes=("", "_cb")
            )
        else:
            # Add empty chargeback columns
            for col in ["cb_dispute_date", "cb_amount", "cb_reason_code",
                       "cb_status", "cb_resolution_date"]:
                self.merged_df[col] = None

        logger.info(f"After merging with chargebacks: {len(self.merged_df)} rows")

        # Normalize amounts (ensure float type)
        amount_columns = ["txn_amount", "order_total_amount", "cb_amount"]
        for col in amount_columns:
            if col in self.merged_df.columns:
                self.merged_df[col] = pd.to_numeric(
                    self.merged_df[col], errors="coerce"
                ).astype(float)

        # Add date column for daily aggregations
        if not self.merged_df.empty and "txn_timestamp" in self.merged_df.columns:
            self.merged_df["txn_date"] = pd.to_datetime(
                self.merged_df["txn_timestamp"]
            ).dt.date
        else:
            self.merged_df["txn_date"] = None

        logger.info("Transformation complete")
        return self

    def daily_volume(self) -> pd.DataFrame:
        """
        Calculate total transaction amount per day.

        Returns:
            DataFrame with date and total_amount columns.
        """
        if self.merged_df.empty:
            logger.warning("No data available for daily volume calculation")
            return pd.DataFrame(columns=["date", "total_amount"])

        result = (
            self.merged_df
            .groupby("txn_date")["txn_amount"]
            .sum()
            .reset_index()
            .rename(columns={"txn_date": "date", "txn_amount": "total_amount"})
            .sort_values("date")
        )

        logger.info(f"Daily volume calculated for {len(result)} days")
        return result

    def chargeback_rates(self) -> pd.DataFrame:
        """
        Calculate chargeback rate per payment method.

        Rate = (Total CB Amount / Total Transaction Amount) * 100

        Returns:
            DataFrame with payment_type, total_txn_amount, total_cb_amount,
            and chargeback_rate columns.
        """
        if self.merged_df.empty:
            logger.warning("No data available for chargeback rate calculation")
            return pd.DataFrame(columns=[
                "payment_type", "total_txn_amount", "total_cb_amount", "chargeback_rate"
            ])

        # Group by payment type
        grouped = self.merged_df.groupby("payment_type").agg(
            total_txn_amount=("txn_amount", "sum"),
            total_cb_amount=("cb_amount", "sum")
        ).reset_index()

        # Fill NaN chargeback amounts with 0
        grouped["total_cb_amount"] = grouped["total_cb_amount"].fillna(0)

        # Calculate rate
        grouped["chargeback_rate"] = (
            grouped["total_cb_amount"] / grouped["total_txn_amount"] * 100
        ).round(2)

        # Handle division by zero
        grouped["chargeback_rate"] = grouped["chargeback_rate"].fillna(0)

        logger.info(f"Chargeback rates calculated for {len(grouped)} payment types")
        return grouped

    def payment_failure_rate(self) -> float:
        """
        Calculate the percentage of transactions with status 'failed'.

        Returns:
            Failure rate as a percentage (0-100).
        """
        if self.merged_df.empty:
            logger.warning("No data available for failure rate calculation")
            return 0.0

        total_transactions = len(self.merged_df)
        failed_transactions = len(
            self.merged_df[self.merged_df["txn_status"] == TransactionStatus.FAILED.value]
        )

        rate = (failed_transactions / total_transactions) * 100

        logger.info(
            f"Payment failure rate: {rate:.2f}% "
            f"({failed_transactions}/{total_transactions})"
        )
        return round(rate, 2)

    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get a summary of validation results.

        Returns:
            Dictionary with validation statistics.
        """
        return {
            "transactions": {
                "valid": len(self.transactions_result.valid_records),
                "failed": len(self.transactions_result.failed_records),
                "success_rate": round(self.transactions_result.success_rate, 2)
            },
            "orders": {
                "valid": len(self.orders_result.valid_records),
                "failed": len(self.orders_result.failed_records),
                "success_rate": round(self.orders_result.success_rate, 2)
            },
            "chargebacks": {
                "valid": len(self.chargebacks_result.valid_records),
                "failed": len(self.chargebacks_result.failed_records),
                "success_rate": round(self.chargebacks_result.success_rate, 2)
            }
        }

    def get_failed_records(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all failed validation records for debugging.

        Returns:
            Dictionary with failed records by type.
        """
        return {
            "transactions": self.transactions_result.failed_records,
            "orders": self.orders_result.failed_records,
            "chargebacks": self.chargebacks_result.failed_records
        }

    def run(self) -> "DataPipeline":
        """
        Execute the full pipeline: ingest, validate, and transform.

        Returns:
            Self for method chaining.
        """
        return self.ingest_and_validate().transform()


def main():
    """Run the pipeline and display results."""
    data_dir = Path(__file__).parent.parent / "data"

    # Run pipeline
    pipeline = DataPipeline(data_dir).run()

    # Print validation summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    summary = pipeline.get_validation_summary()
    for source, stats in summary.items():
        print(f"\n{source.upper()}:")
        print(f"  Valid:   {stats['valid']}")
        print(f"  Failed:  {stats['failed']}")
        print(f"  Success: {stats['success_rate']}%")

    # Print failed records details
    failed = pipeline.get_failed_records()
    total_failed = sum(len(records) for records in failed.values())
    if total_failed > 0:
        print("\n" + "=" * 60)
        print("FAILED VALIDATION DETAILS")
        print("=" * 60)
        for source, records in failed.items():
            if records:
                print(f"\n{source.upper()} failures:")
                for rec in records[:3]:  # Show first 3
                    print(f"  Index {rec['index']}: {rec['errors'][0]['msg']}")
                if len(records) > 3:
                    print(f"  ... and {len(records) - 3} more")

    # Print analysis results
    print("\n" + "=" * 60)
    print("ANALYSIS RESULTS")
    print("=" * 60)

    # Daily volume
    print("\nDAILY VOLUME (sample):")
    daily = pipeline.daily_volume()
    print(daily.head(10).to_string(index=False))

    # Chargeback rates
    print("\nCHARGEBACK RATES BY PAYMENT TYPE:")
    cb_rates = pipeline.chargeback_rates()
    print(cb_rates.to_string(index=False))

    # Payment failure rate
    print(f"\nPAYMENT FAILURE RATE: {pipeline.payment_failure_rate()}%")


if __name__ == "__main__":
    main()
