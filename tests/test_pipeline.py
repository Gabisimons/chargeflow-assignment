"""
Comprehensive unit tests for the DataPipeline.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import mock_open, patch, MagicMock

import pandas as pd
import pytest

from src.models import (
    Chargeback,
    ChargebackStatus,
    Currency,
    Order,
    PaymentMethodType,
    PaymentStatus,
    Transaction,
    TransactionStatus,
)
from src.pipeline import DataPipeline, ValidationResult


# =============================================================================
# FIXTURES - Raw Data
# =============================================================================

@pytest.fixture
def valid_transaction_data() -> Dict[str, Any]:
    """Return a valid transaction as a dictionary."""
    return {
        "transaction_id": "TXN_TEST001",
        "order_id": "ORD_TEST001",
        "timestamp": "2025-01-15T10:30:00",
        "amount": 150.00,
        "currency": "USD",
        "status": "completed",
        "payment_method": {
            "type": "credit_card",
            "provider": "Visa"
        },
        "error_code": None
    }


@pytest.fixture
def valid_transaction_failed_status_data() -> Dict[str, Any]:
    """Return a valid transaction with failed status."""
    return {
        "transaction_id": "TXN_TEST002",
        "order_id": "ORD_TEST002",
        "timestamp": "2025-01-15T11:00:00",
        "amount": 75.50,
        "currency": "EUR",
        "status": "failed",
        "payment_method": {
            "type": "debit_card",
            "provider": "Mastercard Debit"
        },
        "error_code": "INSUFFICIENT_FUNDS"
    }


@pytest.fixture
def invalid_transaction_negative_amount() -> Dict[str, Any]:
    """Return a transaction with negative amount (invalid)."""
    return {
        "transaction_id": "TXN_INVALID001",
        "order_id": "ORD_TEST003",
        "timestamp": "2025-01-15T12:00:00",
        "amount": -50.00,
        "currency": "USD",
        "status": "completed",
        "payment_method": {
            "type": "wallet",
            "provider": "PayPal"
        },
        "error_code": None
    }


@pytest.fixture
def invalid_transaction_missing_error_code() -> Dict[str, Any]:
    """Return a failed transaction without error_code (invalid)."""
    return {
        "transaction_id": "TXN_INVALID002",
        "order_id": "ORD_TEST004",
        "timestamp": "2025-01-15T13:00:00",
        "amount": 100.00,
        "currency": "GBP",
        "status": "failed",
        "payment_method": {
            "type": "credit_card",
            "provider": "Amex"
        },
        "error_code": None  # Should have error_code for failed status
    }


@pytest.fixture
def valid_order_data() -> Dict[str, Any]:
    """Return a valid order as a dictionary."""
    return {
        "order_id": "ORD_TEST001",
        "customer_id": "CUST_TEST001",
        "timestamp": "2025-01-15T10:25:00",
        "total_amount": 150.00,
        "currency": "USD",
        "items": [
            {"product_id": "PROD_001", "quantity": 2, "unit_price": 50.00},
            {"product_id": "PROD_002", "quantity": 1, "unit_price": 50.00}
        ],
        "payment_status": "paid"
    }


@pytest.fixture
def valid_order_data_second() -> Dict[str, Any]:
    """Return a second valid order."""
    return {
        "order_id": "ORD_TEST002",
        "customer_id": "CUST_TEST002",
        "timestamp": "2025-01-15T10:55:00",
        "total_amount": 75.50,
        "currency": "EUR",
        "items": [
            {"product_id": "PROD_003", "quantity": 1, "unit_price": 75.50}
        ],
        "payment_status": "failed"
    }


@pytest.fixture
def invalid_order_negative_amount() -> Dict[str, Any]:
    """Return an order with negative total_amount (invalid)."""
    return {
        "order_id": "ORD_INVALID001",
        "customer_id": "CUST_TEST003",
        "timestamp": "2025-01-15T14:00:00",
        "total_amount": -25.00,
        "currency": "USD",
        "items": [
            {"product_id": "PROD_004", "quantity": 1, "unit_price": 25.00}
        ],
        "payment_status": "refunded"
    }


@pytest.fixture
def valid_chargeback_data() -> Dict[str, Any]:
    """Return a valid chargeback as a dictionary."""
    return {
        "transaction_id": "TXN_TEST001",
        "dispute_date": "2025-01-20T09:00:00",
        "amount": 150.00,
        "reason_code": "4837",
        "status": "open",
        "resolution_date": ""
    }


@pytest.fixture
def valid_chargeback_resolved() -> Dict[str, Any]:
    """Return a valid resolved chargeback."""
    return {
        "transaction_id": "TXN_TEST003",
        "dispute_date": "2025-01-18T09:00:00",
        "amount": 200.00,
        "reason_code": "4853",
        "status": "won",
        "resolution_date": "2025-01-25T15:00:00"
    }


@pytest.fixture
def invalid_chargeback_negative_amount() -> Dict[str, Any]:
    """Return a chargeback with negative amount (invalid)."""
    return {
        "transaction_id": "TXN_TEST002",
        "dispute_date": "2025-01-21T10:00:00",
        "amount": -75.00,
        "reason_code": "13.1",
        "status": "pending_response",
        "resolution_date": ""
    }


@pytest.fixture
def invalid_chargeback_missing_resolution() -> Dict[str, Any]:
    """Return a won chargeback without resolution_date (invalid)."""
    return {
        "transaction_id": "TXN_TEST004",
        "dispute_date": "2025-01-19T11:00:00",
        "amount": 50.00,
        "reason_code": "75",
        "status": "won",
        "resolution_date": ""  # Required for won status
    }


# =============================================================================
# FIXTURES - CSV String Data
# =============================================================================

@pytest.fixture
def valid_chargebacks_csv() -> str:
    """Return valid chargebacks as CSV string."""
    return """transaction_id,dispute_date,amount,reason_code,status,resolution_date
TXN_TEST001,2025-01-20T09:00:00,150.00,4837,open,
TXN_TEST003,2025-01-18T09:00:00,200.00,4853,won,2025-01-25T15:00:00"""


@pytest.fixture
def mixed_chargebacks_csv() -> str:
    """Return chargebacks with valid and invalid records."""
    return """transaction_id,dispute_date,amount,reason_code,status,resolution_date
TXN_TEST001,2025-01-20T09:00:00,150.00,4837,open,
TXN_INVALID,2025-01-21T10:00:00,-75.00,13.1,pending_response,"""


# =============================================================================
# FIXTURES - Pipeline with Mocked Data
# =============================================================================

@pytest.fixture
def mock_pipeline(tmp_path: Path) -> DataPipeline:
    """Create a pipeline with a temporary data directory."""
    return DataPipeline(tmp_path)


@pytest.fixture
def pipeline_with_known_data(
    valid_transaction_data,
    valid_transaction_failed_status_data,
    valid_order_data,
    valid_order_data_second,
    valid_chargeback_data
) -> DataPipeline:
    """Create a pipeline and inject known test data."""
    pipeline = DataPipeline(Path("/fake/path"))

    # Manually populate validation results
    pipeline.transactions_result.valid_records = [
        Transaction.model_validate(valid_transaction_data),
        Transaction.model_validate(valid_transaction_failed_status_data),
    ]
    pipeline.orders_result.valid_records = [
        Order.model_validate(valid_order_data),
        Order.model_validate(valid_order_data_second),
    ]
    pipeline.chargebacks_result.valid_records = [
        Chargeback.model_validate(valid_chargeback_data),
    ]

    return pipeline


# =============================================================================
# TEST: Validation Logic
# =============================================================================

class TestValidationLogic:
    """Tests for the validation logic of the pipeline."""

    def test_valid_transaction_passes(self, valid_transaction_data):
        """Valid transaction should be accepted."""
        txn = Transaction.model_validate(valid_transaction_data)
        assert txn.transaction_id == "TXN_TEST001"
        assert txn.amount == 150.00
        assert txn.currency == Currency.USD
        assert txn.status == TransactionStatus.COMPLETED

    def test_valid_transaction_with_failed_status(self, valid_transaction_failed_status_data):
        """Transaction with failed status and error_code should pass."""
        txn = Transaction.model_validate(valid_transaction_failed_status_data)
        assert txn.status == TransactionStatus.FAILED
        assert txn.error_code == "INSUFFICIENT_FUNDS"

    def test_invalid_transaction_negative_amount_fails(self, invalid_transaction_negative_amount):
        """Transaction with negative amount should fail validation."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            Transaction.model_validate(invalid_transaction_negative_amount)

        errors = exc_info.value.errors()
        assert any("amount must be strictly positive" in str(e["msg"]) for e in errors)

    def test_invalid_transaction_missing_error_code_fails(self, invalid_transaction_missing_error_code):
        """Failed transaction without error_code should fail validation."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            Transaction.model_validate(invalid_transaction_missing_error_code)

        errors = exc_info.value.errors()
        assert any("error_code is required" in str(e["msg"]) for e in errors)

    def test_valid_order_passes(self, valid_order_data):
        """Valid order should be accepted."""
        order = Order.model_validate(valid_order_data)
        assert order.order_id == "ORD_TEST001"
        assert order.total_amount == 150.00
        assert len(order.items) == 2

    def test_invalid_order_negative_amount_fails(self, invalid_order_negative_amount):
        """Order with negative total_amount should fail validation."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            Order.model_validate(invalid_order_negative_amount)

        errors = exc_info.value.errors()
        assert any("total_amount must be strictly positive" in str(e["msg"]) for e in errors)

    def test_valid_chargeback_passes(self, valid_chargeback_data):
        """Valid chargeback should be accepted."""
        cb = Chargeback.model_validate(valid_chargeback_data)
        assert cb.transaction_id == "TXN_TEST001"
        assert cb.amount == 150.00
        assert cb.status == ChargebackStatus.OPEN

    def test_valid_chargeback_resolved_passes(self, valid_chargeback_resolved):
        """Resolved chargeback with resolution_date should pass."""
        cb = Chargeback.model_validate(valid_chargeback_resolved)
        assert cb.status == ChargebackStatus.WON
        assert cb.resolution_date is not None

    def test_invalid_chargeback_negative_amount_fails(self, invalid_chargeback_negative_amount):
        """Chargeback with negative amount should fail validation."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            Chargeback.model_validate(invalid_chargeback_negative_amount)

        errors = exc_info.value.errors()
        assert any("amount must be strictly positive" in str(e["msg"]) for e in errors)

    def test_invalid_chargeback_missing_resolution_fails(self, invalid_chargeback_missing_resolution):
        """Won chargeback without resolution_date should fail validation."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            Chargeback.model_validate(invalid_chargeback_missing_resolution)

        errors = exc_info.value.errors()
        assert any("resolution_date is required" in str(e["msg"]) for e in errors)

    def test_pipeline_separates_valid_and_invalid(
        self,
        mock_pipeline,
        valid_transaction_data,
        invalid_transaction_negative_amount
    ):
        """Pipeline should separate valid and invalid records."""
        raw_records = [valid_transaction_data, invalid_transaction_negative_amount]

        result = mock_pipeline._validate_records(raw_records, Transaction, "Test")

        assert len(result.valid_records) == 1
        assert len(result.failed_records) == 1
        assert result.valid_records[0].transaction_id == "TXN_TEST001"
        assert result.failed_records[0]["index"] == 1

    def test_pipeline_does_not_crash_on_invalid_data(
        self,
        mock_pipeline,
        invalid_transaction_negative_amount,
        invalid_transaction_missing_error_code
    ):
        """Pipeline should not crash when all records are invalid."""
        raw_records = [
            invalid_transaction_negative_amount,
            invalid_transaction_missing_error_code
        ]

        result = mock_pipeline._validate_records(raw_records, Transaction, "Test")

        assert len(result.valid_records) == 0
        assert len(result.failed_records) == 2
        assert result.success_rate == 0.0


# =============================================================================
# TEST: Transform and Merge
# =============================================================================

class TestTransformMerge:
    """Tests for the transformation and merging logic."""

    def test_transform_creates_dataframes(self, pipeline_with_known_data):
        """Transform should create DataFrames from valid records."""
        pipeline = pipeline_with_known_data
        pipeline.transform()

        assert len(pipeline.transactions_df) == 2
        assert len(pipeline.orders_df) == 2
        assert len(pipeline.chargebacks_df) == 1

    def test_transform_merges_transactions_with_orders(self, pipeline_with_known_data):
        """Transactions should merge with matching orders on order_id."""
        pipeline = pipeline_with_known_data
        pipeline.transform()

        # Check merged DataFrame
        assert len(pipeline.merged_df) == 2

        # Find the row for TXN_TEST001 which should merge with ORD_TEST001
        txn1_row = pipeline.merged_df[
            pipeline.merged_df["transaction_id"] == "TXN_TEST001"
        ].iloc[0]

        assert txn1_row["order_id"] == "ORD_TEST001"
        assert txn1_row["customer_id"] == "CUST_TEST001"
        assert txn1_row["txn_amount"] == 150.00
        assert txn1_row["order_total_amount"] == 150.00

    def test_transform_merges_with_chargebacks(self, pipeline_with_known_data):
        """Chargebacks should merge with transactions on transaction_id."""
        pipeline = pipeline_with_known_data
        pipeline.transform()

        # TXN_TEST001 has a chargeback
        txn1_row = pipeline.merged_df[
            pipeline.merged_df["transaction_id"] == "TXN_TEST001"
        ].iloc[0]

        assert txn1_row["cb_amount"] == 150.00
        assert txn1_row["cb_reason_code"] == "4837"

        # TXN_TEST002 has no chargeback
        txn2_row = pipeline.merged_df[
            pipeline.merged_df["transaction_id"] == "TXN_TEST002"
        ].iloc[0]

        assert pd.isna(txn2_row["cb_amount"])

    def test_transform_handles_orphaned_transactions(self):
        """Transactions without matching orders should still be in merged_df."""
        pipeline = DataPipeline(Path("/fake/path"))

        # Transaction with non-existent order
        orphan_txn = {
            "transaction_id": "TXN_ORPHAN",
            "order_id": "ORD_NONEXISTENT",
            "timestamp": "2025-01-15T10:30:00",
            "amount": 100.00,
            "currency": "USD",
            "status": "completed",
            "payment_method": {"type": "credit_card", "provider": "Visa"},
            "error_code": None
        }

        pipeline.transactions_result.valid_records = [
            Transaction.model_validate(orphan_txn)
        ]
        pipeline.orders_result.valid_records = []
        pipeline.chargebacks_result.valid_records = []

        pipeline.transform()

        assert len(pipeline.merged_df) == 1
        assert pipeline.merged_df.iloc[0]["transaction_id"] == "TXN_ORPHAN"
        assert pd.isna(pipeline.merged_df.iloc[0]["customer_id"])

    def test_transform_normalizes_amounts_to_float(self, pipeline_with_known_data):
        """Amount columns should be normalized to float type."""
        pipeline = pipeline_with_known_data
        pipeline.transform()

        assert pipeline.merged_df["txn_amount"].dtype == float
        assert pipeline.merged_df["order_total_amount"].dtype == float

    def test_transform_adds_date_column(self, pipeline_with_known_data):
        """Transform should add txn_date column for daily aggregations."""
        pipeline = pipeline_with_known_data
        pipeline.transform()

        assert "txn_date" in pipeline.merged_df.columns
        assert pipeline.merged_df.iloc[0]["txn_date"] == datetime(2025, 1, 15).date()


# =============================================================================
# TEST: Metrics Calculations
# =============================================================================

class TestMetrics:
    """Tests for the analysis metrics."""

    def test_daily_volume_calculation(self):
        """Daily volume should sum transaction amounts by date."""
        pipeline = DataPipeline(Path("/fake/path"))

        # Create 2 transactions of $100 each on the same day
        txn_data = [
            {
                "transaction_id": f"TXN_{i}",
                "order_id": f"ORD_{i}",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            }
            for i in range(2)
        ]

        pipeline.transactions_result.valid_records = [
            Transaction.model_validate(t) for t in txn_data
        ]
        pipeline.orders_result.valid_records = []
        pipeline.chargebacks_result.valid_records = []

        pipeline.transform()
        daily = pipeline.daily_volume()

        assert len(daily) == 1
        assert daily.iloc[0]["total_amount"] == 200.00
        assert daily.iloc[0]["date"] == datetime(2025, 1, 15).date()

    def test_daily_volume_multiple_days(self):
        """Daily volume should correctly group by different dates."""
        pipeline = DataPipeline(Path("/fake/path"))

        txn_data = [
            {
                "transaction_id": "TXN_1",
                "order_id": "ORD_1",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            },
            {
                "transaction_id": "TXN_2",
                "order_id": "ORD_2",
                "timestamp": "2025-01-16T10:00:00",
                "amount": 150.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            },
            {
                "transaction_id": "TXN_3",
                "order_id": "ORD_3",
                "timestamp": "2025-01-15T15:00:00",
                "amount": 50.00,
                "currency": "EUR",
                "status": "pending",
                "payment_method": {"type": "wallet", "provider": "PayPal"},
                "error_code": None
            },
        ]

        pipeline.transactions_result.valid_records = [
            Transaction.model_validate(t) for t in txn_data
        ]
        pipeline.orders_result.valid_records = []
        pipeline.chargebacks_result.valid_records = []

        pipeline.transform()
        daily = pipeline.daily_volume()

        assert len(daily) == 2

        day1 = daily[daily["date"] == datetime(2025, 1, 15).date()]
        day2 = daily[daily["date"] == datetime(2025, 1, 16).date()]

        assert day1.iloc[0]["total_amount"] == 150.00  # 100 + 50
        assert day2.iloc[0]["total_amount"] == 150.00

    def test_chargeback_rates_by_payment_method(self):
        """Chargeback rate should be calculated per payment method."""
        pipeline = DataPipeline(Path("/fake/path"))

        # 2 credit card transactions: $200 total, $50 chargeback = 25%
        # 1 wallet transaction: $100 total, $0 chargeback = 0%
        txn_data = [
            {
                "transaction_id": "TXN_CC1",
                "order_id": "ORD_1",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            },
            {
                "transaction_id": "TXN_CC2",
                "order_id": "ORD_2",
                "timestamp": "2025-01-15T11:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Mastercard"},
                "error_code": None
            },
            {
                "transaction_id": "TXN_W1",
                "order_id": "ORD_3",
                "timestamp": "2025-01-15T12:00:00",
                "amount": 100.00,
                "currency": "EUR",
                "status": "completed",
                "payment_method": {"type": "wallet", "provider": "PayPal"},
                "error_code": None
            },
        ]

        cb_data = [
            {
                "transaction_id": "TXN_CC1",
                "dispute_date": "2025-01-20T09:00:00",
                "amount": 50.00,
                "reason_code": "4837",
                "status": "open",
                "resolution_date": ""
            }
        ]

        pipeline.transactions_result.valid_records = [
            Transaction.model_validate(t) for t in txn_data
        ]
        pipeline.orders_result.valid_records = []
        pipeline.chargebacks_result.valid_records = [
            Chargeback.model_validate(cb) for cb in cb_data
        ]

        pipeline.transform()
        cb_rates = pipeline.chargeback_rates()

        cc_rate = cb_rates[cb_rates["payment_type"] == "credit_card"]
        wallet_rate = cb_rates[cb_rates["payment_type"] == "wallet"]

        assert cc_rate.iloc[0]["chargeback_rate"] == 25.00
        assert wallet_rate.iloc[0]["chargeback_rate"] == 0.00

    def test_payment_failure_rate(self):
        """Failure rate should be percentage of failed transactions."""
        pipeline = DataPipeline(Path("/fake/path"))

        # 3 transactions: 1 failed, 2 completed = 33.33%
        txn_data = [
            {
                "transaction_id": "TXN_1",
                "order_id": "ORD_1",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            },
            {
                "transaction_id": "TXN_2",
                "order_id": "ORD_2",
                "timestamp": "2025-01-15T11:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "failed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": "CARD_DECLINED"
            },
            {
                "transaction_id": "TXN_3",
                "order_id": "ORD_3",
                "timestamp": "2025-01-15T12:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "wallet", "provider": "PayPal"},
                "error_code": None
            },
        ]

        pipeline.transactions_result.valid_records = [
            Transaction.model_validate(t) for t in txn_data
        ]
        pipeline.orders_result.valid_records = []
        pipeline.chargebacks_result.valid_records = []

        pipeline.transform()
        rate = pipeline.payment_failure_rate()

        assert rate == 33.33

    def test_payment_failure_rate_all_successful(self):
        """Failure rate should be 0% when all transactions succeed."""
        pipeline = DataPipeline(Path("/fake/path"))

        txn_data = [
            {
                "transaction_id": f"TXN_{i}",
                "order_id": f"ORD_{i}",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            }
            for i in range(5)
        ]

        pipeline.transactions_result.valid_records = [
            Transaction.model_validate(t) for t in txn_data
        ]
        pipeline.orders_result.valid_records = []
        pipeline.chargebacks_result.valid_records = []

        pipeline.transform()
        rate = pipeline.payment_failure_rate()

        assert rate == 0.0

    def test_payment_failure_rate_all_failed(self):
        """Failure rate should be 100% when all transactions fail."""
        pipeline = DataPipeline(Path("/fake/path"))

        txn_data = [
            {
                "transaction_id": f"TXN_{i}",
                "order_id": f"ORD_{i}",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "failed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": "CARD_DECLINED"
            }
            for i in range(3)
        ]

        pipeline.transactions_result.valid_records = [
            Transaction.model_validate(t) for t in txn_data
        ]
        pipeline.orders_result.valid_records = []
        pipeline.chargebacks_result.valid_records = []

        pipeline.transform()
        rate = pipeline.payment_failure_rate()

        assert rate == 100.0

    def test_metrics_with_empty_data(self):
        """Metrics should handle empty DataFrames gracefully."""
        pipeline = DataPipeline(Path("/fake/path"))
        pipeline.transactions_result.valid_records = []
        pipeline.orders_result.valid_records = []
        pipeline.chargebacks_result.valid_records = []

        pipeline.transform()

        daily = pipeline.daily_volume()
        cb_rates = pipeline.chargeback_rates()
        failure_rate = pipeline.payment_failure_rate()

        assert daily.empty
        assert cb_rates.empty
        assert failure_rate == 0.0


# =============================================================================
# TEST: File Loading with Mocks
# =============================================================================

class TestFileLoadingMocked:
    """Tests for file loading with mocked file system."""

    def test_ingest_with_mocked_files(self, tmp_path):
        """Test full ingestion with actual temp files."""
        # Create test data files
        transactions = [
            {
                "transaction_id": "TXN_001",
                "order_id": "ORD_001",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            }
        ]

        orders = [
            {
                "order_id": "ORD_001",
                "customer_id": "CUST_001",
                "timestamp": "2025-01-15T09:55:00",
                "total_amount": 100.00,
                "currency": "USD",
                "items": [{"product_id": "PROD_1", "quantity": 1, "unit_price": 100.00}],
                "payment_status": "paid"
            }
        ]

        chargebacks_csv = "transaction_id,dispute_date,amount,reason_code,status,resolution_date\n"
        chargebacks_csv += "TXN_001,2025-01-20T09:00:00,100.00,4837,open,\n"

        # Write files
        (tmp_path / "transactions.json").write_text(json.dumps(transactions))
        (tmp_path / "orders.json").write_text(json.dumps(orders))
        (tmp_path / "chargebacks.csv").write_text(chargebacks_csv)

        # Run pipeline
        pipeline = DataPipeline(tmp_path)
        pipeline.ingest_and_validate()

        assert len(pipeline.transactions_result.valid_records) == 1
        assert len(pipeline.orders_result.valid_records) == 1
        assert len(pipeline.chargebacks_result.valid_records) == 1

    def test_ingest_handles_invalid_records_in_files(self, tmp_path):
        """Test that ingestion separates valid and invalid records from files."""
        transactions = [
            {
                "transaction_id": "TXN_VALID",
                "order_id": "ORD_001",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            },
            {
                "transaction_id": "TXN_INVALID",
                "order_id": "ORD_002",
                "timestamp": "2025-01-15T11:00:00",
                "amount": -50.00,  # Invalid: negative
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "wallet", "provider": "PayPal"},
                "error_code": None
            }
        ]

        orders = []
        chargebacks_csv = "transaction_id,dispute_date,amount,reason_code,status,resolution_date\n"

        (tmp_path / "transactions.json").write_text(json.dumps(transactions))
        (tmp_path / "orders.json").write_text(json.dumps(orders))
        (tmp_path / "chargebacks.csv").write_text(chargebacks_csv)

        pipeline = DataPipeline(tmp_path)
        pipeline.ingest_and_validate()

        assert len(pipeline.transactions_result.valid_records) == 1
        assert len(pipeline.transactions_result.failed_records) == 1
        assert pipeline.transactions_result.valid_records[0].transaction_id == "TXN_VALID"


# =============================================================================
# TEST: Validation Summary and Failed Records
# =============================================================================

class TestValidationSummary:
    """Tests for validation summary and failed records retrieval."""

    def test_get_validation_summary(self, pipeline_with_known_data):
        """Should return correct validation statistics."""
        pipeline = pipeline_with_known_data
        summary = pipeline.get_validation_summary()

        assert summary["transactions"]["valid"] == 2
        assert summary["transactions"]["failed"] == 0
        assert summary["transactions"]["success_rate"] == 100.0

        assert summary["orders"]["valid"] == 2
        assert summary["chargebacks"]["valid"] == 1

    def test_get_failed_records(self, mock_pipeline, invalid_transaction_negative_amount):
        """Should return failed records for debugging."""
        raw_records = [invalid_transaction_negative_amount]
        mock_pipeline._validate_records(raw_records, Transaction, "Transactions")

        # Manually add to the pipeline's result
        mock_pipeline.transactions_result = mock_pipeline._validate_records(
            raw_records, Transaction, "Transactions"
        )

        failed = mock_pipeline.get_failed_records()

        assert len(failed["transactions"]) == 1
        assert failed["transactions"][0]["record"]["transaction_id"] == "TXN_INVALID001"


# =============================================================================
# TEST: Pipeline Run Method
# =============================================================================

class TestPipelineRun:
    """Tests for the full pipeline run."""

    def test_run_chains_ingest_and_transform(self, tmp_path):
        """Run method should execute ingest and transform."""
        transactions = [
            {
                "transaction_id": "TXN_001",
                "order_id": "ORD_001",
                "timestamp": "2025-01-15T10:00:00",
                "amount": 100.00,
                "currency": "USD",
                "status": "completed",
                "payment_method": {"type": "credit_card", "provider": "Visa"},
                "error_code": None
            }
        ]

        orders = [
            {
                "order_id": "ORD_001",
                "customer_id": "CUST_001",
                "timestamp": "2025-01-15T09:55:00",
                "total_amount": 100.00,
                "currency": "USD",
                "items": [{"product_id": "PROD_1", "quantity": 1, "unit_price": 100.00}],
                "payment_status": "paid"
            }
        ]

        (tmp_path / "transactions.json").write_text(json.dumps(transactions))
        (tmp_path / "orders.json").write_text(json.dumps(orders))
        (tmp_path / "chargebacks.csv").write_text(
            "transaction_id,dispute_date,amount,reason_code,status,resolution_date\n"
        )

        pipeline = DataPipeline(tmp_path).run()

        # Check that both ingestion and transformation happened
        assert len(pipeline.transactions_result.valid_records) == 1
        assert not pipeline.merged_df.empty
        assert pipeline.merged_df.iloc[0]["transaction_id"] == "TXN_001"
