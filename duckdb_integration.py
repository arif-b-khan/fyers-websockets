import os
import atexit
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb


class DuckDBManager:
    """Encapsulates DuckDB operations for order book storage and analytics."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._lock = threading.RLock()
        self.db_path = Path(db_path or os.getenv("DUCKDB_PATH", "data/market_data.duckdb"))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.db_path), read_only=False)
        self._conn.execute("PRAGMA threads=2")
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_snapshots (
                    snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    timestamp_ms BIGINT,
                    total_bid_qty BIGINT,
                    total_sell_qty BIGINT,
                    imbalance_10 DOUBLE,
                    imbalance_20 DOUBLE,
                    imbalance_50 DOUBLE,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_levels (
                    level_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    snapshot_id BIGINT NOT NULL,
                    side TEXT NOT NULL,
                    level INTEGER NOT NULL,
                    price DOUBLE,
                    quantity BIGINT,
                    orders BIGINT,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    @contextmanager
    def _transaction(self) -> Iterable[None]:
        with self._lock:
            try:
                self._conn.execute("BEGIN TRANSACTION")
                yield
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def store_market_depth(self, depth_payload: Dict[str, Any]) -> Optional[int]:
        """Persist a market depth snapshot and its levels."""
        if not depth_payload:
            return None

        balances = {
            "imbalance_10": self._extract_imbalance(depth_payload.get("imbalance_10")),
            "imbalance_20": self._extract_imbalance(depth_payload.get("imbalance_20")),
            "imbalance_50": self._extract_imbalance(depth_payload.get("imbalance_50")),
        }

        try:
            with self._transaction():
                snapshot_id = self._conn.execute(
                    """
                    INSERT INTO market_snapshots (
                        ticker, timestamp_ms, total_bid_qty, total_sell_qty,
                        imbalance_10, imbalance_20, imbalance_50
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    RETURNING snapshot_id
                    """,
                    [
                        depth_payload.get("ticker"),
                        depth_payload.get("timestamp"),
                        depth_payload.get("total_bid_qty"),
                        depth_payload.get("total_sell_qty"),
                        balances["imbalance_10"],
                        balances["imbalance_20"],
                        balances["imbalance_50"],
                    ],
                ).fetchone()[0]

                self._insert_levels(snapshot_id, "bid", depth_payload.get("bids", []))
                self._insert_levels(snapshot_id, "ask", depth_payload.get("asks", []))
                return snapshot_id
        except Exception as exc:
            print(f"DuckDB storage error: {exc}")
            return None

    def _insert_levels(self, snapshot_id: int, side: str, levels: List[Dict[str, Any]]) -> None:
        if not levels:
            return

        params = [
            (
                snapshot_id,
                side,
                level.get("level"),
                level.get("price"),
                level.get("quantity"),
                level.get("orders"),
            )
            for level in levels
        ]

        self._conn.executemany(
            """
            INSERT INTO market_levels (
                snapshot_id, side, level, price, quantity, orders
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            params,
        )

    def fetch_snapshots(self, ticker: str, limit: int = 25) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT snapshot_id, ticker, timestamp_ms, total_bid_qty, total_sell_qty,
                       imbalance_10, imbalance_20, imbalance_50, created_at
                FROM market_snapshots
                WHERE ticker = ?
                ORDER BY timestamp_ms DESC NULLS LAST
                LIMIT ?
                """,
                [ticker, limit],
            ).fetchall()

        return [
            {
                "snapshot_id": row[0],
                "ticker": row[1],
                "timestamp_ms": row[2],
                "total_bid_qty": row[3],
                "total_sell_qty": row[4],
                "imbalance_10": row[5],
                "imbalance_20": row[6],
                "imbalance_50": row[7],
                "created_at": row[8],
            }
            for row in rows
        ]

    def fetch_levels(self, snapshot_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT side, level, price, quantity, orders, created_at
                FROM market_levels
                WHERE snapshot_id = ?
                ORDER BY side, level
                """,
                [snapshot_id],
            ).fetchall()

        return [
            {
                "side": row[0],
                "level": row[1],
                "price": row[2],
                "quantity": row[3],
                "orders": row[4],
                "created_at": row[5],
            }
            for row in rows
        ]

    def update_snapshot_totals(
        self,
        snapshot_id: int,
        total_bid_qty: Optional[int] = None,
        total_sell_qty: Optional[int] = None,
    ) -> bool:
        if total_bid_qty is None and total_sell_qty is None:
            return False

        assignments: List[str] = []
        params: List[Any] = []
        if total_bid_qty is not None:
            assignments.append("total_bid_qty = ?")
            params.append(total_bid_qty)
        if total_sell_qty is not None:
            assignments.append("total_sell_qty = ?")
            params.append(total_sell_qty)
        params.append(snapshot_id)

        with self._transaction():
            cursor = self._conn.execute(
                f"""
                UPDATE market_snapshots
                SET {', '.join(assignments)}
                WHERE snapshot_id = ?
                """,
                params,
            )
        return bool(cursor.rowcount and cursor.rowcount > 0)

    def delete_snapshot(self, snapshot_id: int) -> bool:
        with self._transaction():
            self._conn.execute(
                "DELETE FROM market_levels WHERE snapshot_id = ?",
                [snapshot_id],
            )
            cursor = self._conn.execute(
                "DELETE FROM market_snapshots WHERE snapshot_id = ?",
                [snapshot_id],
            )
        return bool(cursor.rowcount and cursor.rowcount > 0)

    def ingest_csv(self, file_path: str, table_name: str, replace: bool = False) -> None:
        self._ingest_file(file_path, table_name, "csv", replace)

    def ingest_json(self, file_path: str, table_name: str, replace: bool = False) -> None:
        self._ingest_file(file_path, table_name, "json", replace)

    def ingest_parquet(self, file_path: str, table_name: str, replace: bool = False) -> None:
        self._ingest_file(file_path, table_name, "parquet", replace)

    def _ingest_file(self, file_path: str, table_name: str, fmt: str, replace: bool) -> None:
        fmt = fmt.lower()
        if fmt not in {"csv", "json", "parquet"}:
            raise ValueError("Unsupported ingestion format")

        target = self._sanitize_identifier(table_name)
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        with self._lock:
            reader = self._reader_for(fmt)
            if replace:
                self._conn.execute(
                    f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {reader}(?)",
                    [str(path)],
                )
            else:
                self._conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM {reader}(?) LIMIT 0",
                    [str(path)],
                )
                self._conn.execute(
                    f"INSERT INTO {target} SELECT * FROM {reader}(?)",
                    [str(path)],
                )

    def execute_query(self, query: str, params: Optional[Iterable[Any]] = None) -> List[Dict[str, Any]]:
        params = list(params or [])
        with self._lock:
            try:
                result = self._conn.execute(query, params)
                columns = [desc[0] for desc in result.description]
                return [dict(zip(columns, row)) for row in result.fetchall()]
            except Exception as exc:
                print(f"DuckDB query error: {exc}")
                return []

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _extract_imbalance(self, payload: Optional[Dict[str, Any]]) -> Optional[float]:
        if not payload:
            return None
        return payload.get("imbalance")

    def _sanitize_identifier(self, value: str) -> str:
        if not value or any(ch for ch in value if not (ch.isalnum() or ch == "_")):
            raise ValueError("Invalid identifier")
        return value

    def _reader_for(self, fmt: str) -> str:
        if fmt == "csv":
            return "read_csv_auto"
        if fmt == "json":
            return "read_json_auto"
        return "read_parquet"


duckdb_manager = DuckDBManager()
atexit.register(duckdb_manager.close)
