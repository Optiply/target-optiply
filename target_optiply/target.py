"""Optiply target class."""
import os
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from target_optiply.sinks import (
    BaseOptiplySink,
    ProductsSink,
    SupplierSink,
    SupplierProductSink,
    BuyOrderSink,
    BuyOrderLineSink,
    SellOrderSink,
    SellOrderLineSink,
    PromotionSink,
    PromotionProductSink,
)

from target_hotglue.target import TargetHotglue
from typing import Callable, Dict, List, Optional, Set, Tuple, Type, Union
from pathlib import Path, PurePath

DEFAULT_WRITE_WORKERS = 1
MAX_WRITE_WORKERS = 5


def get_write_workers_from_env() -> int:
    """Number of parallel Optiply write workers, from env var OPTIPLY_WRITE_WORKERS (1-5)."""
    raw = os.environ.get("OPTIPLY_WRITE_WORKERS")
    if raw is None or raw.strip() == "":
        return DEFAULT_WRITE_WORKERS
    try:
        workers = int(raw)
    except ValueError:
        print(f"WARNING -- invalid OPTIPLY_WRITE_WORKERS={raw!r}, using {DEFAULT_WRITE_WORKERS}")
        return DEFAULT_WRITE_WORKERS
    return max(1, min(workers, MAX_WRITE_WORKERS))




class TargetOptiply(TargetHotglue):
    """Target for Optiply."""

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        self.logger.info(f"Target config file: {self.config_file}")
        super().__init__(config, parse_env_config, validate_config)
        
        # Log the config structure after initialization
        self.logger.info(f"Target config keys: {list(self._config.keys())}")
        
        # Log the access_token being used
        if "access_token" in self._config:
            access_token = self._config["access_token"]
            # Log first few characters of token for security
            if access_token and len(access_token) > 8:
                masked_token = access_token[:4] + "*" * (len(access_token) - 8) + access_token[-4:]
                self.logger.info(f"Using access_token: {masked_token}")
            else:
                self.logger.info(f"Using access_token: {access_token}")
        else:
            self.logger.warning("No access_token found in config")
        
        # Also check for "token" key for backward compatibility
        if "token" in self._config:
            token = self._config["token"]
            # Log first few characters of token for security
            if token and len(token) > 8:
                masked_token = token[:4] + "*" * (len(token) - 8) + token[-4:]
                self.logger.info(f"Using token: {masked_token}")
            else:
                self.logger.info(f"Using token: {token}")
        else:
            self.logger.info("No token key found in config (using access_token instead)")

        # Records of one stream are written by up to OPTIPLY_WRITE_WORKERS threads; 1 keeps the
        # sequential write path.
        self._write_workers = get_write_workers_from_env()
        self.logger.info(f"OPTIPLY_WRITE_WORKERS: {self._write_workers}")
        self._write_pool = (
            ThreadPoolExecutor(max_workers=self._write_workers) if self._write_workers > 1 else None
        )
        self._pending_writes: Dict[Future, Set] = {}
        self._pending_sink = None

    def claim_writes(self, sink) -> None:
        """Finish another stream's in-flight writes before this sink starts preprocessing.

        Later streams read results of earlier ones (e.g. promotion IDs), so streams never overlap.
        """
        if self._pending_sink is not sink:
            self.flush_writes()
            self._pending_sink = sink

    def submit_write(self, sink, keys: Set, write: Callable[[], None]) -> None:
        """Run one record write, in parallel when write workers are enabled.

        A record sharing a key (hash or Optiply ID) with an in-flight write waits for all in-flight
        writes first, so duplicates are still detected and updates to one ID keep their order.
        """
        if self._write_pool is None:
            write()
            return
        self.claim_writes(sink)
        if any(not keys.isdisjoint(pending) for pending in self._pending_writes.values()):
            self.flush_writes()
        while len(self._pending_writes) >= 2 * self._write_workers:
            done, _ = wait(self._pending_writes, return_when=FIRST_COMPLETED)
            self._collect_writes(done)
        self._pending_writes[self._write_pool.submit(write)] = keys

    def flush_writes(self) -> None:
        """Wait for every in-flight write; re-raise the first worker error."""
        if self._pending_writes:
            wait(self._pending_writes)
            self._collect_writes(list(self._pending_writes))

    def _collect_writes(self, done) -> None:
        error = None
        for future in done:
            self._pending_writes.pop(future, None)
            if error is None:
                error = future.exception()
        if error is not None:
            raise error

    def drain_all(self, is_endofpipe: bool = False) -> None:
        """Finish in-flight writes before the SDK snapshots and emits target state."""
        self.flush_writes()
        super().drain_all(is_endofpipe)
        if is_endofpipe and self._write_pool is not None:
            self._write_pool.shutdown()

    def _save_failed_job_state(self):
        """Let in-flight writes record their outcome before the failed-job state is saved."""
        if self._pending_writes:
            wait(self._pending_writes)
            self._pending_writes.clear()
        super()._save_failed_job_state()

    SINK_TYPES = [
        BaseOptiplySink,
        ProductsSink,
        SupplierSink,
        SupplierProductSink,
        BuyOrderSink,
        BuyOrderLineSink,
        SellOrderSink,
        SellOrderLineSink,
        PromotionSink,
        PromotionProductSink,
    ]
    MAX_PARALLELISM = 10
    name = "target-optiply"

    def get_sink_class(self, stream_name: str):
        """Get sink class for the given stream name."""
        # Map stream names to sink classes
        sink_map = {
            "BuyOrders": BuyOrderSink,
            "Products": ProductsSink,
            "Suppliers": SupplierSink,
            "SupplierProducts": SupplierProductSink,
            "BuyOrderLines": BuyOrderLineSink,
            "SellOrders": SellOrderSink,
            "SellOrderLines": SellOrderLineSink,
            "Promotions": PromotionSink,
            "PromotionProducts": PromotionProductSink,
            "promotions": PromotionSink,
            "promotionProducts": PromotionProductSink,
        }
        
        return sink_map.get(stream_name, BaseOptiplySink)

    def _simplify_records_dict(self, records_dict: dict) -> dict:
        """Helper method to simplify records dictionary to show only counts."""
        simplified_dict = {}
        for stream_name, records in records_dict.items():
            if isinstance(records, list):
                # ETL-skipped records are stored with success and existing; count them only as existing.
                existing_count = sum(1 for record in records if record.get("existing", False))
                success_count = sum(
                    1 for record in records
                    if record.get("success", False) and not record.get("existing", False)
                )
                fail_count = len(records) - success_count - existing_count

                simplified_dict[stream_name] = {
                    "total": len(records),
                    "success": success_count,
                    "failed": fail_count,
                    "existing": existing_count,
                }
            else:
                # Keep as is if not a list
                simplified_dict[stream_name] = records
        
        return simplified_dict

    def get_state(self) -> dict:
        """Override to provide simplified state with only counts."""
        # Get the original state from parent
        original_state = super().get_state()
        
        # Simplify the bookmarks to only show counts
        if "bookmarks" in original_state:
            original_state["bookmarks"] = self._simplify_records_dict(original_state["bookmarks"])
        
        return original_state

    def _get_export_summary(self) -> dict:
        """Override to provide simplified export summary."""
        # Get the original export summary from parent
        original_summary = super()._get_export_summary()
        
        # Simplify the export details to only show counts
        if "exportDetails" in original_summary:
            original_summary["exportDetails"] = self._simplify_records_dict(original_summary["exportDetails"])
        
        return original_summary

    def _get_export_details(self) -> dict:
        """Override to provide simplified export details."""
        # Get the original export details from parent
        original_details = super()._get_export_details()
        
        # Simplify the export details to only show counts
        return self._simplify_records_dict(original_details)

    def _get_metrics(self) -> dict:
        """Override to provide simplified metrics."""
        # Get the original metrics from parent
        original_metrics = super()._get_metrics()
        
        # Simplify the export details to only show counts
        if "exportDetails" in original_metrics:
            original_metrics["exportDetails"] = self._simplify_records_dict(original_metrics["exportDetails"])
        
        return original_metrics


if __name__ == "__main__":
    TargetOptiply.cli()
