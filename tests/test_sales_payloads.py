import importlib.util
import json
import logging
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Tuple
from unittest.mock import Mock, patch


def _module(name: str, **attributes: object) -> ModuleType:
    module = ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


def _load_sink_classes() -> Tuple[Any, Any, Any]:
    singer_sdk = _module("singer_sdk")
    setattr(singer_sdk, "__path__", [])
    exceptions = _module(
        "singer_sdk.exceptions",
        FatalAPIError=type("FatalAPIError", (Exception,), {}),
        RetriableAPIError=type("RetriableAPIError", (Exception,), {}),
    )
    plugin_base = _module("singer_sdk.plugin_base", PluginBase=object)
    setattr(singer_sdk, "exceptions", exceptions)
    setattr(singer_sdk, "plugin_base", plugin_base)

    target_hotglue = _module("target_hotglue")
    setattr(target_hotglue, "__path__", [])
    target_hotglue_client = _module("target_hotglue.client", HotglueSink=object)
    setattr(target_hotglue, "client", target_hotglue_client)

    target_optiply = _module("target_optiply")
    setattr(target_optiply, "__path__", [])
    target_optiply_client = _module(
        "target_optiply.client", OptiplySink=object
    )
    target_optiply_auth = _module(
        "target_optiply.auth", OptiplyAuthenticator=object
    )
    setattr(target_optiply, "client", target_optiply_client)
    setattr(target_optiply, "auth", target_optiply_auth)

    fake_modules = {
        "backoff": _module("backoff"),
        "requests": _module("requests"),
        "singer_sdk": singer_sdk,
        "singer_sdk.exceptions": exceptions,
        "singer_sdk.plugin_base": plugin_base,
        "target_hotglue": target_hotglue,
        "target_hotglue.client": target_hotglue_client,
        "target_optiply": target_optiply,
        "target_optiply.client": target_optiply_client,
        "target_optiply.auth": target_optiply_auth,
    }
    sink_path = Path(__file__).resolve().parents[1] / "target_optiply" / "sinks.py"
    spec = importlib.util.spec_from_file_location("_test_target_optiply_sinks", sink_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {sink_path}")

    with patch.dict(sys.modules, fake_modules, clear=False):
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return getattr(module, "SellOrderLineSink"), getattr(module, "SellOrderSink"), module


SellOrderLineSink, SellOrderSink, _sinks = _load_sink_classes()


def _sink(sink_class: Any) -> Any:
    sink = object.__new__(sink_class)
    sink.stream_name = sink_class.endpoint
    sink.endpoint = sink_class.endpoint
    sink.logger = logging.getLogger(sink_class.__name__)
    sink._target = Mock()
    return sink


class _Response:
    status_code = 201
    text = ""

    def json(self):
        return {"data": {"id": "created-order"}}


CREATED = ("created-order", True, {})


def test_nested_sell_order_payload_and_post_without_target_id():
    sink = _sink(SellOrderSink)
    record = {
        "order_id": "fe-order-1",
        "placed": "2025-01-01T00:00:00Z",
        "completed": "2025-01-02T00:00:00Z",
        "totalValue": "99.99",
        "line_items": json.dumps(
            [
                {
                    "quantity": 2,
                    "subtotalValue": "12.50",
                    "productId": 7,
                    "placed": "2025-01-01T01:00:00Z",
                },
                {"quantity": 1, "subtotalValue": 0, "productId": 8, "placed": " "},
            ]
        ),
    }

    payload = sink.preprocess_record(record, {})
    attributes = payload["data"]["attributes"]
    assert "id" not in payload["data"]
    assert attributes["completed"] == "2025-01-02T00:00:00Z"
    assert attributes["totalValue"] == "99.99"
    assert attributes["orderLines"] == [
        {
            "quantity": 2,
            "subtotalValue": "12.5",
            "productId": 7,
            "placed": "2025-01-01T01:00:00Z",
        },
        {
            "quantity": 1,
            "subtotalValue": "0.0",
            "productId": 8,
        },
    ]

    for completed in (" ", None):
        optional_completed = sink.preprocess_record(
            {**record, "completed": completed}, {}
        )
        assert "completed" not in optional_completed["data"]["attributes"]

    missing_completed = sink.preprocess_record(
        {key: value for key, value in record.items() if key != "completed"}, {}
    )
    assert "completed" not in missing_completed["data"]["attributes"]

    request_api = Mock(return_value=_Response())
    with patch.object(sink, "request_api", new=request_api, create=True):
        result = sink.upsert_record(payload, {})
    assert result == CREATED
    request_api.assert_called_once_with(
        http_method="POST",
        endpoint="sellOrders",
        request_data=payload,
    )


def test_sell_order_remote_id_is_sdk_external_id_and_stays_in_optiply_payload():
    sink = _sink(SellOrderSink)
    payload = sink.preprocess_record(
        {"remoteId": "source-order", "placed": "2025-01-01", "totalValue": 12}, {}
    )
    assert payload["externalId"] == "source-order"
    assert payload["data"]["attributes"]["remoteId"] == "source-order"

    # HotglueSink removes externalId before handing the record to upsert_record.
    sdk_payload = dict(payload)
    assert sdk_payload.pop("externalId", None) == "source-order"
    request_api = Mock(return_value=_Response())
    with patch.object(sink, "request_api", new=request_api, create=True):
        assert sink.upsert_record(sdk_payload, {}) == CREATED
    request_api.assert_called_once_with(
        http_method="POST",
        endpoint="sellOrders",
        request_data=sdk_payload,
    )
    assert sdk_payload["data"]["attributes"]["remoteId"] == "source-order"

    missing_remote_id = sink.preprocess_record(
        {"order_id": "source-order", "placed": "2025-01-01", "totalValue": 12}, {}
    )
    assert "externalId" not in missing_remote_id


def test_sell_order_total_fallback_and_standalone_line_placed():
    order_sink = _sink(SellOrderSink)
    payload = order_sink.preprocess_record(
        {
            "placed": "2025-01-01T00:00:00Z",
            "line_items": json.dumps(
                [
                    {"quantity": 1, "subtotalValue": "1.25", "productId": 7},
                    {"quantity": 2, "subtotalValue": "2", "productId": 8},
                ]
            ),
        },
        {},
    )
    assert payload["data"]["attributes"]["totalValue"] == "3.25"

    line_sink = _sink(SellOrderLineSink)
    line_record = {
        "quantity": 1,
        "subtotalValue": "4.00",
        "productId": 7,
        "sellOrderId": 3,
        "placed": "2025-01-01T01:00:00Z",
    }
    line = line_sink.preprocess_record(line_record, {})
    assert line["data"]["attributes"]["placed"] == "2025-01-01T01:00:00Z"

    for placed in ("", None):
        optional_line = line_sink.preprocess_record(
            {**line_record, "placed": placed}, {}
        )
        assert "placed" not in optional_line["data"]["attributes"]

    missing_line = line_sink.preprocess_record(
        {key: value for key, value in line_record.items() if key != "placed"}, {}
    )
    assert "placed" not in missing_line["data"]["attributes"]


def test_etl_skipped_sell_order_is_counted_as_existing_without_api_call():
    sink = _sink(SellOrderSink)
    sink.latest_state = {
        "summary": {"SellOrders": {"success": 0, "fail": 0, "existing": 0, "updated": 0}},
        "bookmarks": {"SellOrders": []},
    }
    payload = sink.preprocess_record(
        {"remoteId": "o1", "skip": True, "skipReason": "line 2: product X not found"}, {}
    )
    request_api = Mock()
    with patch.object(sink, "request_api", new=request_api, create=True):
        sink.process_record(payload, {})

    request_api.assert_not_called()
    sink._target.submit_write.assert_not_called()
    assert sink.latest_state["summary"]["SellOrders"] == {
        "success": 0, "fail": 0, "existing": 1, "updated": 0
    }
    assert sink.latest_state["bookmarks"]["SellOrders"] == [
        {"success": True, "existing": True, "externalId": "o1",
         "skipReason": "line 2: product X not found"}
    ]

    # Only a boolean true skips; anything else is a normal write.
    not_skipped = sink.preprocess_record({"remoteId": "o2", "skip": "false", "placed": "2025-01-01"}, {})
    assert "data" in not_skipped and "skip" not in not_skipped


def test_export_details_count_skipped_records_as_existing():
    target_hotglue = _module("target_hotglue")
    setattr(target_hotglue, "__path__", [])
    sink_names = [
        "BaseOptiplySink", "ProductsSink", "SupplierSink", "SupplierProductSink", "BuyOrderSink",
        "BuyOrderLineSink", "SellOrderSink", "SellOrderLineSink", "PromotionSink",
        "PromotionProductSink",
    ]
    fake_modules = {
        "target_hotglue": target_hotglue,
        "target_hotglue.target": _module("target_hotglue.target", TargetHotglue=object),
        "target_optiply.sinks": _module("target_optiply.sinks", **{n: object for n in sink_names}),
    }
    target_path = Path(__file__).resolve().parents[1] / "target_optiply" / "target.py"
    spec = importlib.util.spec_from_file_location("_test_target_optiply_target", target_path)
    with patch.dict(sys.modules, fake_modules, clear=False):
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    details = module.TargetOptiply._simplify_records_dict(None, {
        "SellOrders": [
            {"success": True, "id": "1"},
            {"success": False},
            {"success": True, "existing": True, "externalId": "o3"},
        ]
    })
    assert details == {"SellOrders": {"total": 3, "success": 1, "failed": 1, "existing": 1}}
