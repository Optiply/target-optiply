"""Optiply target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import json
import logging
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase

from target_optiply.auth import OptiplyAuthenticator
from target_optiply.client import OptiplySink

logger = logging.getLogger(__name__)

_promotions_id_cache: Dict[str, str] = {}

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder for datetime objects."""

    def default(self, obj):
        """Encode datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class BaseOptiplySink(OptiplySink):
    """Base sink for Optiply streams."""

    endpoint = None
    field_mappings = {}
    _processed_records = []

    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = self.stream_name.lower() if not self.endpoint else self.endpoint
        self._processed_records = []
        self._stashed_external_id = None

    def process_record(self, record: dict, context: dict) -> None:
        self._stashed_external_id = record.get("externalId") or record.get("inputId")
        super().process_record(record, context)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess the record before sending to API."""
        # Build attributes using field mappings
        attributes = self.build_attributes(record, self.get_field_mappings())

        # Add any additional attributes from the record
        self._add_additional_attributes(record, attributes)

        payload = {
            "data": {
                "type": self.endpoint,
                "attributes": attributes
            }
        }
        
        # Add ID for PATCH requests
        if "id" in record:
            payload["data"]["id"] = record["id"]
            
        return payload

    def upsert_record(self, record: dict, context: dict) -> tuple:
        """Process the record and return (id, success, state_updates)."""
        state_updates = {}
        
        try:
            # Get the ID from the preprocessed record structure
            record_id = None
            if 'data' in record and 'id' in record['data']:
                record_id = record['data']['id']
            elif 'id' in record:
                record_id = record['id']
            
            # Set http_method based on presence of id field
            http_method = "PATCH" if record_id else "POST"
            
            # Log record processing
            self.logger.info(f"Processing record for {self.stream_name} (ID: {record_id})")

            # For POST requests, check mandatory fields
            if http_method == "POST":
                mandatory_fields = self.get_mandatory_fields()
                missing_fields = []
                
                # Get the actual record data - it might be nested in data.attributes
                actual_record = record
                if 'data' in record and 'attributes' in record['data']:
                    actual_record = record['data']['attributes']
                
                for field in mandatory_fields:
                    if field not in actual_record or actual_record[field] is None or (isinstance(actual_record[field], str) and not actual_record[field].strip()):
                        missing_fields.append(field)
                if missing_fields:
                    error_msg = f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}"
                    self.logger.error(error_msg)
                    return None, False, state_updates

            # Get the URL for the request
            if record_id:
                endpoint = f"{self.endpoint}/{record_id}"
            else:
                endpoint = self.endpoint
                
            self.logger.info(f"Making {http_method} request to: {endpoint}")

            # Make the request
            response = self.request_api(
                http_method=http_method,
                endpoint=endpoint,
                request_data=record
            )
            
            self.logger.info(f"Response status: {response.status_code}")
            self.logger.info(f"Response body: {response.text}")

            # Handle response
            if response.status_code == 404:
                # Get meaningful error message from response
                error_details = self._get_error_message_from_response(response.text, response.status_code)
                error_msg = f"Record not found (404): {record_id} - {error_details}"
                self.logger.warning(error_msg)
                return None, False, state_updates
            elif response.status_code >= 400:
                # Get meaningful error message from response
                error_details = self._get_error_message_from_response(response.text, response.status_code)
                error_msg = f"Request failed with status {response.status_code}: {error_details}"
                self.logger.error(error_msg)
                return None, False, state_updates

            # Parse response to get ID
            response_data = response.json()
            if "data" in response_data and "id" in response_data["data"]:
                response_record_id = response_data["data"]["id"]
            else:
                response_record_id = record_id or "unknown"

            self.logger.info(f"{self.stream_name} processed with id: {response_record_id}")
            return response_record_id, True, state_updates

        except Exception as e:
            error_msg = f"Error processing record: {str(e)}"
            self.logger.error(error_msg)
            return None, False, state_updates

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return self.field_mappings

    def _get_error_message_from_response(self, response_text: str, status_code: int) -> str:
        """Get a meaningful error message from response text."""
        if not response_text or response_text.strip() in ['', 'null', 'None']:
            return f"No error details provided (status {status_code})"
        
        # Try to parse JSON error response
        try:
            import json
            error_data = json.loads(response_text)
            if isinstance(error_data, dict):
                if 'errors' in error_data and isinstance(error_data['errors'], list):
                    error_messages = []
                    for error in error_data['errors']:
                        if isinstance(error, dict):
                            if 'meta' in error and 'message' in error['meta']:
                                error_messages.append(error['meta']['message'])
                            elif 'detail' in error:
                                error_messages.append(error['detail'])
                            elif 'message' in error:
                                error_messages.append(error['message'])
                    if error_messages:
                        return f"API Error: {'; '.join(error_messages)}"
                elif 'message' in error_data:
                    return f"API Error: {error_data['message']}"
                elif 'error' in error_data:
                    return f"API Error: {error_data['error']}"
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
        
        # Fallback to raw response text if it's meaningful
        if len(response_text.strip()) > 0:
            return f"API Error: {response_text}"
        else:
            return f"No error details provided (status {status_code})"

    def build_attributes(self, record: Dict, field_mappings: Dict[str, str]) -> Dict:
        """Build attributes dictionary from record using field mappings.

        Args:
            record: The record to transform
            field_mappings: Dictionary mapping record fields to API fields

        Returns:
            Dictionary of attributes for the API request
        """
        attributes = {}
        from datetime import datetime
        from decimal import Decimal
        for record_field, api_field in field_mappings.items():
            if record_field in record and record[record_field] is not None:
                value = record[record_field]
                # Handle datetime objects
                if isinstance(value, datetime):
                    value = value.isoformat()
                elif isinstance(value, Decimal):
                    value = float(value)
                attributes[api_field] = value
        return attributes

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        # Handle emails field - convert from JSON string to array if present
        if "emails" in record and record["emails"] is not None:
            try:
                attributes["emails"] = json.loads(record["emails"])
            except json.JSONDecodeError:
                self.logger.warning(f"Could not parse emails JSON string: {record['emails']}")
                attributes["emails"] = []

        # Fields that should be integers
        integer_fields = [
            "deliveryTime",
            "userReplenishmentPeriod",
            "lostSalesReaction",
            "lostSalesMovReaction",
            "backorderThreshold",
            "backordersReaction",
            "maxLoadCapacity",
            "containerVolume"
        ]
        for field in integer_fields:
            if field in record and record[field] is not None:
                try:
                    # First convert to float to handle decimal strings, then to int
                    attributes[field] = int(float(record[field]))
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {record[field]}")
                    attributes.pop(field, None)

        # Fields that should be floats
        float_fields = [
            "minimumOrderValue",
            "fixedCosts"
        ]
        for field in float_fields:
            if field in record and record[field] is not None:
                try:
                    attributes[field] = float(record[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to float: {record[field]}")
                    attributes.pop(field, None)

        # Validate type field
        if "type" in record and record["type"] not in ["vendor", "producer"]:
            self.logger.warning(f"Invalid type value: {record['type']}. Must be 'vendor' or 'producer'")
            attributes["type"] = "vendor"  # Default to vendor if invalid

        # Validate globalLocationNumber length
        if "globalLocationNumber" in record and len(record["globalLocationNumber"]) != 13:
            self.logger.warning(f"Invalid globalLocationNumber length: {len(record['globalLocationNumber'])}. Must be 13 characters")
            attributes.pop("globalLocationNumber", None)  # Remove if invalid

        # Remove remoteDataSyncedToDate as it's not accepted by the API
        attributes.pop("remoteDataSyncedToDate", None)

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink."""
        return []


class ProductsSink(BaseOptiplySink):
    """Products sink class."""

    endpoint = "products"
    
    @property
    def name(self) -> str:
        return "Products"
    field_mappings = {
        "name": "name",
        "skuCode": "skuCode",
        "eanCode": "eanCode",
        "articleCode": "articleCode",
        "price": "price",
        "unlimitedStock": "unlimitedStock",
        "stockLevel": "stockLevel",
        "notBeingBought": "notBeingBought",
        "resumingPurchase": "resumingPurchase",
        "status": "status",
        "assembled": "assembled",
        "minimumStock": "minimumStock",
        "maximumStock": "maximumStock",
        "ignored": "ignored",
        "manualServiceLevel": "manualServiceLevel",
        "createdAtRemote": "createdAtRemote",
        "stockMeasurementUnit": "stockMeasurementUnit"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "stockLevel", "unlimitedStock"]


class SupplierSink(BaseOptiplySink):
    """Optiply target sink class for suppliers."""

    endpoint = "suppliers"
    
    @property
    def name(self) -> str:
        return "Suppliers"
    field_mappings = {
        "name": "name",
        "emails": "emails",
        "minimumOrderValue": "minimumOrderValue",
        "fixedCosts": "fixedCosts",
        "deliveryTime": "deliveryTime",
        "userReplenishmentPeriod": "userReplenishmentPeriod",
        "reactingToLostSales": "reactingToLostSales",
        "lostSalesReaction": "lostSalesReaction",
        "lostSalesMovReaction": "lostSalesMovReaction",
        "backorders": "backorders",
        "backorderThreshold": "backorderThreshold",
        "backordersReaction": "backordersReaction",
        "maxLoadCapacity": "maxLoadCapacity",
        "containerVolume": "containerVolume",
        "ignored": "ignored",
        "globalLocationNumber": "globalLocationNumber",
        "type": "type"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name"]


class SupplierProductSink(BaseOptiplySink):
    """Optiply target sink class for supplier products."""

    endpoint = "supplierProducts"
    
    @property
    def name(self) -> str:
        return "SupplierProducts"
    field_mappings = {
        "name": "name",
        "skuCode": "skuCode",
        "eanCode": "eanCode",
        "articleCode": "articleCode",
        "price": "price",
        "minimumPurchaseQuantity": "minimumPurchaseQuantity",
        "lotSize": "lotSize",
        "availability": "availability",
        "availabilityDate": "availabilityDate",
        "preferred": "preferred",
        "productId": "productId",
        "supplierId": "supplierId",
        "deliveryTime": "deliveryTime",
        "status": "status",
        "freeStock": "freeStock",
        "weight": "weight",
        "volume": "volume"
    }

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes with proper data type conversion."""
        super()._add_additional_attributes(record, attributes)
        
        # Convert price to float and round to 2 decimal places
        if "price" in attributes and attributes["price"] is not None:
            try:
                price = float(attributes["price"])
                # Round to 2 decimal places as per API spec
                attributes["price"] = round(price, 2)
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert price to float: {attributes['price']}")
                attributes.pop("price", None)
        
        # Convert boolean fields
        boolean_fields = ["availability", "preferred"]
        for field in boolean_fields:
            if field in attributes and attributes[field] is not None:
                value = attributes[field]
                if isinstance(value, str):
                    if value.lower() in ['true', '1', 'yes']:
                        attributes[field] = True
                    elif value.lower() in ['false', '0', 'no']:
                        attributes[field] = False
                    else:
                        self.logger.warning(f"Could not convert {field} to boolean: {value}")
                        attributes.pop(field, None)
        
        # Convert integer fields with validation
        integer_fields = ["productId", "supplierId", "deliveryTime", "freeStock"]
        for field in integer_fields:
            if field in attributes and attributes[field] is not None:
                try:
                    attributes[field] = int(float(attributes[field]))
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {attributes[field]}")
                    attributes.pop(field, None)
        
        # Convert and validate minimumPurchaseQuantity and lotSize (must be >= 1)
        for field in ["minimumPurchaseQuantity", "lotSize"]:
            if field in attributes and attributes[field] is not None:
                try:
                    value = int(float(attributes[field]))
                    if value >= 1:
                        attributes[field] = value
                    else:
                        self.logger.warning(f"{field} must be >= 1, got: {value}")
                        attributes.pop(field, None)
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {attributes[field]}")
                    attributes.pop(field, None)
        
        # Convert double fields (weight, volume) with appropriate precision
        double_fields = ["weight", "volume"]
        for field in double_fields:
            if field in attributes and attributes[field] is not None:
                try:
                    value = float(attributes[field])
                    # For very small values (< 0.001), preserve more precision
                    if abs(value) < 0.001 and value != 0:
                        # Use 6 decimal places for small values to preserve precision
                        attributes[field] = round(value, 6)
                    else:
                        # Use 2 decimal places for larger values
                        attributes[field] = round(value, 2)
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to float: {attributes[field]}")
                    attributes.pop(field, None)
        
        # Validate and normalize status field (should be lowercase per API spec)
        if "status" in attributes and attributes["status"] is not None:
            status = attributes["status"].lower()
            if status in ["enabled", "active", "true", "1"]:
                attributes["status"] = "enabled"
            elif status in ["disabled", "inactive", "false", "0"]:
                attributes["status"] = "disabled"
            else:
                self.logger.warning(f"Invalid status value: {attributes['status']}. Must be 'enabled' or 'disabled'")
                attributes["status"] = "enabled"  # Default to enabled
        
        # Remove any None values that might cause issues
        attributes = {k: v for k, v in attributes.items() if v is not None}
        
        # Log the final attributes for debugging
        self.logger.info(f"Final attributes for {self.stream_name}: {attributes}")

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "productId", "supplierId"]


class BuyOrderSink(BaseOptiplySink):
    """Optiply target sink class for buy orders."""

    endpoint = "buyOrders"
    
    @property
    def name(self) -> str:
        return "BuyOrders"
    field_mappings = {
        "placed": "placed",
        "completed": "completed",
        "expectedDeliveryDate": "expectedDeliveryDate",
        "totalValue": "totalValue",
        "supplierId": "supplierId",
        "accountId": "accountId",
        "assembly": "assembly"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["placed", "totalValue", "supplierId", "accountId"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        if "line_items" in record:
            line_items = json.loads(record["line_items"])
            buy_order_lines = []
            total_value = 0
            for item in line_items:
                subtotal_value = float(item["subtotalValue"])
                total_value += subtotal_value
                buy_order_lines.append({
                    "type": "buyOrderLines",
                    "attributes": {
                        "quantity": item["quantity"],
                        "subtotalValue": str(subtotal_value),
                        "productId": item["productId"],
                        "expectedDeliveryDate": item.get("expectedDeliveryDate")
                    }
                })
            attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = buy_order_lines


class BuyOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for buy order lines."""

    endpoint = "buyOrderLines"
    
    @property
    def name(self) -> str:
        return "BuyOrderLines"
    field_mappings = {
        "quantity": "quantity",
        "subtotalValue": "subtotalValue",
        "productId": "productId",
        "buyOrderId": "buyOrderId",
        "expectedDeliveryDate": "expectedDeliveryDate"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["subtotalValue", "productId", "quantity", "buyOrderId"]


class SellOrderSink(BaseOptiplySink):
    """Optiply target sink class for sell orders."""

    endpoint = "sellOrders"
    
    @property
    def name(self) -> str:
        return "SellOrders"
    field_mappings = {
        "placed": "placed",
        "completed": "completed",
        "totalValue": "totalValue",
        "remoteId": "remoteId"
    }

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload = super().preprocess_record(record, context)
        if "remoteId" in record:
            payload["externalId"] = record["remoteId"]
        return payload

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["totalValue", "placed"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        completed = attributes.get("completed")
        if isinstance(completed, str) and not completed.strip():
            attributes.pop("completed", None)

        if "line_items" in record:
            line_items = json.loads(record["line_items"])
            sell_order_lines = []
            total_value = 0
            for item in line_items:
                subtotal_value = float(item["subtotalValue"])
                total_value += subtotal_value
                line_attributes = {
                    "quantity": item["quantity"],
                    "subtotalValue": str(subtotal_value),
                    "productId": item["productId"]
                }
                placed = item.get("placed")
                if placed is not None and (not isinstance(placed, str) or placed.strip()):
                    line_attributes["placed"] = placed
                sell_order_lines.append(line_attributes)
            if "totalValue" not in attributes:
                attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = sell_order_lines


class SellOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for sell order lines."""

    endpoint = "sellOrderLines"
    
    @property
    def name(self) -> str:
        return "SellOrderLines"
    field_mappings = {
        "quantity": "quantity",
        "subtotalValue": "subtotalValue",
        "productId": "productId",
        "sellOrderId": "sellOrderId",
        "placed": "placed"
    }

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Omit blank optional placement dates from standalone lines."""
        placed = attributes.get("placed")
        if isinstance(placed, str) and not placed.strip():
            attributes.pop("placed", None)

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["subtotalValue", "sellOrderId", "productId", "quantity"]


class PromotionValueNormalizer:
    """Shared normalization helpers for promotion sinks."""

    def _normalize_boolean_fields(self, attributes: Dict, fields: List[str]) -> None:
        """Convert common string boolean values to booleans."""
        for field in fields:
            if field not in attributes or attributes[field] is None:
                continue

            value = attributes[field]
            if isinstance(value, bool):
                continue
            if isinstance(value, str):
                normalized_value = value.strip().lower()
                if normalized_value in ["true", "1", "yes"]:
                    attributes[field] = True
                elif normalized_value in ["false", "0", "no"]:
                    attributes[field] = False
                else:
                    self.logger.warning(f"Could not convert {field} to boolean: {value}")
                    attributes.pop(field, None)

    def _normalize_float_fields(self, attributes: Dict, fields: List[str]) -> None:
        """Convert numeric strings to floats."""
        for field in fields:
            if field not in attributes or attributes[field] is None:
                continue

            try:
                attributes[field] = float(attributes[field])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert {field} to float: {attributes[field]}")
                attributes.pop(field, None)

    def _normalize_uplift_type(self, attributes: Dict, field: str) -> None:
        """Normalize uplift type values accepted by the Optiply API."""
        if field not in attributes or attributes[field] is None:
            return

        value = str(attributes[field]).strip().lower()
        if value in ["relative", "absolute", "close_out"]:
            attributes[field] = value
        else:
            self.logger.warning(f"Invalid {field} value: {attributes[field]}")
            attributes.pop(field, None)

    def _normalize_integer_fields(self, attributes: Dict, fields: List[str]) -> None:
        """Convert integer-like input values to integers."""
        for field in fields:
            if field not in attributes or attributes[field] is None:
                continue

            try:
                attributes[field] = int(float(attributes[field]))
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert {field} to integer: {attributes[field]}")
                attributes.pop(field, None)


class PromotionSink(PromotionValueNormalizer, BaseOptiplySink):
    """Optiply target sink class for promotions."""

    endpoint = "promotions"

    @property
    def name(self) -> str:
        return "Promotions"

    field_mappings = {
        "name": "name",
        "startDate": "startDate",
        "endDate": "endDate",
        "entireShop": "entireShop",
        "enabled": "enabled",
        "upliftType": "upliftType",
        "upliftIncrease": "upliftIncrease",
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "startDate", "endDate", "entireShop", "enabled", "upliftType"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Normalize promotion fields before sending them to Optiply."""
        super()._add_additional_attributes(record, attributes)

        self._normalize_boolean_fields(attributes, ["entireShop", "enabled"])
        self._normalize_float_fields(attributes, ["upliftIncrease"])
        self._normalize_uplift_type(attributes, "upliftType")

    def upsert_record(self, record: dict, context: dict) -> tuple:
        record_id, success, state_updates = super().upsert_record(record, context)
        if success and record_id and self._stashed_external_id:
            _promotions_id_cache[str(self._stashed_external_id)] = str(record_id)
        return record_id, success, state_updates


class PromotionProductSink(PromotionValueNormalizer, BaseOptiplySink):
    """Optiply target sink class for promotion products."""

    endpoint = "promotionProducts"

    @property
    def name(self) -> str:
        return "PromotionProducts"

    field_mappings = {
        "productId": "productId",
        "promotionId": "promotionId",
        "specificUpliftType": "specificUpliftType",
        "specificUpliftIncrease": "specificUpliftIncrease",
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["productId", "promotionId"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Normalize promotion product fields before sending them to Optiply."""
        super()._add_additional_attributes(record, attributes)

        remote_promotion = record.get("Remote_promotionId")
        promotion_id = (
            _promotions_id_cache.get(str(remote_promotion)) if remote_promotion else None
        ) or attributes.get("promotionId")

        if promotion_id is not None:
            attributes["promotionId"] = promotion_id

        self._normalize_integer_fields(attributes, ["productId", "promotionId"])
        self._normalize_float_fields(attributes, ["specificUpliftIncrease"])
        self._normalize_uplift_type(attributes, "specificUpliftType")
