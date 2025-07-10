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
from singer_sdk.sinks import RecordSink
import singer_sdk.typing as th

from target_optiply.auth import OptiplyAuthenticator
from target_optiply.client import OptiplySink

logger = logging.getLogger(__name__)

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

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = self.stream_name.lower() if not self.endpoint else self.endpoint
        self._processed_records = []

    @property
    def success_count(self) -> int:
        """Get the number of successfully processed records."""
        return sum(1 for record in self._processed_records if record.get('success', False))

    @property
    def failure_count(self) -> int:
        """Get the number of failed records."""
        return sum(1 for record in self._processed_records if not record.get('success', False))

    def get_url(
        self,
        context: Optional[dict] = None,
    ) -> str:
        """Get the stream's API URL."""
        endpoint = self.endpoint
        record = context.get("record", {}) if context else {}
        
        # Construct the base URL with endpoint
        if context and context.get("http_method") == "PATCH" and "id" in record:
            url = f"{self.base_url}/{endpoint}/{record['id']}"
        else:
            url = f"{self.base_url}/{endpoint}"

        # Add query parameters
        query_params = {}
        
        # Add account and coupling IDs if they exist in config
        if hasattr(self, 'config'):
            if 'account_id' in self.config:
                query_params['accountId'] = self.config['account_id']
            if 'coupling_id' in self.config:
                query_params['couplingId'] = self.config['coupling_id']

        # Add any additional query parameters from the original URL
        url_parts = self.url().split('?')
        if len(url_parts) > 1:
            additional_params = dict(param.split('=') for param in url_parts[1].split('&'))
            query_params.update(additional_params)

        # Construct final URL with query parameters
        if query_params:
            query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
            url = f"{url}?{query_string}"

        return url

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return self.field_mappings

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

    def _prepare_payload(
        self,
        context: Optional[dict] = None,
        record: Optional[dict] = None,
    ) -> dict:
        """Prepare the payload for the API request."""
        self.logger.info(f"Preparing payload for {self.stream_name}")
        self.logger.info(f"Context: {context}")
        self.logger.info(f"Record: {record}")

        # Get the HTTP method from context
        http_method = context.get("http_method", "POST")
        self.logger.info(f"HTTP Method from context: {http_method}")

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
        self.logger.info(f"Final payload: {json.dumps(payload, indent=2)}")
        return payload

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

    def process_record(self, record: Dict, context: Dict = None) -> None:
        """Process a record."""
        try:
            # Create context if not provided
            if context is None:
                context = {}

            # Set http_method based on presence of id field
            http_method = "PATCH" if "id" in record else "POST"
            context["http_method"] = http_method
            context["record"] = record  # Add record to context for URL construction
            
            # Log record processing
            self.logger.info(f"Processing record for {self.stream_name} (ID: {record.get('id')})")

            # For POST requests, check mandatory fields
            if http_method == "POST":
                mandatory_fields = self.get_mandatory_fields()
                missing_fields = []
                for field in mandatory_fields:
                    if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                        missing_fields.append(field)
                if missing_fields:
                    error_msg = f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}"
                    self.logger.error(error_msg)
                    self._processed_records.append({
                        "hash": self._generate_record_hash(record),
                        "success": False,
                        "id": record.get('id'),
                        "externalId": record.get('externalId'),
                        "error": error_msg
                    })
                    return None

            # Prepare the payload
            payload = self._prepare_payload(context, record)
            if not payload:
                error_msg = "Failed to prepare payload"
                self.logger.error(error_msg)
                self._processed_records.append({
                    "hash": self._generate_record_hash(record),
                    "success": False,
                    "id": record.get('id'),
                    "externalId": record.get('externalId'),
                    "error": error_msg
                })
                return

            # Get the URL for the request
            url = self.get_url(context)
            self.logger.info(f"Making {http_method} request to: {url}")

            # Make the request using the _request method to handle 401 errors with token refresh
            # Construct the endpoint properly for the _request method
            if record.get('id'):
                endpoint = f"{self.endpoint}/{record.get('id')}"
            else:
                endpoint = self.endpoint
                
            response = self._request(
                http_method=http_method,
                endpoint=endpoint,
                request_data=payload
            )
            self.logger.info(f"Response status: {response.status_code}")
            self.logger.info(f"Response body: {response.text}")

            # Validate the response
            if response.status_code == 404:
                error_msg = f"Record not found (404): {record.get('id')}"
                self.logger.warning(error_msg)
                self._processed_records.append({
                    "hash": self._generate_record_hash(record),
                    "success": False,
                    "id": record.get('id'),
                    "externalId": record.get('externalId'),
                    "error": error_msg
                })
                return
            elif response.status_code >= 400:
                error_msg = f"Request failed with status {response.status_code}: {response.text}"
                self.logger.error(error_msg)
                self._processed_records.append({
                    "hash": self._generate_record_hash(record),
                    "success": False,
                    "id": record.get('id'),
                    "externalId": record.get('externalId'),
                    "error": error_msg
                })
                return

            # If we get here, the record was processed successfully
            self._processed_records.append({
                "hash": self._generate_record_hash(record),
                "success": True,
                "id": record.get('id'),
                "externalId": record.get('externalId')
            })

        except Exception as e:
            error_msg = f"Error processing record: {str(e)}"
            self.logger.error(error_msg)
            self._processed_records.append({
                "hash": self._generate_record_hash(record),
                "success": False,
                "id": record.get('id'),
                "externalId": record.get('externalId'),
                "error": error_msg
            })

    def _generate_record_hash(self, record: Dict) -> str:
        """Generate a hash for the record to track its state.
        
        Args:
            record: The record to generate a hash for.
            
        Returns:
            A hash string representing the record's state.
        """
        import hashlib
        from datetime import datetime
        from decimal import Decimal
        
        # Create a serializable copy of the record
        serializable_record = {}
        for key, value in record.items():
            if isinstance(value, datetime):
                serializable_record[key] = value.isoformat()
            elif isinstance(value, Decimal):
                serializable_record[key] = float(value)
            else:
                serializable_record[key] = value
        
        # Create a string representation of the record's key fields
        key_fields = sorted(serializable_record.items())
        record_str = json.dumps(key_fields, sort_keys=True)
        # Generate SHA-256 hash
        return hashlib.sha256(record_str.encode()).hexdigest()

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink."""
        return []

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about processed records.

        Returns:
            Dictionary containing success, failure counts, and processed records details.
        """
        success_count = self.success_count
        failure_count = self.failure_count
        return {
            "success": success_count,
            "failure": failure_count,
            "total": success_count + failure_count,
            "processed_records": self._processed_records
        }

class ProductsSink(BaseOptiplySink):
    """Products sink class."""

    endpoint = "products"
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

    def __init__(self, target: Any, stream_name: str, schema: Dict, key_properties: List[str]):
        """Initialize the sink."""
        super().__init__(target, stream_name, schema, key_properties)
        # Override key_properties to make id optional for creation
        self._key_properties = []

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "stockLevel", "unlimitedStock"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """

class SupplierSink(BaseOptiplySink):
    """Optiply target sink class for suppliers."""

    endpoint = "suppliers"
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

class SupplierProductSink(BaseOptiplySink):
    """Optiply target sink class for supplier products."""

    endpoint = "supplierProducts"
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

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "productId", "supplierId"]

class BuyOrderSink(BaseOptiplySink):
    """Optiply target sink class for buy orders."""

    endpoint = "buyOrders"
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
    field_mappings = {
        "placed": "placed",
        "totalValue": "totalValue"
    }

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
        if "line_items" in record:
            line_items = json.loads(record["line_items"])
            sell_order_lines = []
            total_value = 0
            for item in line_items:
                subtotal_value = float(item["subtotalValue"])
                total_value += subtotal_value
                sell_order_lines.append({
                    "type": "sellOrderLines",
                    "attributes": {
                        "quantity": item["quantity"],
                        "subtotalValue": str(subtotal_value),
                        "productId": item["productId"]
                    }
                })
            attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = sell_order_lines

class SellOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for sell order lines."""

    endpoint = "sellOrderLines"
    field_mappings = {
        "quantity": "quantity",
        "subtotalValue": "subtotalValue",
        "productId": "productId",
        "sellOrderId": "sellOrderId"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["subtotalValue", "sellOrderId", "productId", "quantity"]