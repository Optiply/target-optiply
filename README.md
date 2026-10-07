# target-optiply

`target-optiply` is a Singer target for Optiply, built with the Meltano Singer SDK.

## Overview

This target integrates with the Optiply API to sync data from various sources into Optiply's inventory management system. It supports multiple entity types including products, suppliers, orders, and order lines.

## Installation

Install from PyPi:

```bash
pipx install target-optiply
```

Install from GitHub:

```bash
pipx install git+https://github.com/optiply/target-optiply.git@main
```

## Configuration

### Required Configuration

The following configuration options are required:

| Setting | Type | Description |
|---------|------|-------------|
| `username` | string | Optiply API username |
| `client_id` | string | Optiply API client ID |
| `client_secret` | string | Optiply API client secret |
| `password` | string | Optiply API password |

### Optional Configuration

| Setting | Type | Description | Default |
|---------|------|-------------|---------|
| `account_id` | integer | Optiply account ID | None |
| `coupling_id` | integer | Optiply coupling ID | None |
| `start_date` | string | Start date for data sync | '2010-01-01T00:00:00Z' |
| `hotglue_metadata` | object | Hotglue metadata configuration | None |

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `optiply_base_url` | Base URL for Optiply API | `https://api.optiply.com/v1` |
| `optiply_dashboard_url` | Dashboard URL for authentication | `https://dashboard.optiply.nl/api` |

### Configuration Example

```json
{
  "username": "your_username",
  "client_id": "your_client_id",
  "client_secret": "your_client_secret",
  "password": "your_password",
  "account_id": 12345,
  "coupling_id": 67890,
  "start_date": "2023-01-01T00:00:00Z"
}
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Supported Streams

The target supports the following Optiply entity types:

### Products
- **Endpoint**: `/products`
- **Mandatory Fields**: `name`, `stockLevel`, `unlimitedStock`
- **Supported Fields**: `name`, `skuCode`, `eanCode`, `articleCode`, `price`, `unlimitedStock`, `stockLevel`, `notBeingBought`, `resumingPurchase`, `status`, `assembled`, `minimumStock`, `maximumStock`, `ignored`, `manualServiceLevel`, `createdAtRemote`, `stockMeasurementUnit`

### Suppliers
- **Endpoint**: `/suppliers`
- **Mandatory Fields**: `name`
- **Supported Fields**: `name`, `emails`, `minimumOrderValue`, `fixedCosts`, `deliveryTime`, `userReplenishmentPeriod`, `reactingToLostSales`, `lostSalesReaction`, `lostSalesMovReaction`, `backorders`, `backorderThreshold`, `backordersReaction`, `maxLoadCapacity`, `containerVolume`, `ignored`, `globalLocationNumber`, `type`

### Supplier Products
- **Endpoint**: `/supplierProducts`
- **Mandatory Fields**: `name`, `productId`, `supplierId`
- **Supported Fields**: `name`, `skuCode`, `eanCode`, `articleCode`, `price`, `minimumPurchaseQuantity`, `lotSize`, `availability`, `availabilityDate`, `preferred`, `productId`, `supplierId`, `deliveryTime`, `status`, `freeStock`, `weight`, `volume`

### Buy Orders
- **Endpoint**: `/buyOrders`
- **Mandatory Fields**: `placed`, `totalValue`, `supplierId`, `accountId`
- **Supported Fields**: `placed`, `completed`, `expectedDeliveryDate`, `totalValue`, `supplierId`, `accountId`, `assembly`

### Buy Order Lines
- **Endpoint**: `/buyOrderLines`
- **Mandatory Fields**: `subtotalValue`, `productId`, `quantity`, `buyOrderId`
- **Supported Fields**: `quantity`, `subtotalValue`, `productId`, `buyOrderId`, `expectedDeliveryDate`

### Sell Orders
- **Endpoint**: `/sellOrders`
- **Mandatory Fields**: `totalValue`, `placed`
- **Supported Fields**: `placed`, `completed`, `totalValue`
- **Sales-history ETL contract**: CSV `order_id` and `order_placed` are required. Keep `order_id` as the source/grouping key only; it is not the Optiply JSON:API `data.id`, so omit `id` and send a POST. Map `order_placed` to `placed` and map nonblank `order_completed` to `completed`; omit blank optional dates. Emit a non-empty `line_items` JSON string, resolve `product_ean` or `product_sku` to `productId`, require `line_quantity`, map blank `line_subtotalvalue` to `subtotalValue` `0`, and map nonblank `line_placed` to nested `placed`. The ETL calculates `totalValue` and skips orders with no lines.

### Sell Order Lines
- **Endpoint**: `/sellOrderLines`
- **Mandatory Fields**: `subtotalValue`, `sellOrderId`, `productId`, `quantity`
- **Supported Fields**: `quantity`, `subtotalValue`, `productId`, `sellOrderId`, `placed`

### Promotions
- **Endpoint**: `/promotions`
- **Mandatory Fields**: `name`, `startDate`, `endDate`, `entireShop`, `enabled`, `upliftType`
- **Supported Fields**: `name`, `startDate`, `endDate`, `entireShop`, `enabled`, `upliftType`, `upliftIncrease`

### Promotion Products
- **Endpoint**: `/promotionProducts`
- **Mandatory Fields**: `productId`, `promotionId`
- **Supported Fields**: `productId`, `promotionId`, `Remote_promotionId`, `specificUpliftType`, `specificUpliftIncrease`
- **Product ID contract**: `productId` must be the Optiply product ID. If FE provides SKU, EAN, or article code, resolve it to Optiply `productId` in the ETL before sending records to this target.
- **Two-file import contract**: send `Promotions` before `PromotionProducts` in the same target run. `PromotionProducts.Remote_promotionId` must match the source `externalId` or `inputId` from the corresponding `Promotions` row so the target can resolve the Optiply promotion ID from the in-memory run cache.

## Usage

You can easily run `target-optiply` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-optiply --version
target-optiply --help
# Test using a sample tap:
tap-carbon-intensity | target-optiply --config /path/to/target-optiply-config.json
```

### Using with Meltano

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-optiply
meltano install

# Test invocation:
meltano invoke target-optiply --version
# Run a test pipeline:
meltano run tap-carbon-intensity target-optiply
```

## API Authentication

The target uses OAuth 2.0 password flow for authentication with Optiply's API. The authentication process:

1. Uses client credentials (client_id + client_secret) for Basic Auth
2. Exchanges username/password for access token
3. Automatically refreshes tokens when they expire
4. Includes Bearer token in all API requests
5. Handles 401 errors with automatic token refresh and retry

## Data Processing

### Field Mapping
Each sink defines field mappings that transform incoming record fields to Optiply API fields.

### Data Type Conversion
The target automatically handles:
- **Boolean conversion**: String "true"/"false" to boolean values
- **Integer conversion**: String numbers to integers
- **Float conversion**: String numbers to floats
- **Date conversion**: Datetime objects to ISO format strings
- **Array conversion**: JSON string arrays to actual arrays

### Validation
- **Mandatory fields**: Records missing required fields are skipped
- **Data validation**: Invalid data types or values are logged and handled gracefully
- **Business rules**: Supplier types, GLN lengths, and other business validations

### Error Handling
- **401 errors**: Automatic token refresh and retry with exponential backoff
- **404 errors**: Record not found, logged as warning
- **400+ errors**: Client errors, logged as errors
- **500+ errors**: Server errors, retried with exponential backoff

## Output Files

The target generates both Singer state messages (to stdout) and a `target-state.json` file containing processing statistics and record details:

```json
{
  "bookmarks": {
    "stream_name": [
      {
        "hash": "record_hash",
        "success": true,
        "id": "record_id",
        "externalId": "external_id"
      }
    ]
  },
  "summary": {
    "stream_name": {
      "success": 10,
      "fail": 0,
      "existing": 0,
      "updated": 0
    }
  }
}
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `target-optiply` CLI interface directly using `poetry run`:

```bash
poetry run target-optiply --help
```

### SDK Dev Guide

See the [Meltano Singer SDK documentation](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Singer SDK to
develop your own Singer taps and targets.

## License

Apache-2.0
