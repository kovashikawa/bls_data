# BLS Data RESTful API

A comprehensive RESTful API for the Bureau of Labor Statistics (BLS) data PostgreSQL database, optimized for future integration as the core API layer for an MCP server.

## Features

- **Full CRUD Operations**: Complete Create, Read, Update, Delete operations for all database tables
- **JSON API**: All requests and responses use JSON format
- **Psycopg3 Integration**: Modern PostgreSQL driver with connection pooling
- **Comprehensive Error Handling**: Detailed error responses with appropriate HTTP status codes
- **FastAPI Framework**: High-performance, modern Python web framework
- **Interactive Documentation**: Auto-generated OpenAPI/Swagger documentation
- **Type Safety**: Full Pydantic model validation for request/response data

## Database Tables

The API provides endpoints for the following database tables:

1. **`bls_series`** - Series metadata and catalog information
2. **`bls_data_points`** - Time series data points
3. **`bls_aliases`** - Series aliases for human-readable names
4. **`bls_extraction_logs`** - Data extraction tracking and logging
5. **`bls_data_quality`** - Data quality and validation tracking
6. **`bls_data_freshness`** - Data freshness tracking and caching

## Quick Start

### 1. Install Dependencies

```bash
# Using the Makefile
make api-install

# Or manually
./venv/bin/pip install fastapi "uvicorn[standard]" "psycopg[binary]" pydantic python-multipart
```

### 2. Test the API

```bash
# Run the test suite
make api-test

# Or manually
./venv/bin/python test_api.py
```

### 3. Start the Server

```bash
# Using the Makefile
make api-start

# Or manually
./venv/bin/python bls_api.py
```

The API will be available at:
- **API Server**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## API Endpoints

### Series Management (`/bls_series`)

- `GET /bls_series` - List all series (with optional filtering)
- `POST /bls_series` - Create a new series
- `GET /bls_series/{series_id}` - Get a specific series
- `PUT /bls_series/{series_id}` - Update a series
- `DELETE /bls_series/{series_id}` - Delete a series

### Data Points (`/bls_data_points`)

- `GET /bls_data_points` - List all data points (with optional filtering)
- `POST /bls_data_points` - Create a new data point
- `GET /bls_data_points/{data_point_id}` - Get a specific data point
- `PUT /bls_data_points/{data_point_id}` - Update a data point
- `DELETE /bls_data_points/{data_point_id}` - Delete a data point

### Aliases (`/bls_aliases`)

- `GET /bls_aliases` - List all aliases (with optional filtering)
- `POST /bls_aliases` - Create a new alias
- `GET /bls_aliases/{alias_id}` - Get a specific alias
- `PUT /bls_aliases/{alias_id}` - Update an alias
- `DELETE /bls_aliases/{alias_id}` - Delete an alias

### Extraction Logs (`/bls_extraction_logs`)

- `GET /bls_extraction_logs` - List all extraction logs (with optional filtering)
- `POST /bls_extraction_logs` - Create a new extraction log
- `GET /bls_extraction_logs/{extraction_id}` - Get a specific extraction log
- `PUT /bls_extraction_logs/{extraction_id}` - Update an extraction log
- `DELETE /bls_extraction_logs/{extraction_id}` - Delete an extraction log

### Data Quality (`/bls_data_quality`)

- `GET /bls_data_quality` - List all data quality records (with optional filtering)
- `POST /bls_data_quality` - Create a new data quality record
- `GET /bls_data_quality/{quality_id}` - Get a specific data quality record
- `PUT /bls_data_quality/{quality_id}` - Update a data quality record
- `DELETE /bls_data_quality/{quality_id}` - Delete a data quality record

### Data Freshness (`/bls_data_freshness`)

- `GET /bls_data_freshness` - List all data freshness records (with optional filtering)
- `POST /bls_data_freshness` - Create a new data freshness record
- `GET /bls_data_freshness/{series_id}` - Get a specific data freshness record
- `PUT /bls_data_freshness/{series_id}` - Update a data freshness record
- `DELETE /bls_data_freshness/{series_id}` - Delete a data freshness record

## Query Parameters

Most list endpoints support the following query parameters:

- `limit` (int): Maximum number of records to return (default: 100, max: 1000)
- `offset` (int): Number of records to skip (default: 0)
- Table-specific filters (e.g., `series_id`, `year`, `status`)

## Example Usage

### Get all series with filtering

```bash
curl "http://localhost:8000/bls_series?limit=10&survey_name=CPI&area=U.S. City Average"
```

### Create a new series

```bash
curl -X POST "http://localhost:8000/bls_series" \
  -H "Content-Type: application/json" \
  -d '{
    "series_id": "CUSR0000SA0",
    "series_title": "Consumer Price Index for All Urban Consumers",
    "survey_name": "Consumer Price Index",
    "area": "U.S. City Average",
    "item": "All Items",
    "seasonality": "Seasonally Adjusted",
    "data_frequency": "monthly"
  }'
```

### Get data points for a specific series

```bash
curl "http://localhost:8000/bls_data_points?series_id=CUSR0000SA0&year=2023&limit=12"
```

## Error Handling

The API returns detailed error information in JSON format:

```json
{
  "error": "Series not found",
  "detail": "Series with ID 'INVALID_ID' does not exist"
}
```

Common HTTP status codes:
- `200` - Success
- `201` - Created
- `400` - Bad Request
- `404` - Not Found
- `500` - Internal Server Error
- `503` - Service Unavailable (database connection issues)

## Database Configuration

The API uses environment variables for database configuration:

```bash
DB_USER=postgres
DB_PASS=password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bls_data
```

These can be set in a `.env` file or as environment variables.

## Development

### Running Tests

```bash
make api-test
```

### Code Quality

The API code follows the same quality standards as the main project:

```bash
# Run linting
make ruff-check

# Run formatting
make ruff-format

# Auto-fix issues
make ruff-fix
```

### Adding New Endpoints

1. Define Pydantic models for request/response data
2. Add database query functions
3. Create FastAPI route handlers
4. Add tests to `test_api.py`
5. Update this documentation

## Integration with MCP Server

This API is designed to be easily integrated as the core API layer for an MCP (Model Context Protocol) server. The RESTful design and comprehensive CRUD operations make it ideal for:

- Data retrieval and manipulation
- Real-time data access
- Integration with AI/ML systems
- Microservices architecture
- API-first data access patterns

## Performance Considerations

- Connection pooling is enabled for optimal database performance
- Pagination is implemented for large datasets
- Indexes are utilized for efficient querying
- JSON responses are optimized for minimal payload size

## Security Notes

- No authentication is implemented (as per requirements)
- Input validation is handled by Pydantic models
- SQL injection protection via parameterized queries
- CORS is enabled for cross-origin requests

## Troubleshooting

### Database Connection Issues

1. Ensure PostgreSQL is running
2. Check database credentials in `.env` file
3. Verify database exists and is accessible
4. Run `make api-test` to verify connection

### API Not Starting

1. Check if all dependencies are installed: `make api-install`
2. Verify Python environment: `./venv/bin/python --version`
3. Check for port conflicts (default: 8000)
4. Review error messages in the console

### Data Issues

1. Ensure database is populated with BLS data
2. Run `make db-setup` to initialize tables
3. Use `make extract-cpi` to populate with sample data
4. Check database logs for errors

## Support

For issues or questions:
1. Check the interactive documentation at `/docs`
2. Review the test suite in `test_api.py`
3. Check the main project documentation
4. Review database logs and API logs
