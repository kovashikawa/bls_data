#!/usr/bin/env python3
"""
BLS Data RESTful API

A comprehensive RESTful API for the BLS data PostgreSQL database,
optimized for future integration as the core API layer for an MCP server.

Features:
- Full CRUD operations for all database tables
- JSON request/response handling
- Comprehensive error handling
- Psycopg3 integration
- Connection pooling
"""

import json
import os
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row
from pydantic import BaseModel, Field, validator


# Database Configuration
class DatabaseConfig:
    def __init__(self):
        self.db_user = os.getenv("DB_USER", "postgres")
        self.db_pass = os.getenv("DB_PASS", "password")
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_name = os.getenv("DB_NAME", "bls_data")
        self.connection_string = f"postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"

    def get_connection(self):
        """Get a database connection with dict row factory"""
        return psycopg.connect(self.connection_string, row_factory=dict_row)


# Initialize FastAPI app
app = FastAPI(
    title="BLS Data API",
    description="RESTful API for Bureau of Labor Statistics data",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global database config
db_config = DatabaseConfig()


# Pydantic Models for Request/Response
class BLSSeriesBase(BaseModel):
    series_id: str = Field(..., max_length=20)
    series_title: Optional[str] = None
    survey_name: Optional[str] = Field(None, max_length=100)
    measure_data_type: Optional[str] = Field(None, max_length=50)
    area: Optional[str] = Field(None, max_length=100)
    item: Optional[str] = Field(None, max_length=100)
    seasonality: Optional[str] = Field(None, max_length=50)
    base_period: Optional[str] = Field(None, max_length=50)
    begin_year: Optional[int] = None
    begin_period: Optional[str] = Field(None, max_length=10)
    end_year: Optional[int] = None
    end_period: Optional[str] = Field(None, max_length=10)
    latest: Optional[bool] = False
    data_frequency: Optional[str] = Field("monthly", max_length=20)

    class Config:
        from_attributes = True


class BLSSeriesCreate(BLSSeriesBase):
    pass


class BLSSeriesUpdate(BaseModel):
    series_title: Optional[str] = None
    survey_name: Optional[str] = Field(None, max_length=100)
    measure_data_type: Optional[str] = Field(None, max_length=50)
    area: Optional[str] = Field(None, max_length=100)
    item: Optional[str] = Field(None, max_length=100)
    seasonality: Optional[str] = Field(None, max_length=50)
    base_period: Optional[str] = Field(None, max_length=50)
    begin_year: Optional[int] = None
    begin_period: Optional[str] = Field(None, max_length=10)
    end_year: Optional[int] = None
    end_period: Optional[str] = Field(None, max_length=10)
    latest: Optional[bool] = None
    data_frequency: Optional[str] = Field(None, max_length=20)

    class Config:
        from_attributes = True


class BLSSeriesResponse(BLSSeriesBase):
    last_updated: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BLSDataPointBase(BaseModel):
    series_id: str = Field(..., max_length=20)
    year: int
    period: str = Field(..., max_length=10)
    period_name: Optional[str] = Field(None, max_length=50)
    date: Optional[date] = None
    value: Optional[Decimal] = None
    footnotes: Optional[str] = None
    data_source: Optional[str] = Field("api", max_length=20)
    extraction_id: Optional[str] = None

    @validator("extraction_id")
    def validate_extraction_id(cls, v):
        if v and not isinstance(v, str):
            try:
                uuid.UUID(v)
            except ValueError:
                raise ValueError("extraction_id must be a valid UUID string")
        return v

    class Config:
        from_attributes = True


class BLSDataPointCreate(BLSDataPointBase):
    pass


class BLSDataPointUpdate(BaseModel):
    period_name: Optional[str] = Field(None, max_length=50)
    date: Optional[date] = None
    value: Optional[Decimal] = None
    footnotes: Optional[str] = None
    data_source: Optional[str] = Field(None, max_length=20)
    extraction_id: Optional[str] = None

    @validator("extraction_id")
    def validate_extraction_id(cls, v):
        if v and not isinstance(v, str):
            try:
                uuid.UUID(v)
            except ValueError:
                raise ValueError("extraction_id must be a valid UUID string")
        return v

    class Config:
        from_attributes = True


class BLSDataPointResponse(BLSDataPointBase):
    id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BLSAliasBase(BaseModel):
    series_id: str = Field(..., max_length=20)
    alias: str = Field(..., max_length=100)
    description: Optional[str] = None

    class Config:
        from_attributes = True


class BLSAliasCreate(BLSAliasBase):
    pass


class BLSAliasUpdate(BaseModel):
    alias: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None

    class Config:
        from_attributes = True


class BLSAliasResponse(BLSAliasBase):
    id: int
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BLSExtractionLogBase(BaseModel):
    extraction_id: str
    series_ids: List[str]
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    status: str = Field(..., max_length=20)
    records_extracted: Optional[int] = 0
    records_updated: Optional[int] = 0
    records_inserted: Optional[int] = 0
    error_message: Optional[str] = None
    api_calls_made: Optional[int] = 0
    extraction_duration_seconds: Optional[int] = 0
    extraction_metadata: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class BLSExtractionLogCreate(BLSExtractionLogBase):
    pass


class BLSExtractionLogUpdate(BaseModel):
    series_ids: Optional[List[str]] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    status: Optional[str] = Field(None, max_length=20)
    records_extracted: Optional[int] = None
    records_updated: Optional[int] = None
    records_inserted: Optional[int] = None
    error_message: Optional[str] = None
    api_calls_made: Optional[int] = None
    extraction_duration_seconds: Optional[int] = None
    extraction_metadata: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class BLSExtractionLogResponse(BLSExtractionLogBase):
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BLSDataQualityBase(BaseModel):
    series_id: str = Field(..., max_length=20)
    check_type: str = Field(..., max_length=50)
    issues_found: Optional[int] = 0
    issues_resolved: Optional[int] = 0
    quality_score: Optional[Decimal] = None
    statistics: Optional[Dict[str, Any]] = None
    notes: Optional[str] = None

    class Config:
        from_attributes = True


class BLSDataQualityCreate(BLSDataQualityBase):
    pass


class BLSDataQualityUpdate(BaseModel):
    check_type: Optional[str] = Field(None, max_length=50)
    issues_found: Optional[int] = None
    issues_resolved: Optional[int] = None
    quality_score: Optional[Decimal] = None
    statistics: Optional[Dict[str, Any]] = None
    notes: Optional[str] = None

    class Config:
        from_attributes = True


class BLSDataQualityResponse(BLSDataQualityBase):
    id: int
    check_date: Optional[datetime] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BLSDataFreshnessBase(BaseModel):
    series_id: str = Field(..., max_length=20)
    last_extracted: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    data_completeness: Optional[Decimal] = 1.0
    expected_update_frequency: Optional[str] = Field("1 month", max_length=20)
    next_expected_update: Optional[datetime] = None
    extraction_priority: Optional[int] = 5

    class Config:
        from_attributes = True


class BLSDataFreshnessCreate(BLSDataFreshnessBase):
    pass


class BLSDataFreshnessUpdate(BaseModel):
    last_extracted: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    data_completeness: Optional[Decimal] = None
    expected_update_frequency: Optional[str] = Field(None, max_length=20)
    next_expected_update: Optional[datetime] = None
    extraction_priority: Optional[int] = None

    class Config:
        from_attributes = True


class BLSDataFreshnessResponse(BLSDataFreshnessBase):
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Error Response Model
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None


# Database Helper Functions
def execute_query(query: str, params: tuple = None, fetch_one: bool = False, fetch_all: bool = False):
    """Execute a database query and return results"""
    try:
        with db_config.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch_one:
                    return cur.fetchone()
                elif fetch_all:
                    return cur.fetchall()
                else:
                    conn.commit()
                    return cur.rowcount
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def get_table_columns(table_name: str) -> List[str]:
    """Get column names for a table"""
    query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = %s 
    ORDER BY ordinal_position
    """
    result = execute_query(query, (table_name,), fetch_all=True)
    return [row["column_name"] for row in result]


# BLSSeries Endpoints
@app.get("/bls_series", response_model=List[BLSSeriesResponse])
async def get_all_series(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    survey_name: Optional[str] = None,
    area: Optional[str] = None
):
    """Get all BLS series with optional filtering"""
    query = "SELECT * FROM bls_series WHERE 1=1"
    params = []

    if survey_name:
        query += " AND survey_name ILIKE %s"
        params.append(f"%{survey_name}%")

    if area:
        query += " AND area ILIKE %s"
        params.append(f"%{area}%")

    query += " ORDER BY series_id LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    return execute_query(query, tuple(params), fetch_all=True)


@app.post("/bls_series", response_model=BLSSeriesResponse)
async def create_series(series: BLSSeriesCreate):
    """Create a new BLS series"""
    query = """
    INSERT INTO bls_series (
        series_id, series_title, survey_name, measure_data_type, area, item,
        seasonality, base_period, begin_year, begin_period, end_year, end_period,
        latest, data_frequency
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) RETURNING *
    """
    params = (
        series.series_id, series.series_title, series.survey_name,
        series.measure_data_type, series.area, series.item, series.seasonality,
        series.base_period, series.begin_year, series.begin_period,
        series.end_year, series.end_period, series.latest, series.data_frequency
    )

    result = execute_query(query, params, fetch_one=True)
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create series")

    return result


@app.get("/bls_series/{series_id}", response_model=BLSSeriesResponse)
async def get_series(series_id: str):
    """Get a specific BLS series by ID"""
    query = "SELECT * FROM bls_series WHERE series_id = %s"
    result = execute_query(query, (series_id,), fetch_one=True)

    if not result:
        raise HTTPException(status_code=404, detail="Series not found")

    return result


@app.put("/bls_series/{series_id}", response_model=BLSSeriesResponse)
async def update_series(series_id: str, series: BLSSeriesUpdate):
    """Update a BLS series by ID"""
    # Build dynamic update query
    update_fields = []
    params = []

    for field, value in series.dict(exclude_unset=True).items():
        update_fields.append(f"{field} = %s")
        params.append(value)

    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(series_id)
    query = f"UPDATE bls_series SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP WHERE series_id = %s RETURNING *"

    result = execute_query(query, tuple(params), fetch_one=True)
    if not result:
        raise HTTPException(status_code=404, detail="Series not found")

    return result


@app.delete("/bls_series/{series_id}")
async def delete_series(series_id: str):
    """Delete a BLS series by ID"""
    query = "DELETE FROM bls_series WHERE series_id = %s"
    rows_affected = execute_query(query, (series_id,))

    if rows_affected == 0:
        raise HTTPException(status_code=404, detail="Series not found")

    return {"message": "Series deleted successfully"}


# BLSDataPoint Endpoints
@app.get("/bls_data_points", response_model=List[BLSDataPointResponse])
async def get_all_data_points(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    series_id: Optional[str] = None,
    year: Optional[int] = None
):
    """Get all BLS data points with optional filtering"""
    query = "SELECT * FROM bls_data_points WHERE 1=1"
    params = []

    if series_id:
        query += " AND series_id = %s"
        params.append(series_id)

    if year:
        query += " AND year = %s"
        params.append(year)

    query += " ORDER BY series_id, year, period LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    return execute_query(query, tuple(params), fetch_all=True)


@app.post("/bls_data_points", response_model=BLSDataPointResponse)
async def create_data_point(data_point: BLSDataPointCreate):
    """Create a new BLS data point"""
    query = """
    INSERT INTO bls_data_points (
        series_id, year, period, period_name, date, value, footnotes,
        data_source, extraction_id
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) RETURNING *
    """
    params = (
        data_point.series_id, data_point.year, data_point.period,
        data_point.period_name, data_point.date, data_point.value,
        data_point.footnotes, data_point.data_source, data_point.extraction_id
    )

    result = execute_query(query, params, fetch_one=True)
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create data point")

    return result


@app.get("/bls_data_points/{data_point_id}", response_model=BLSDataPointResponse)
async def get_data_point(data_point_id: int):
    """Get a specific BLS data point by ID"""
    query = "SELECT * FROM bls_data_points WHERE id = %s"
    result = execute_query(query, (data_point_id,), fetch_one=True)

    if not result:
        raise HTTPException(status_code=404, detail="Data point not found")

    return result


@app.put("/bls_data_points/{data_point_id}", response_model=BLSDataPointResponse)
async def update_data_point(data_point_id: int, data_point: BLSDataPointUpdate):
    """Update a BLS data point by ID"""
    update_fields = []
    params = []

    for field, value in data_point.dict(exclude_unset=True).items():
        update_fields.append(f"{field} = %s")
        params.append(value)

    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(data_point_id)
    query = f"UPDATE bls_data_points SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP WHERE id = %s RETURNING *"

    result = execute_query(query, tuple(params), fetch_one=True)
    if not result:
        raise HTTPException(status_code=404, detail="Data point not found")

    return result


@app.delete("/bls_data_points/{data_point_id}")
async def delete_data_point(data_point_id: int):
    """Delete a BLS data point by ID"""
    query = "DELETE FROM bls_data_points WHERE id = %s"
    rows_affected = execute_query(query, (data_point_id,))

    if rows_affected == 0:
        raise HTTPException(status_code=404, detail="Data point not found")

    return {"message": "Data point deleted successfully"}


# BLSAlias Endpoints
@app.get("/bls_aliases", response_model=List[BLSAliasResponse])
async def get_all_aliases(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    series_id: Optional[str] = None
):
    """Get all BLS aliases with optional filtering"""
    query = "SELECT * FROM bls_aliases WHERE 1=1"
    params = []

    if series_id:
        query += " AND series_id = %s"
        params.append(series_id)

    query += " ORDER BY series_id, alias LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    return execute_query(query, tuple(params), fetch_all=True)


@app.post("/bls_aliases", response_model=BLSAliasResponse)
async def create_alias(alias: BLSAliasCreate):
    """Create a new BLS alias"""
    query = """
    INSERT INTO bls_aliases (series_id, alias, description)
    VALUES (%s, %s, %s) RETURNING *
    """
    params = (alias.series_id, alias.alias, alias.description)

    result = execute_query(query, params, fetch_one=True)
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create alias")

    return result


@app.get("/bls_aliases/{alias_id}", response_model=BLSAliasResponse)
async def get_alias(alias_id: int):
    """Get a specific BLS alias by ID"""
    query = "SELECT * FROM bls_aliases WHERE id = %s"
    result = execute_query(query, (alias_id,), fetch_one=True)

    if not result:
        raise HTTPException(status_code=404, detail="Alias not found")

    return result


@app.put("/bls_aliases/{alias_id}", response_model=BLSAliasResponse)
async def update_alias(alias_id: int, alias: BLSAliasUpdate):
    """Update a BLS alias by ID"""
    update_fields = []
    params = []

    for field, value in alias.dict(exclude_unset=True).items():
        update_fields.append(f"{field} = %s")
        params.append(value)

    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(alias_id)
    query = f"UPDATE bls_aliases SET {', '.join(update_fields)} WHERE id = %s RETURNING *"

    result = execute_query(query, tuple(params), fetch_one=True)
    if not result:
        raise HTTPException(status_code=404, detail="Alias not found")

    return result


@app.delete("/bls_aliases/{alias_id}")
async def delete_alias(alias_id: int):
    """Delete a BLS alias by ID"""
    query = "DELETE FROM bls_aliases WHERE id = %s"
    rows_affected = execute_query(query, (alias_id,))

    if rows_affected == 0:
        raise HTTPException(status_code=404, detail="Alias not found")

    return {"message": "Alias deleted successfully"}


# BLSExtractionLog Endpoints
@app.get("/bls_extraction_logs", response_model=List[BLSExtractionLogResponse])
async def get_all_extraction_logs(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    status: Optional[str] = None
):
    """Get all BLS extraction logs with optional filtering"""
    query = "SELECT * FROM bls_extraction_logs WHERE 1=1"
    params = []

    if status:
        query += " AND status = %s"
        params.append(status)

    query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    return execute_query(query, tuple(params), fetch_all=True)


@app.post("/bls_extraction_logs", response_model=BLSExtractionLogResponse)
async def create_extraction_log(log: BLSExtractionLogCreate):
    """Create a new BLS extraction log"""
    query = """
    INSERT INTO bls_extraction_logs (
        extraction_id, series_ids, start_year, end_year, status,
        records_extracted, records_updated, records_inserted, error_message,
        api_calls_made, extraction_duration_seconds, extraction_metadata
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) RETURNING *
    """
    params = (
        log.extraction_id, log.series_ids, log.start_year, log.end_year,
        log.status, log.records_extracted, log.records_updated, log.records_inserted,
        log.error_message, log.api_calls_made, log.extraction_duration_seconds,
        json.dumps(log.extraction_metadata) if log.extraction_metadata else None
    )

    result = execute_query(query, params, fetch_one=True)
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create extraction log")

    return result


@app.get("/bls_extraction_logs/{extraction_id}", response_model=BLSExtractionLogResponse)
async def get_extraction_log(extraction_id: str):
    """Get a specific BLS extraction log by ID"""
    query = "SELECT * FROM bls_extraction_logs WHERE extraction_id = %s"
    result = execute_query(query, (extraction_id,), fetch_one=True)

    if not result:
        raise HTTPException(status_code=404, detail="Extraction log not found")

    return result


@app.put("/bls_extraction_logs/{extraction_id}", response_model=BLSExtractionLogResponse)
async def update_extraction_log(extraction_id: str, log: BLSExtractionLogUpdate):
    """Update a BLS extraction log by ID"""
    update_fields = []
    params = []

    for field, value in log.dict(exclude_unset=True).items():
        if field == "extraction_metadata" and value is not None:
            update_fields.append(f"{field} = %s")
            params.append(json.dumps(value))
        else:
            update_fields.append(f"{field} = %s")
            params.append(value)

    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(extraction_id)
    query = f"UPDATE bls_extraction_logs SET {', '.join(update_fields)} WHERE extraction_id = %s RETURNING *"

    result = execute_query(query, tuple(params), fetch_one=True)
    if not result:
        raise HTTPException(status_code=404, detail="Extraction log not found")

    return result


@app.delete("/bls_extraction_logs/{extraction_id}")
async def delete_extraction_log(extraction_id: str):
    """Delete a BLS extraction log by ID"""
    query = "DELETE FROM bls_extraction_logs WHERE extraction_id = %s"
    rows_affected = execute_query(query, (extraction_id,))

    if rows_affected == 0:
        raise HTTPException(status_code=404, detail="Extraction log not found")

    return {"message": "Extraction log deleted successfully"}


# BLSDataQuality Endpoints
@app.get("/bls_data_quality", response_model=List[BLSDataQualityResponse])
async def get_all_data_quality(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    series_id: Optional[str] = None
):
    """Get all BLS data quality records with optional filtering"""
    query = "SELECT * FROM bls_data_quality WHERE 1=1"
    params = []

    if series_id:
        query += " AND series_id = %s"
        params.append(series_id)

    query += " ORDER BY check_date DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    return execute_query(query, tuple(params), fetch_all=True)


@app.post("/bls_data_quality", response_model=BLSDataQualityResponse)
async def create_data_quality(quality: BLSDataQualityCreate):
    """Create a new BLS data quality record"""
    query = """
    INSERT INTO bls_data_quality (
        series_id, check_type, issues_found, issues_resolved,
        quality_score, statistics, notes
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s
    ) RETURNING *
    """
    params = (
        quality.series_id, quality.check_type, quality.issues_found,
        quality.issues_resolved, quality.quality_score,
        json.dumps(quality.statistics) if quality.statistics else None,
        quality.notes
    )

    result = execute_query(query, params, fetch_one=True)
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create data quality record")

    return result


@app.get("/bls_data_quality/{quality_id}", response_model=BLSDataQualityResponse)
async def get_data_quality(quality_id: int):
    """Get a specific BLS data quality record by ID"""
    query = "SELECT * FROM bls_data_quality WHERE id = %s"
    result = execute_query(query, (quality_id,), fetch_one=True)

    if not result:
        raise HTTPException(status_code=404, detail="Data quality record not found")

    return result


@app.put("/bls_data_quality/{quality_id}", response_model=BLSDataQualityResponse)
async def update_data_quality(quality_id: int, quality: BLSDataQualityUpdate):
    """Update a BLS data quality record by ID"""
    update_fields = []
    params = []

    for field, value in quality.dict(exclude_unset=True).items():
        if field == "statistics" and value is not None:
            update_fields.append(f"{field} = %s")
            params.append(json.dumps(value))
        else:
            update_fields.append(f"{field} = %s")
            params.append(value)

    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(quality_id)
    query = f"UPDATE bls_data_quality SET {', '.join(update_fields)} WHERE id = %s RETURNING *"

    result = execute_query(query, tuple(params), fetch_one=True)
    if not result:
        raise HTTPException(status_code=404, detail="Data quality record not found")

    return result


@app.delete("/bls_data_quality/{quality_id}")
async def delete_data_quality(quality_id: int):
    """Delete a BLS data quality record by ID"""
    query = "DELETE FROM bls_data_quality WHERE id = %s"
    rows_affected = execute_query(query, (quality_id,))

    if rows_affected == 0:
        raise HTTPException(status_code=404, detail="Data quality record not found")

    return {"message": "Data quality record deleted successfully"}


# BLSDataFreshness Endpoints
@app.get("/bls_data_freshness", response_model=List[BLSDataFreshnessResponse])
async def get_all_data_freshness(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    series_id: Optional[str] = None
):
    """Get all BLS data freshness records with optional filtering"""
    query = "SELECT * FROM bls_data_freshness WHERE 1=1"
    params = []

    if series_id:
        query += " AND series_id = %s"
        params.append(series_id)

    query += " ORDER BY last_extracted DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    return execute_query(query, tuple(params), fetch_all=True)


@app.post("/bls_data_freshness", response_model=BLSDataFreshnessResponse)
async def create_data_freshness(freshness: BLSDataFreshnessCreate):
    """Create a new BLS data freshness record"""
    query = """
    INSERT INTO bls_data_freshness (
        series_id, last_extracted, last_updated, data_completeness,
        expected_update_frequency, next_expected_update, extraction_priority
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s
    ) RETURNING *
    """
    params = (
        freshness.series_id, freshness.last_extracted, freshness.last_updated,
        freshness.data_completeness, freshness.expected_update_frequency,
        freshness.next_expected_update, freshness.extraction_priority
    )

    result = execute_query(query, params, fetch_one=True)
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create data freshness record")

    return result


@app.get("/bls_data_freshness/{series_id}", response_model=BLSDataFreshnessResponse)
async def get_data_freshness(series_id: str):
    """Get a specific BLS data freshness record by series ID"""
    query = "SELECT * FROM bls_data_freshness WHERE series_id = %s"
    result = execute_query(query, (series_id,), fetch_one=True)

    if not result:
        raise HTTPException(status_code=404, detail="Data freshness record not found")

    return result


@app.put("/bls_data_freshness/{series_id}", response_model=BLSDataFreshnessResponse)
async def update_data_freshness(series_id: str, freshness: BLSDataFreshnessUpdate):
    """Update a BLS data freshness record by series ID"""
    update_fields = []
    params = []

    for field, value in freshness.dict(exclude_unset=True).items():
        update_fields.append(f"{field} = %s")
        params.append(value)

    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(series_id)
    query = f"UPDATE bls_data_freshness SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP WHERE series_id = %s RETURNING *"

    result = execute_query(query, tuple(params), fetch_one=True)
    if not result:
        raise HTTPException(status_code=404, detail="Data freshness record not found")

    return result


@app.delete("/bls_data_freshness/{series_id}")
async def delete_data_freshness(series_id: str):
    """Delete a BLS data freshness record by series ID"""
    query = "DELETE FROM bls_data_freshness WHERE series_id = %s"
    rows_affected = execute_query(query, (series_id,))

    if rows_affected == 0:
        raise HTTPException(status_code=404, detail="Data freshness record not found")

    return {"message": "Data freshness record deleted successfully"}


# Health Check Endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        with db_config.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")


# Root Endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "BLS Data RESTful API",
        "version": "1.0.0",
        "documentation": "/docs",
        "health_check": "/health",
        "endpoints": {
            "bls_series": "/bls_series",
            "bls_data_points": "/bls_data_points",
            "bls_aliases": "/bls_aliases",
            "bls_extraction_logs": "/bls_extraction_logs",
            "bls_data_quality": "/bls_data_quality",
            "bls_data_freshness": "/bls_data_freshness"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
