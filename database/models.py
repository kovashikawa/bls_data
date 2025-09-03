# bls_data/database/models.py

from sqlalchemy import Column, String, Integer, Boolean, Text, DateTime, ForeignKey, Index, UniqueConstraint, Numeric
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid

from .config import Base


class BLSSeries(Base):
    """Series metadata and catalog information"""
    __tablename__ = 'bls_series'
    
    series_id = Column(String(20), primary_key=True)
    series_title = Column(Text)
    survey_name = Column(String(100))
    measure_data_type = Column(String(50))
    area = Column(String(100))
    item = Column(String(100))
    seasonality = Column(String(50))
    base_period = Column(String(50))
    begin_year = Column(Integer)
    begin_period = Column(String(10))
    end_year = Column(Integer)
    end_period = Column(String(10))
    latest = Column(Boolean, default=False)
    data_frequency = Column(String(20), default='monthly')
    last_updated = Column(DateTime, default=func.current_timestamp())
    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    data_points = relationship("BLSDataPoint", back_populates="series")
    aliases = relationship("BLSAlias", back_populates="series")
    data_quality = relationship("BLSDataQuality", back_populates="series")
    freshness = relationship("BLSDataFreshness", back_populates="series", uselist=False)
    
    # Indexes
    __table_args__ = (
        Index('idx_series_survey', 'survey_name'),
        Index('idx_series_area_item', 'area', 'item'),
    )


class BLSDataPoint(Base):
    """Time series data points"""
    __tablename__ = 'bls_data_points'
    
    id = Column(Integer, primary_key=True)
    series_id = Column(String(20), ForeignKey('bls_series.series_id'), nullable=False)
    year = Column(Integer, nullable=False)
    period = Column(String(10), nullable=False)
    period_name = Column(String(50))
    value = Column(Numeric(15, 4))
    footnotes = Column(Text)
    data_source = Column(String(20), default='api')
    extraction_id = Column(UUID(as_uuid=True))
    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    series = relationship("BLSSeries", back_populates="data_points")
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('series_id', 'year', 'period', name='uq_data_point_series_year_period'),
        Index('idx_data_points_series_year', 'series_id', 'year'),
        Index('idx_data_points_period', 'year', 'period'),
        Index('idx_data_points_value', 'value'),
        Index('idx_data_points_recent', 'series_id', 'year', 'period'),
    )


class BLSAlias(Base):
    """Series aliases for human-readable names"""
    __tablename__ = 'bls_aliases'
    
    id = Column(Integer, primary_key=True)
    alias = Column(String(100), nullable=False)
    series_id = Column(String(20), ForeignKey('bls_series.series_id'), nullable=False)
    alias_type = Column(String(20), default='user')
    created_at = Column(DateTime, default=func.current_timestamp())
    
    # Relationships
    series = relationship("BLSSeries", back_populates="aliases")
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('alias', 'series_id', name='uq_alias_series'),
        Index('idx_aliases_alias', 'alias'),
        Index('idx_aliases_series', 'series_id'),
    )


class BLSExtractionLog(Base):
    """Data extraction logs for tracking and debugging"""
    __tablename__ = 'bls_extraction_logs'
    
    id = Column(Integer, primary_key=True)
    extraction_id = Column(UUID(as_uuid=True), nullable=False, default=uuid.uuid4)
    series_ids = Column(ARRAY(String), nullable=False)
    start_year = Column(Integer)
    end_year = Column(Integer)
    records_extracted = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    extraction_status = Column(String(20), default='pending')
    error_message = Column(Text)
    api_calls_made = Column(Integer, default=0)
    extraction_duration_seconds = Column(Integer)
    extraction_metadata = Column(JSONB)
    created_at = Column(DateTime, default=func.current_timestamp())
    
    # Indexes
    __table_args__ = (
        Index('idx_extraction_logs_date', 'created_at'),
        Index('idx_extraction_logs_status', 'extraction_status'),
        Index('idx_extraction_logs_series_ids', 'series_ids'),
        Index('idx_extraction_logs_metadata', 'extraction_metadata'),
    )


class BLSDataQuality(Base):
    """Data quality and validation tracking"""
    __tablename__ = 'bls_data_quality'
    
    id = Column(Integer, primary_key=True)
    series_id = Column(String(20), ForeignKey('bls_series.series_id'), nullable=False)
    check_type = Column(String(50), nullable=False)
    check_date = Column(DateTime, default=func.current_date())
    issues_found = Column(Integer, default=0)
    issues_resolved = Column(Integer, default=0)
    quality_score = Column(Numeric(3, 2))
    statistics = Column(JSONB)
    notes = Column(Text)
    created_at = Column(DateTime, default=func.current_timestamp())
    
    # Relationships
    series = relationship("BLSSeries", back_populates="data_quality")
    
    # Constraints and indexes
    __table_args__ = (
        Index('idx_data_quality_series', 'series_id'),
        Index('idx_data_quality_check_date', 'check_date'),
        Index('idx_data_quality_statistics', 'statistics'),
    )


class BLSDataFreshness(Base):
    """Data freshness tracking (replaces Redis caching)"""
    __tablename__ = 'bls_data_freshness'
    
    series_id = Column(String(20), ForeignKey('bls_series.series_id'), primary_key=True)
    last_extracted = Column(DateTime)
    last_updated = Column(DateTime)
    data_completeness = Column(Numeric(3, 2), default=1.0)
    expected_update_frequency = Column(String(20), default='1 month')
    next_expected_update = Column(DateTime)
    extraction_priority = Column(Integer, default=5)  # 1=high, 10=low
    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    series = relationship("BLSSeries", back_populates="freshness")
    
    # Indexes
    __table_args__ = (
        Index('idx_freshness_last_extracted', 'last_extracted'),
        Index('idx_freshness_next_update', 'next_expected_update'),
        Index('idx_freshness_priority', 'extraction_priority'),
    )
