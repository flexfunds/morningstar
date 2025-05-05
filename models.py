from sqlalchemy import create_engine, Column, String, Float, Date, DateTime, Integer, ForeignKey, Enum, UniqueConstraint, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import enum

Base = declarative_base()


class SeriesStatus(enum.Enum):
    ACTIVE = "A"      # Active
    INACTIVE = "D"    # Discontinued
    MATURED = "Matured"


class NAVFrequency(enum.Enum):
    DAILY = "Daily"
    WEEKLY = "Weekly"
    MONTHLY = "Monthly"
    QUARTERLY = "Quarterly"


class Series(Base):
    __tablename__ = 'series'

    isin = Column(String(12), primary_key=True)
    common_code = Column(String(50))
    series_number = Column(String(50))
    series_name = Column(String(255), nullable=False)
    status = Column(Enum(SeriesStatus))
    issuance_type = Column(String(50))
    product_type = Column(String(50))
    issuance_date = Column(Date)
    maturity_date = Column(Date)
    close_date = Column(Date)

    issuer = Column(String(255))
    relationship_manager = Column(String(255))
    series_region = Column(String(100))
    portfolio_manager_jurisdiction = Column(String(100))
    portfolio_manager = Column(String(255))
    borrower = Column(String(255))
    asset_manager = Column(String(255))
    currency = Column(String(3))
    nav_frequency = Column(Enum(NAVFrequency))

    issuance_principal_amount = Column(Float)
    underlying_valuation_update = Column(String(50))
    fees_frequency = Column(String(50))
    payment_method = Column(String(50))

    # Relationships
    custodians = relationship("Custodian", back_populates="series")
    fee_structures = relationship("FeeStructure", back_populates="series")


class Custodian(Base):
    __tablename__ = 'custodians'

    id = Column(Integer, primary_key=True)
    series_isin = Column(String(12), ForeignKey('series.isin'))
    custodian_name = Column(String(255))
    account_number = Column(String(100))

    # Relationship
    series = relationship("Series", back_populates="custodians")


class FeeType(enum.Enum):
    FIXED = "Fixed"
    AUM_BASED = "AUM Based"


class FeeStructure(Base):
    __tablename__ = 'fee_structures'

    id = Column(Integer, primary_key=True)
    series_isin = Column(String(12), ForeignKey('series.isin'))
    fee_type = Column(String(50))  # e.g., 'Arranger Fee', 'Maintenance Fee'
    fee_type_category = Column(Enum(FeeType), nullable=False)
    # The AUM threshold in millions, nullable for fixed fees
    aum_threshold = Column(Float, nullable=True)
    fee_percentage = Column(Float, nullable=True)  # The fee percentage
    fixed_amount = Column(Float, nullable=True)  # For fixed amount fees
    # Currency for fixed amount fees
    currency = Column(String(3), nullable=True)
    # For storing non-standard fee values or ranges
    notes = Column(String(255), nullable=True)

    # Relationship
    series = relationship("Series", back_populates="fee_structures")


class NAVEntry(Base):
    __tablename__ = 'nav_entries'

    id = Column(Integer, primary_key=True)
    isin = Column(String(12), ForeignKey(
        'series.isin'), index=True, nullable=False)
    series_number = Column(String(50), index=True)
    nav_date = Column(Date, index=True, nullable=False)
    nav_value = Column(Float, nullable=False)
    distribution_type = Column(String(20), nullable=False)
    emitter = Column(String(10))
    created_at = Column(DateTime, default=datetime.utcnow)

    # Add unique constraint
    __table_args__ = (
        UniqueConstraint('isin', 'nav_date',
                         name='uix_nav_entry_isin_date'),
    )

    # Relationship
    series = relationship("Series")

    def __repr__(self):
        return f"<NAVEntry(isin='{self.isin}', series_number='{self.series_number}', date='{self.nav_date}', value={self.nav_value})>"


class Trade(Base):
    """Model for storing trade data from BNY files"""
    __tablename__ = 'trades'

    id = Column(Integer, primary_key=True)
    series_number = Column(String(50), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    trade_type = Column(String(50))  # Buy/Sell
    security_type = Column(String(50))  # Equity, Fixed Income, etc.
    security_name = Column(String(255))
    security_id = Column(String(100))  # ISIN, CUSIP, etc.
    quantity = Column(Float)
    price = Column(Float)
    currency = Column(String(10))
    settlement_date = Column(Date)
    trade_value = Column(Float)
    broker = Column(String(100))
    account = Column(String(100))
    source_file = Column(String(255))  # Original file name
    source_folder = Column(String(255))  # ETPCAP2 or HFMX
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False,
                        default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Trade(series_number='{self.series_number}', trade_date='{self.trade_date}', security_name='{self.security_name}')>"


def init_db(connection_string='sqlite:///nav_data.db'):
    """Initialize the database and create tables"""
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)
