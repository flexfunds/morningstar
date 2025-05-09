import logging
from typing import Dict, List, Tuple, Optional, Set, Any
import pandas as pd
from datetime import datetime
from sqlalchemy import func
from models import Series, SeriesStatus, NAVEntry
from db_service import DatabaseService
from config import AppConfig

logger = logging.getLogger(__name__)


class DBManager:
    """Handles database operations for NAV processing"""

    def __init__(self, config: AppConfig):
        """
        Initialize the database manager

        Args:
            config: Application configuration
        """
        self.config = config
        self.db_service = DatabaseService(config.db_connection_string)

    def get_series_by_isins(self, isins: Set[str]) -> Dict[str, Series]:
        """
        Get series information from database

        Args:
            isins: Set of ISINs to look up

        Returns:
            Dictionary of Series objects keyed by ISIN
        """
        with self.db_service.SessionMaker() as session:
            series_info = session.query(Series).filter(
                Series.isin.in_(isins)).all()
            return {s.isin: s for s in series_info}

    def get_isins_by_frequency(self, frequency: str) -> List[str]:
        """
        Get ISINs for a specific NAV frequency

        Args:
            frequency: NAV frequency to filter by

        Returns:
            List of ISINs
        """
        # Convert frequency to uppercase for consistent comparison
        frequency = frequency.upper()
        with self.db_service.SessionMaker() as session:
            return [r[0] for r in session.query(Series.isin)
                    .filter(func.upper(Series.nav_frequency) == frequency)
                    .filter(Series.status == SeriesStatus.ACTIVE)
                    .all()]

    def get_isins_by_product_type(self, product_type: str) -> List[str]:
        """
        Get ISINs for a specific product type

        Args:
            product_type: Product type to filter by

        Returns:
            List of ISINs
        """
        with self.db_service.SessionMaker() as session:
            product_type_pattern = f"%{product_type}%"
            return [r[0] for r in session.query(Series.isin)
                    .filter(Series.product_type.ilike(product_type_pattern))
                    .filter(Series.status == SeriesStatus.ACTIVE)
                    .all()]

    def get_target_isins(self, isin_filter) -> Optional[Set[str]]:
        """
        Process ISIN filters and return target ISINs

        Args:
            isin_filter: Can be one of:
                - A predefined frequency ("daily", "weekly", "monthly", "quarterly")
                - A predefined product type ("wrappers_hybrid", "loan")
                - A list of predefined frequencies or product types
                - A specific ISIN or list of ISINs
                - None to process all ISINs

        Returns:
            Set of target ISINs or None if no filter applied
        """
        if not isin_filter:
            return None

        target_isins = set()

        if isinstance(isin_filter, str):
            # Check if it's a frequency, product type, or a specific ISIN
            if isin_filter.upper() in ['DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY']:
                target_isins.update(
                    self.get_isins_by_frequency(isin_filter.upper()))
            elif isin_filter.lower() == 'wrappers_hybrid':
                target_isins.update(
                    self.get_isins_by_product_type('Wrappers Hybrid'))
            elif isin_filter.lower() == 'loan':
                target_isins.update(
                    self.get_isins_by_product_type('Loan'))
            else:
                target_isins.add(isin_filter)
        else:
            # List of frequencies, product types, or ISINs
            for item in isin_filter:
                if item.upper() in ['DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY']:
                    target_isins.update(
                        self.get_isins_by_frequency(item.upper()))
                elif item.lower() == 'wrappers_hybrid':
                    target_isins.update(
                        self.get_isins_by_product_type('Wrappers Hybrid'))
                elif item.lower() == 'loan':
                    target_isins.update(
                        self.get_isins_by_product_type('Loan'))
                else:
                    target_isins.add(item)

        return target_isins if target_isins else None

    def save_nav_data(self, nav_dfs: List[Tuple[str, pd.DataFrame]],
                      distribution_type: str) -> Tuple[int, int, int]:
        """
        Save NAV data to database

        Args:
            nav_dfs: List of (emitter, dataframe) tuples
            distribution_type: Type of distribution

        Returns:
            Tuple of (added_count, duplicates_count, invalids_count)
        """
        total_added = 0
        total_duplicates = 0
        total_invalids = 0

        for emitter, df in nav_dfs:
            if not df.empty:
                logger.info(f"Processing {emitter} data: {len(df)} entries")

                added, duplicates, invalids = self.db_service.save_nav_entries(
                    df, distribution_type, emitter)
                total_added += added
                total_duplicates += duplicates
                total_invalids += invalids

                if added > 0:
                    logger.info(f"{emitter}: Added {added} entries")

        # Log final summary in a more concise format
        if total_added > 0 or total_duplicates > 0:
            logger.info(
                f"DB Import: {total_added} added, {total_duplicates} duplicates, {total_invalids} invalid")

        return total_added, total_duplicates, total_invalids

    def get_nav_history(self, isin: Optional[str] = None, start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None, page: int = 1, per_page: int = 50,
                        series_number: Optional[str] = None) -> Dict[str, Any]:
        """
        Get NAV history for a specific ISIN or series number within a date range with pagination

        Args:
            isin: Optional ISIN to filter by
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            page: Page number (1-based)
            per_page: Number of entries per page
            series_number: Optional series number to filter by

        Returns:
            Dict containing entries, total_pages, and total_entries
        """
        try:
            nav_entries = self.db_service.get_nav_history(
                isin=isin,
                start_date=start_date,
                end_date=end_date,
                page=page,
                per_page=per_page,
                series_number=series_number
            )
            logger.info(
                f"Retrieved NAV entries for query: ISIN={isin}, series_number={series_number}, results: {len(nav_entries['entries'])}")
            return nav_entries
        except Exception as e:
            logger.error(f"Error retrieving NAV history: {str(e)}")
            raise

    def import_historic_data(self, excel_path: str) -> Tuple[int, int]:
        """
        Import historic NAV data from Excel file

        Args:
            excel_path: Path to the Excel file containing historic NAV data

        Returns:
            Tuple of (added_count, duplicates_count)
        """
        try:
            results = self.db_service.import_historic_data(excel_path)

            # Calculate total counts across all sheets
            total_added = sum(
                result.added_count for result in results.values())
            total_duplicates = sum(
                result.duplicates_count for result in results.values())

            logger.info(
                f"Successfully imported historic NAV data: {total_added} new entries added, "
                f"{total_duplicates} duplicates skipped"
            )
            return total_added, total_duplicates
        except Exception as e:
            logger.error(f"Error importing historic NAV data: {str(e)}")
            raise
