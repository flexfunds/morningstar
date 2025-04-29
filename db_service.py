from sqlalchemy.orm import Session
from sqlalchemy import and_, update
import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple, NamedTuple
from models import NAVEntry, init_db, Series
import math
from sqlalchemy.sql import func


class ImportResult(NamedTuple):
    added_count: int
    duplicates_count: int
    invalid_series_count: int

    def __str__(self):
        return (f"Import Results:\n"
                f"  Added entries: {self.added_count}\n"
                f"  Duplicate entries skipped: {self.duplicates_count}\n"
                f"  Invalid series skipped: {self.invalid_series_count}")


class DatabaseService:
    def __init__(self, connection_string='sqlite:///nav_data.db'):
        """Initialize database service with connection string"""
        self.SessionMaker = init_db(connection_string)

    def fix_missing_series_numbers(self) -> Dict[str, Any]:
        """
        Fix NAV entries with missing series numbers by updating them from their corresponding Series.

        Returns:
            Dict containing statistics about the fix operation
        """
        with self.SessionMaker() as session:
            # First, check if there are any Series records with null series_numbers
            series_without_numbers = session.query(Series).filter(
                Series.series_number.is_(None)).all()
            if series_without_numbers:
                print("\nWARNING: Found Series records with null series_numbers:")
                for series in series_without_numbers:
                    print(f"- ISIN: {series.isin}, Name: {series.series_name}")
                print("\nPlease fix the series_numbers in the Series table first.")
                return {
                    'error': 'Series records found with null series_numbers',
                    'affected_isins': [s.isin for s in series_without_numbers]
                }

            # Get all series ISIN to series_number mappings
            series_info = dict(session.query(
                Series.isin, Series.series_number).all())

            # Get all NAV entries with missing series numbers
            missing_before = session.query(NAVEntry).filter(
                NAVEntry.series_number.is_(None)).count()

            # Get ISINs with missing series numbers
            isins_with_missing = [
                row[0] for row in
                session.query(NAVEntry.isin)
                .filter(NAVEntry.series_number.is_(None))
                .distinct()
                .all()
            ]

            # Check which ISINs exist in Series table
            missing_isins = [
                isin for isin in isins_with_missing if isin not in series_info]
            fixable_isins = [
                isin for isin in isins_with_missing if isin in series_info]

            if missing_isins:
                print(
                    "\nWARNING: The following ISINs have NAV entries but no corresponding Series:")
                for isin in missing_isins:
                    print(f"- {isin}")

            # Update NAV entries in batches
            updated_count = 0
            error_count = 0

            try:
                # Update all entries in one go using a case statement
                case_stmt = (
                    "CASE isin "
                    + " ".join(f"WHEN '{isin}' THEN '{series_info[isin]}'" for isin in fixable_isins)
                    + " END"
                )

                if fixable_isins:  # Only attempt update if we have ISINs to fix
                    updated = session.query(NAVEntry)\
                        .filter(NAVEntry.isin.in_(fixable_isins))\
                        .filter(NAVEntry.series_number.is_(None))\
                        .update(
                            {NAVEntry.series_number: case_stmt},
                            synchronize_session=False
                    )
                    updated_count = updated
                    session.commit()

                    # Double-check the update
                    remaining = session.query(NAVEntry)\
                        .filter(NAVEntry.isin.in_(fixable_isins))\
                        .filter(NAVEntry.series_number.is_(None))\
                        .count()

                    if remaining > 0:
                        print(
                            f"\nWARNING: {remaining} entries still have null series numbers after update")

            except Exception as e:
                print(f"Error during batch update: {str(e)}")
                error_count += 1
                session.rollback()

                # Fallback to individual updates if batch update fails
                print("Falling back to individual updates...")
                for isin in fixable_isins:
                    series_number = series_info[isin]
                    if not series_number:
                        print(f"\nWARNING: Series {isin} has no series_number")
                        continue

                    try:
                        updated = session.query(NAVEntry)\
                            .filter(NAVEntry.isin == isin)\
                            .filter(NAVEntry.series_number.is_(None))\
                            .update({'series_number': series_number})
                        updated_count += updated
                        session.commit()
                    except Exception as e:
                        print(
                            f"Error updating NAV entries for ISIN {isin}: {str(e)}")
                        error_count += 1
                        session.rollback()

            return {
                'missing_before': missing_before,
                'updated_count': updated_count,
                'error_count': error_count,
                'missing_isins': missing_isins,
                'fixable_isins': fixable_isins
            }

    def verify_nav_entries(self, isin: Optional[str] = None) -> Dict[str, Any]:
        """
        Verify NAV entries in the database and return statistics

        Args:
            isin: Optional ISIN to verify specific series

        Returns:
            Dict containing verification statistics
        """
        with self.SessionMaker() as session:
            query = session.query(NAVEntry)
            if isin:
                query = query.filter(NAVEntry.isin == isin)

            # Get basic statistics
            total_entries = query.count()

            # Get distribution by type
            distribution_stats = (
                session.query(
                    NAVEntry.distribution_type,
                    NAVEntry.emitter,
                    func.count().label('count')
                )
                .group_by(NAVEntry.distribution_type, NAVEntry.emitter)
                .all()
            )

            # Get date range
            date_range = (
                session.query(
                    func.min(NAVEntry.nav_date).label('earliest'),
                    func.max(NAVEntry.nav_date).label('latest')
                )
                .filter(query.whereclause if query.whereclause else True)
                .first()
            )

            # Check for missing series_numbers
            missing_series = (
                session.query(NAVEntry.isin, func.count().label('count'))
                .filter(NAVEntry.series_number.is_(None))
                .group_by(NAVEntry.isin)
                .all()
            )

            # Get series info for missing series numbers
            missing_series_info = []
            if missing_series:
                isins = [isin for isin, _ in missing_series]
                series_data = session.query(Series).filter(
                    Series.isin.in_(isins)).all()
                series_dict = {s.isin: s for s in series_data}

                for isin, count in missing_series:
                    series = series_dict.get(isin)
                    missing_series_info.append({
                        'isin': isin,
                        'count': count,
                        'series_exists': series is not None,
                        'series_number': series.series_number if series else None
                    })

            return {
                'total_entries': total_entries,
                'distribution_stats': [
                    {
                        'type': d_type,
                        'emitter': emitter,
                        'count': count
                    } for d_type, emitter, count in distribution_stats
                ],
                'date_range': {
                    'earliest': date_range.earliest if date_range else None,
                    'latest': date_range.latest if date_range else None
                },
                'missing_series_numbers': sum(count for _, count in missing_series),
                'missing_series_details': missing_series_info
            }

    def save_nav_entries(self, nav_df: pd.DataFrame, distribution_type: str, emitter: str) -> ImportResult:
        """
        Save NAV entries from DataFrame to database

        Returns:
            ImportResult containing counts of added, duplicate, and invalid entries
        """
        with self.SessionMaker() as session:
            # Get all valid ISINs from the series table
            valid_isins = set(isin[0]
                              for isin in session.query(Series.isin).all())

            print(f"\nProcessing {emitter} data:")
            print(f"Total rows in DataFrame: {len(nav_df)}")
            print(f"Valid ISINs in database: {len(valid_isins)}")

            # First, get existing entries to avoid duplicates (only check ISIN and date)
            existing_entries = set()
            for entry in session.query(NAVEntry.isin, NAVEntry.nav_date).all():
                existing_entries.add((entry.isin, entry.nav_date))

            print(f"Existing entries in database: {len(existing_entries)}")

            entries_to_add = []
            duplicates_count = 0
            invalid_series_count = 0

            for _, row in nav_df.iterrows():
                try:
                    nav_date = pd.to_datetime(
                        row['Valuation Period-End Date']).date()
                    isin = row['ISIN']
                    entry_key = (isin, nav_date)

                    # Skip if entry already exists
                    if entry_key in existing_entries:
                        duplicates_count += 1
                        continue

                    # Skip if series doesn't exist
                    if isin not in valid_isins:
                        print(f"Invalid ISIN: {isin}")
                        invalid_series_count += 1
                        continue

                    entry = NAVEntry(
                        isin=isin,
                        nav_date=nav_date,
                        nav_value=float(row['NAV']),
                        distribution_type=distribution_type,
                        emitter=emitter,
                        series_number=None  # We'll update this in a second pass
                    )
                    entries_to_add.append(entry)
                except Exception as e:
                    print(f"Error processing row: {row}")
                    print(f"Error details: {str(e)}")
                    continue

            print(f"\nResults for {emitter}:")
            print(f"Entries to add: {len(entries_to_add)}")
            print(f"Duplicates skipped: {duplicates_count}")
            print(f"Invalid series skipped: {invalid_series_count}")

            if entries_to_add:
                # First try bulk insert
                try:
                    session.bulk_save_objects(entries_to_add)
                    session.commit()
                    print(f"Successfully added {len(entries_to_add)} entries")
                except Exception as e:
                    # If bulk insert fails, try individual inserts
                    session.rollback()
                    print(f"Bulk insert failed: {str(e)}")
                    print("Falling back to individual inserts...")

                    added_count = 0
                    for entry in entries_to_add:
                        try:
                            session.add(entry)
                            session.commit()
                            added_count += 1
                        except Exception as e:
                            session.rollback()
                            print(
                                f"Failed to add entry for ISIN {entry.isin}: {str(e)}")
                            duplicates_count += 1
                            continue

                    # Update the entries_to_add list to only include successfully added entries
                    entries_to_add = entries_to_add[:added_count]
                    print(
                        f"Successfully added {added_count} entries individually")

                # Update series_number for the newly added entries
                if entries_to_add:
                    series_info = {isin: number for isin, number in session.query(
                        Series.isin, Series.series_number).all()}
                    for entry in entries_to_add:
                        entry.series_number = series_info.get(entry.isin)
                    session.bulk_save_objects(entries_to_add)
                    session.commit()
                    print("Updated series numbers for added entries")

            result = ImportResult(
                added_count=len(entries_to_add),
                duplicates_count=duplicates_count,
                invalid_series_count=invalid_series_count
            )
            return result

    def get_nav_history(self, isin: Optional[str] = None,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        page: int = 1,
                        per_page: int = 50,
                        series_number: Optional[str] = None) -> Dict[str, Any]:
        """
        Get NAV history with pagination

        Args:
            isin: Optional ISIN to filter by
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            page: Page number (1-based)
            per_page: Number of entries per page
            series_number: Optional series number to filter by

        Returns:
            Dict containing:
                - entries: List of NAVEntry objects for current page
                - total_pages: Total number of pages
                - total_entries: Total number of entries matching filters
        """
        with self.SessionMaker() as session:
            # Build base query
            query = session.query(NAVEntry)

            # Apply filters
            if isin:
                query = query.filter(NAVEntry.isin == isin)
            if series_number:
                # First find all ISINs with this series number
                series_isins = session.query(Series.isin).filter(
                    Series.series_number == series_number).all()
                if series_isins:
                    query = query.filter(NAVEntry.isin.in_(
                        [s[0] for s in series_isins]))
                else:
                    # If no series found with this number, return empty result
                    return {
                        'entries': [],
                        'total_pages': 0,
                        'total_entries': 0
                    }
            if start_date:
                query = query.filter(NAVEntry.nav_date >= start_date)
            if end_date:
                query = query.filter(NAVEntry.nav_date <= end_date)

            # Get total count for pagination
            total_entries = query.count()
            total_pages = math.ceil(total_entries / per_page)

            # Get paginated results
            entries = query.order_by(NAVEntry.nav_date.desc()) \
                .offset((page - 1) * per_page) \
                .limit(per_page) \
                .all()

            # Update series numbers from Series table
            if entries:
                # Get all unique ISINs from the entries
                isins = {entry.isin for entry in entries}
                # Get series info for these ISINs
                series_info = dict(
                    session.query(Series.isin, Series.series_number)
                    .filter(Series.isin.in_(isins))
                    .all()
                )
                # Update series numbers
                for entry in entries:
                    entry.series_number = series_info.get(entry.isin)

            return {
                'entries': entries,
                'total_pages': total_pages,
                'total_entries': total_entries
            }

    def import_historic_data(self, excel_path: str) -> Dict[str, ImportResult]:
        """
        Import historic NAV data from Excel file with multiple sheets (Weekly, Monthly, Daily)
        Sheet structure:
        - Row 6: Contains ISIN numbers starting from column E
        - Row 7 onwards: Contains dates in column E and NAV values in corresponding ISIN columns
        - First relevant data column is E (dates)
        - NAV values start from column F onwards

        Args:
            excel_path: Path to the Excel file

        Returns:
            Dict mapping sheet names to their ImportResult containing counts of added, duplicate, and invalid entries
        """
        results = {}
        sheet_configs = {
            'Weekly': {'type': 'weekly', 'usecols': 'E:BY'},
            'Monthly': {'type': 'monthly', 'usecols': 'E:EU'},
            'Daily': {'type': 'daily', 'usecols': 'E:F'}
        }

        try:
            for sheet_name, config in sheet_configs.items():
                try:
                    print(f"\nProcessing {sheet_name} sheet...")
                    distribution_type = config['type']
                    usecols = config['usecols']

                    # First, read the ISINs from row 6
                    isins_df = pd.read_excel(
                        excel_path,
                        sheet_name=sheet_name,
                        header=None,
                        nrows=1,
                        skiprows=5,  # Skip to row 6 (0-based index 5)
                        usecols=usecols
                    )

                    # Get ISINs (skip the first column which is 'Dates')
                    isins = isins_df.iloc[0, 1:].values

                    # Now read the actual data starting from row 7
                    df = pd.read_excel(
                        excel_path,
                        sheet_name=sheet_name,
                        header=None,
                        skiprows=6,  # Skip to row 7
                        usecols=usecols
                    )

                    # Rename columns
                    df.columns = ['Valuation Period-End Date'] + list(isins)

                    print(f"Debug - First few rows of raw data:")
                    print(df.head())

                    if sheet_name == 'Daily':
                        # For Daily, we only have one ISIN column
                        isin = isins[0]
                        df = df.rename(columns={isin: 'NAV'})
                        df['ISIN'] = isin
                    else:
                        # For Weekly and Monthly, melt the multiple ISIN columns
                        df = df.melt(
                            id_vars=['Valuation Period-End Date'],
                            var_name='ISIN',
                            value_name='NAV'
                        )

                    # Clean up the data
                    df = df.dropna(subset=['NAV'])
                    df['Valuation Period-End Date'] = pd.to_datetime(
                        df['Valuation Period-End Date'])

                    print(f"\nDebug - Processed data for {sheet_name}:")
                    print(f"Total rows: {len(df)}")
                    print(
                        f"Date range: {df['Valuation Period-End Date'].min()} to {df['Valuation Period-End Date'].max()}")
                    print("Sample of processed data:")
                    print(df.head())

                    # Use save_nav_entries to handle the import with the specific distribution type
                    results[sheet_name] = self.save_nav_entries(
                        df, distribution_type, 'HISTORIC')

                except Exception as e:
                    print(f"Error processing sheet {sheet_name}: {str(e)}")
                    print(f"Full error details:", e)
                    results[sheet_name] = ImportResult(
                        added_count=0, duplicates_count=0, invalid_series_count=0)

            return results

        except Exception as e:
            raise Exception(f"Error importing historic data: {str(e)}")
