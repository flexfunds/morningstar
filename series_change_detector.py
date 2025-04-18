import pandas as pd
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import os
from models import Series, SeriesStatus, NAVFrequency, Custodian, FeeStructure
from sqlalchemy.orm import Session
from import_data import parse_date, parse_float
import glob


@dataclass
class SeriesChange:
    isin: str
    change_type: str
    field_name: str
    old_value: Any
    new_value: Any
    series_number: str = None
    nav_frequency: str = None  # Add NAV frequency field


class SeriesChangeDetector:
    # Fields that we want to specifically track for changes
    IMPORTANT_FIELDS = [
        'ISIN',
        'Series Number',
        'Series Name',
        'Status',
        'Issuance Date',
        'Scheduled Maturity Date',
        'Close Date',
        'Portfolio Manager',
        'Asset Manager',
        'Currency',
        'NAV Frequency',
        'Issuance Principal Amount'
    ]

    def __init__(self, master_file_path: str, session_maker=None):
        """
        Initialize the change detector with the path to the master file.

        Args:
            master_file_path (str): Path to the master Series Qualitative Data file
            session_maker: SQLAlchemy session maker for database operations
        """
        self.master_file_path = master_file_path
        self.master_data = pd.read_excel(master_file_path)
        self.session_maker = session_maker

        # Ensure required columns exist in master file
        required_columns = ['ISIN', 'Series Number', 'NAV Frequency']
        missing_columns = [
            col for col in required_columns if col not in self.master_data.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in master file: {', '.join(missing_columns)}")

        self.master_data.set_index('ISIN', inplace=True)

    def _get_safe_value(self, df: pd.DataFrame, isin: str, column: str) -> str:
        """Safely get a value from a dataframe, handling missing values and NaN"""
        try:
            value = df.loc[isin, column]
            return str(value) if pd.notna(value) else 'N/A'
        except:
            return 'N/A'

    def detect_changes(self, new_file_path: str) -> List[SeriesChange]:
        """
        Compare a new file against the master file and detect all changes.

        Args:
            new_file_path (str): Path to the new Series Qualitative Data file

        Returns:
            List[SeriesChange]: List of detected changes
        """
        new_data = pd.read_excel(new_file_path)

        # Ensure required columns exist in new file
        required_columns = ['ISIN', 'Series Number', 'NAV Frequency']
        missing_columns = [
            col for col in required_columns if col not in new_data.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in new file: {', '.join(missing_columns)}")

        new_data.set_index('ISIN', inplace=True)

        changes: List[SeriesChange] = []

        # Detect new series
        new_isins = set(new_data.index) - set(self.master_data.index)
        for isin in new_isins:
            changes.append(SeriesChange(
                isin=isin,
                change_type='NEW_SERIES',
                field_name='',
                old_value=None,
                new_value=None,
                series_number=self._get_safe_value(
                    new_data, isin, 'Series Number'),
                nav_frequency=self._get_safe_value(
                    new_data, isin, 'NAV Frequency')
            ))

        # Detect removed series
        removed_isins = set(self.master_data.index) - set(new_data.index)
        for isin in removed_isins:
            changes.append(SeriesChange(
                isin=isin,
                change_type='REMOVED_SERIES',
                field_name='',
                old_value=None,
                new_value=None,
                series_number=self._get_safe_value(
                    self.master_data, isin, 'Series Number'),
                nav_frequency=self._get_safe_value(
                    self.master_data, isin, 'NAV Frequency')
            ))

        # Detect changes in existing series
        common_isins = set(new_data.index) & set(self.master_data.index)
        for isin in common_isins:
            for field in self.IMPORTANT_FIELDS:
                if field == 'ISIN':
                    continue

                old_value = self.master_data.loc[isin, field]
                new_value = new_data.loc[isin, field]

                # Handle NaN comparisons
                if pd.isna(old_value) and pd.isna(new_value):
                    continue

                if pd.isna(old_value) != pd.isna(new_value) or old_value != new_value:
                    changes.append(SeriesChange(
                        isin=isin,
                        change_type='FIELD_UPDATE',
                        field_name=field,
                        old_value=old_value,
                        new_value=new_value,
                        series_number=self._get_safe_value(
                            new_data, isin, 'Series Number'),
                        nav_frequency=self._get_safe_value(
                            new_data, isin, 'NAV Frequency')
                    ))

        return changes

    def generate_change_report(self, changes: List[SeriesChange]) -> str:
        """
        Generate a human-readable report of the changes.

        Args:
            changes (List[SeriesChange]): List of detected changes

        Returns:
            str: Formatted report of changes
        """
        if not changes:
            return "No changes detected."

        def format_value(value):
            """Helper function to format values, especially dates"""
            if pd.isna(value):
                return 'None'
            if isinstance(value, pd.Timestamp):
                return value.strftime('%Y-%m-%d')
            return str(value)

        report = []
        report.append("Change Report")
        report.append("=" * 80)

        # Group changes by type
        new_series = [c for c in changes if c.change_type == 'NEW_SERIES']
        removed_series = [
            c for c in changes if c.change_type == 'REMOVED_SERIES']
        field_updates = [c for c in changes if c.change_type == 'FIELD_UPDATE']

        # Report new series
        if new_series:
            report.append("\nNew Series Added:")
            report.append("-" * 40)
            for change in new_series:
                report.append(
                    f"- ISIN: {change.isin} (Series Number: {change.series_number}, NAV Frequency: {change.nav_frequency})")

        # Report removed series
        if removed_series:
            report.append("\nSeries Removed:")
            report.append("-" * 40)
            for change in removed_series:
                report.append(
                    f"- ISIN: {change.isin} (Series Number: {change.series_number}, NAV Frequency: {change.nav_frequency})")

        # Report field updates
        if field_updates:
            report.append("\nField Updates:")
            report.append("-" * 40)
            # Group by ISIN
            updates_by_isin: Dict[str, List[SeriesChange]] = {}
            for change in field_updates:
                if change.isin not in updates_by_isin:
                    updates_by_isin[change.isin] = []
                updates_by_isin[change.isin].append(change)

            for isin, updates in updates_by_isin.items():
                report.append(
                    f"\nISIN: {isin} (Series Number: {updates[0].series_number}, NAV Frequency: {updates[0].nav_frequency})")
                for update in updates:
                    report.append(f"  - {update.field_name}:")
                    report.append(
                        f"    From: {format_value(update.old_value)}")
                    report.append(
                        f"    To:   {format_value(update.new_value)}")

        return "\n".join(report)

    def _parse_fee_value(self, value: Any) -> Tuple[float, float, str]:
        """
        Parse a fee value that might be in various formats.
        Returns a tuple of (percentage, fixed_amount, notes).
        """
        if pd.isna(value):
            return None, None, None

        value_str = str(value).strip()

        # Handle empty or zero values
        if not value_str or value_str == '0' or value_str.lower() == 'n/a':
            return 0.0, 0.0, None

        # Handle percentage ranges (e.g., "15.00% - 30.00%")
        if ' - ' in value_str and '%' in value_str:
            # Store the range in notes and use the lower value for calculation
            lower = float(value_str.split(' - ')
                          [0].replace('%', '').strip()) / 100
            return lower, None, value_str

        # Handle simple percentages
        if '%' in value_str:
            try:
                return float(value_str.replace('%', '').strip()) / 100, None, None
            except ValueError:
                return None, None, value_str

        # Handle numeric values
        try:
            float_val = float(value_str)
            return None, float_val, None
        except ValueError:
            # If we can't parse it as a number, store it as a note
            return None, None, value_str

    def _sync_with_database(self, df: pd.DataFrame, session: Session):
        """
        Sync the Excel data with the database.

        Args:
            df (pd.DataFrame): DataFrame containing the series data
            session (Session): SQLAlchemy session
        """
        # First, clear existing fee structures and custodians
        session.query(FeeStructure).delete()
        session.query(Custodian).delete()

        # Process each row
        for idx, row in df.iterrows():
            # Check if series exists
            series = session.query(Series).filter(
                Series.isin == row['ISIN']).first()

            if series:
                # Update existing series
                series.common_code = row.get('Common Code')
                series.series_number = row.get('Series Number')
                series.series_name = row['Series Name']
                series.status = SeriesStatus.ACTIVE if str(
                    row.get('Status', '')).upper() == 'A' else SeriesStatus.INACTIVE
                series.issuance_type = row.get('Issuance Type')
                series.product_type = row.get('Product type')
                series.issuance_date = parse_date(row.get('Issuance Date'))
                series.maturity_date = parse_date(
                    row.get('Scheduled Maturity Date'))
                series.close_date = parse_date(row.get('Close Date'))
                series.issuer = row.get('Issuer')
                series.relationship_manager = row.get('Relationship Manager')
                series.series_region = row.get('Series Region')
                series.portfolio_manager_jurisdiction = row.get(
                    'Portfolio Manager Country of Jurisdiction')
                series.portfolio_manager = row.get('Portfolio Manager')
                series.borrower = row.get('Borrower')
                series.asset_manager = row.get('Asset Manager')
                series.currency = row.get('Currency')
                series.nav_frequency = (
                    NAVFrequency.DAILY if 'daily' in str(row.get('NAV Frequency', '')).lower()
                    else NAVFrequency.WEEKLY if 'weekly' in str(row.get('NAV Frequency', '')).lower()
                    else NAVFrequency.MONTHLY if 'monthly' in str(row.get('NAV Frequency', '')).lower()
                    else NAVFrequency.QUARTERLY
                )
                series.issuance_principal_amount = parse_float(
                    row.get('Issuance Principal Amount'))
                series.underlying_valuation_update = row.get(
                    'Underlying Valuation Update')
                series.fees_frequency = row.get('Fees Frequency')
                series.payment_method = row.get('Payment Method')
            else:
                # Create new series
                series = Series(
                    isin=row['ISIN'],
                    common_code=row.get('Common Code'),
                    series_number=row.get('Series Number'),
                    series_name=row['Series Name'],
                    status=SeriesStatus.ACTIVE if str(
                        row.get('Status', '')).upper() == 'A' else SeriesStatus.INACTIVE,
                    issuance_type=row.get('Issuance Type'),
                    product_type=row.get('Product type'),
                    issuance_date=parse_date(row.get('Issuance Date')),
                    maturity_date=parse_date(
                        row.get('Scheduled Maturity Date')),
                    close_date=parse_date(row.get('Close Date')),
                    issuer=row.get('Issuer'),
                    relationship_manager=row.get('Relationship Manager'),
                    series_region=row.get('Series Region'),
                    portfolio_manager_jurisdiction=row.get(
                        'Portfolio Manager Country of Jurisdiction'),
                    portfolio_manager=row.get('Portfolio Manager'),
                    borrower=row.get('Borrower'),
                    asset_manager=row.get('Asset Manager'),
                    currency=row.get('Currency'),
                    nav_frequency=(
                        NAVFrequency.DAILY if 'daily' in str(row.get('NAV Frequency', '')).lower()
                        else NAVFrequency.WEEKLY if 'weekly' in str(row.get('NAV Frequency', '')).lower()
                        else NAVFrequency.MONTHLY if 'monthly' in str(row.get('NAV Frequency', '')).lower()
                        else NAVFrequency.QUARTERLY
                    ),
                    issuance_principal_amount=parse_float(
                        row.get('Issuance Principal Amount')),
                    underlying_valuation_update=row.get(
                        'Underlying Valuation Update'),
                    fees_frequency=row.get('Fees Frequency'),
                    payment_method=row.get('Payment Method')
                )
                session.add(series)

            # Add Custodians
            for i in range(1, 4):  # We have Custodian 1, 2, and 3
                custodian_name = row.get(f'Custodian {i}')
                account_number = row.get(f'Custodian {i} Account Number')

                if pd.notna(custodian_name):
                    custodian = Custodian(
                        series_isin=row['ISIN'],
                        custodian_name=custodian_name,
                        account_number=account_number if pd.notna(
                            account_number) else None
                    )
                    session.add(custodian)

            # Add Fee Structures
            fee_fields = [
                ('Arranger Fee', 'AUM_BASED'),
                ('Maintenance Fee', 'AUM_BASED'),
                ('Set Up Fees', 'FIXED'),
                ('Price Dissemination Fee', 'FIXED'),
                ('Inventory Cost', 'FIXED'),
                ('Notes Registration Fee', 'FIXED'),
                ('Technology Service Charge', 'FIXED'),
                ('Performance Fee', 'FIXED'),
                ('Trustee / Corporate Fees', 'FIXED'),
                ('Auditor Fee', 'FIXED'),
                ('Transfer Agent Fee', 'FIXED'),
                ('Ad hoc NAV', 'FIXED')
            ]

            for fee_name, fee_category in fee_fields:
                fee_value = row.get(fee_name)
                if pd.notna(fee_value):
                    percentage, fixed_amount, notes = self._parse_fee_value(
                        fee_value)

                    fee = FeeStructure(
                        series_isin=row['ISIN'],
                        fee_type=fee_name,
                        fee_type_category=fee_category,
                        fee_percentage=percentage if fee_category == 'AUM_BASED' or percentage is not None else None,
                        fixed_amount=fixed_amount if fee_category == 'FIXED' or fixed_amount is not None else None,
                        currency=row.get(
                            'Currency') if fee_category == 'FIXED' else None,
                        notes=notes
                    )
                    session.add(fee)

        # Commit all changes
        session.commit()

    def _cleanup_backups(self, backup_dir: str, keep_count: int = 5) -> None:
        """
        Clean up old backup files, keeping only the specified number of most recent backups.

        Args:
            backup_dir (str): Directory containing backup files
            keep_count (int): Number of most recent backups to keep
        """
        # Get list of backup files
        backup_pattern = os.path.join(
            backup_dir, 'Series_Qualitative_Data_backup_*.xlsx')
        backup_files = glob.glob(backup_pattern)

        # If we have more backups than we want to keep
        if len(backup_files) > keep_count:
            # Sort files by modification time (oldest first)
            backup_files.sort(key=lambda x: os.path.getmtime(x))

            # Remove oldest files, keeping only the most recent ones
            files_to_remove = backup_files[:-keep_count]
            for file_path in files_to_remove:
                try:
                    os.remove(file_path)
                except OSError as e:
                    print(f"Error removing backup file {file_path}: {e}")

    def update_master_file(self, new_file_path: str, backup: bool = True) -> None:
        """
        Update the master file with the new data, optionally creating a backup.

        Args:
            new_file_path (str): Path to the new Series Qualitative Data file
            backup (bool): Whether to create a backup of the current master file
        """
        if backup:
            backup_dir = os.path.join(os.path.dirname(
                self.master_file_path), 'backups')
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(
                backup_dir,
                f'Series_Qualitative_Data_backup_{timestamp}.xlsx'
            )
            self.master_data.to_excel(backup_path)

            # Clean up old backups after creating new one
            self._cleanup_backups(backup_dir)

        # Update master file
        new_data = pd.read_excel(new_file_path)
        new_data.to_excel(self.master_file_path)
        self.master_data = new_data.set_index('ISIN')

        # Sync with database if session maker is available
        if self.session_maker:
            with self.session_maker() as session:
                self._sync_with_database(new_data, session)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Detect changes in Series Qualitative Data files')
    parser.add_argument(
        'master_file', help='Path to the master Series Qualitative Data file')
    parser.add_argument(
        'new_file', help='Path to the new Series Qualitative Data file')
    parser.add_argument('--update', action='store_true',
                        help='Update master file with new data')

    args = parser.parse_args()

    detector = SeriesChangeDetector(args.master_file)
    changes = detector.detect_changes(args.new_file)
    print(detector.generate_change_report(changes))

    if args.update:
        detector.update_master_file(args.new_file)
        print("\nMaster file has been updated with the new data.")


if __name__ == '__main__':
    main()
