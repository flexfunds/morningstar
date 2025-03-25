import pandas as pd
from sqlalchemy.orm import Session
from models import Series, Custodian, FeeStructure, SeriesStatus, NAVFrequency, FeeType, NAVEntry
from datetime import datetime


def parse_date(date_str):
    if pd.isna(date_str):
        return None
    try:
        return pd.to_datetime(date_str).date()
    except:
        return None


def parse_float(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except:
        return None


def parse_fee_value(fee_value, fee_name):
    """Parse different types of fee values."""
    if pd.isna(fee_value):
        return None

    fee_str = str(fee_value).strip()

    # Handle AUM-based tiered fees
    if '<' in fee_str:
        tiers = fee_str.split('\n')
        result = []
        for tier in tiers:
            if '<' in tier:
                threshold = float(tier.split('<')[1].split('MM')[0].strip())
                percentage = float(tier.split(
                    '=')[1].strip().replace('%', '')) / 100
                result.append(('aum_based', threshold, percentage, None))
        return result

    # Handle range-based fees (e.g., "15.00% - 30.00%")
    if ' - ' in fee_str and '%' in fee_str:
        try:
            min_pct, max_pct = fee_str.split(' - ')
            min_val = float(min_pct.replace('%', '')) / 100
            max_val = float(max_pct.replace('%', '')) / 100
            return [('range', None, min_val, max_val)]
        except:
            print(
                f"Could not parse range-based fee for {fee_name}: {fee_value}")
            return None

    # Handle simple percentage or fixed amount
    try:
        if '%' in fee_str:
            return [('fixed', None, float(fee_str.replace('%', '')) / 100, None)]
        else:
            return [('fixed', None, None, float(fee_str))]
    except:
        print(f"Could not parse fee value for {fee_name}: {fee_value}")
        return None


def import_series_data(excel_path: str, session: Session):
    # Clear existing data (preserving NAV entries)
    print("Clearing existing data (preserving NAV entries)...")
    session.query(FeeStructure).delete()
    session.query(Custodian).delete()
    session.query(Series).delete()
    session.commit()
    print("Existing data cleared.")

    # Read Excel file
    print("Reading Excel file...")
    df = pd.read_excel(excel_path)
    print(f"Found {len(df)} series to import.")

    # Process each row
    for idx, row in df.iterrows():
        if idx % 10 == 0:
            print(f"Processing series {idx + 1} of {len(df)}...")

        # Create Series entry
        series = Series(
            isin=row['ISIN'],
            common_code=row['Common Code'],
            series_number=row['Series Number'],
            series_name=row['Series Name'],
            status=SeriesStatus.ACTIVE if str(
                row['Status']).upper() == 'A' else SeriesStatus.INACTIVE,
            issuance_type=row['Issuance Type'],
            product_type=row['Product type'],
            issuance_date=parse_date(row['Issuance Date']),
            maturity_date=parse_date(row['Scheduled Maturity Date']),
            close_date=parse_date(row['Close Date']),
            issuer=row['Issuer'],
            relationship_manager=row['Relationship Manager'],
            series_region=row['Series Region'],
            portfolio_manager_jurisdiction=row['Portfolio Manager Country of Jurisdiction'],
            portfolio_manager=row['Portfolio Manager'],
            borrower=row['Borrower'],
            asset_manager=row['Asset Manager'],
            currency=row['Currency'],
            nav_frequency=NAVFrequency.DAILY if 'daily' in str(row['NAV Frequency']).lower() else
            NAVFrequency.WEEKLY if 'weekly' in str(row['NAV Frequency']).lower() else
            NAVFrequency.MONTHLY if 'monthly' in str(row['NAV Frequency']).lower() else
            NAVFrequency.QUARTERLY,
            issuance_principal_amount=parse_float(
                row['Issuance Principal Amount']),
            underlying_valuation_update=row['Underlying Valuation Update'],
            fees_frequency=row['Fees Frequency'],
            payment_method=row['Payment Method']
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

        for fee_name, default_category in fee_fields:
            fee_value = row.get(fee_name)
            parsed_fees = parse_fee_value(fee_value, fee_name)

            if parsed_fees:
                for fee_type, threshold, value1, value2 in parsed_fees:
                    if fee_type == 'aum_based':
                        fee = FeeStructure(
                            series_isin=row['ISIN'],
                            fee_type=fee_name,
                            fee_type_category=FeeType.AUM_BASED,
                            aum_threshold=threshold,
                            fee_percentage=value1
                        )
                    elif fee_type == 'range':
                        # For range-based fees, store min and max as separate entries
                        fee_min = FeeStructure(
                            series_isin=row['ISIN'],
                            fee_type=f"{fee_name} (Min)",
                            fee_type_category=FeeType.FIXED,
                            fee_percentage=value1
                        )
                        fee_max = FeeStructure(
                            series_isin=row['ISIN'],
                            fee_type=f"{fee_name} (Max)",
                            fee_type_category=FeeType.FIXED,
                            fee_percentage=value2
                        )
                        session.add(fee_min)
                        session.add(fee_max)
                        continue
                    else:  # fixed
                        fee = FeeStructure(
                            series_isin=row['ISIN'],
                            fee_type=fee_name,
                            fee_type_category=FeeType.FIXED,
                            fee_percentage=value1,
                            fixed_amount=value2,
                            currency=row['Currency']
                        )
                    session.add(fee)

    # Commit all changes
    session.commit()


if __name__ == "__main__":
    from models import init_db
    import sys

    if len(sys.argv) != 2:
        print("Usage: python import_data.py <excel_file_path>")
        sys.exit(1)

    excel_path = sys.argv[1]
    Session = init_db()

    with Session() as session:
        import_series_data(excel_path, session)
