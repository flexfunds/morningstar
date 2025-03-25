from models import Series, Custodian, FeeStructure, init_db, NAVEntry
from db_service import DatabaseService
import argparse
from typing import List
from sqlalchemy import func


def check_specific_isins(session, isins):
    """Check specific ISINs for series data and NAV entries"""
    print("\nChecking specific ISINs:")
    print("-" * 80)

    # Get series data for these ISINs
    series_data = session.query(Series).filter(Series.isin.in_(isins)).all()
    series_dict = {s.isin: s for s in series_data}

    # Get NAV entry counts for these ISINs
    nav_counts = dict(
        session.query(NAVEntry.isin, func.count())
        .filter(NAVEntry.isin.in_(isins))
        .group_by(NAVEntry.isin)
        .all()
    )

    # Get counts of NAV entries with null series numbers
    null_counts = dict(
        session.query(NAVEntry.isin, func.count())
        .filter(NAVEntry.isin.in_(isins))
        .filter(NAVEntry.series_number.is_(None))
        .group_by(NAVEntry.isin)
        .all()
    )

    # Get a sample NAV entry for each ISIN
    sample_navs = {}
    for isin in isins:
        sample_nav = session.query(NAVEntry).filter(
            NAVEntry.isin == isin).first()
        if sample_nav:
            sample_navs[isin] = sample_nav

    missing_series = []
    series_without_number = []
    for isin in isins:
        series = series_dict.get(isin)
        nav_count = nav_counts.get(isin, 0)
        null_count = null_counts.get(isin, 0)
        sample_nav = sample_navs.get(isin)

        print(f"\nISIN: {isin}")
        if series:
            print(f"Series exists in database:")
            print(f"  Series Number: {series.series_number}")
            print(f"  Series Name: {series.series_name}")
            print(f"  Status: {series.status}")
            if not series.series_number:
                series_without_number.append(isin)
                print("  WARNING: Series exists but has no series_number!")
        else:
            missing_series.append(isin)
            print("WARNING: No series found in database")

        print(f"NAV Entry Information:")
        print(f"  Total NAV entries: {nav_count}")
        print(f"  NAV entries with null series number: {null_count}")
        if sample_nav:
            print(f"  Sample NAV entry:")
            print(f"    Date: {sample_nav.nav_date}")
            print(f"    Value: {sample_nav.nav_value}")
            print(f"    Distribution Type: {sample_nav.distribution_type}")
            print(f"    Emitter: {sample_nav.emitter}")

    if missing_series:
        print("\nSummary of missing series:")
        print("-" * 80)
        print("The following ISINs do not exist in the Series table:")
        for isin in missing_series:
            print(f"- {isin}")

    if series_without_number:
        print("\nSeries without series_number:")
        print("-" * 80)
        print("The following ISINs have series records but no series_number:")
        for isin in series_without_number:
            print(f"- {isin}")

    return missing_series, series_without_number


def verify_import(fix_missing: bool = False, check_isins: List[str] = None):
    Session = init_db()
    db_service = DatabaseService()

    with Session() as session:
        if check_isins:
            missing_series, series_without_number = check_specific_isins(
                session, check_isins)
            if fix_missing and not missing_series and not series_without_number:
                print("\nAttempting to fix missing series numbers...")
                fix_results = db_service.fix_missing_series_numbers()

                if 'error' in fix_results:
                    print("\nError:", fix_results['error'])
                    print("Affected ISINs:")
                    for isin in fix_results['affected_isins']:
                        print(f"- {isin}")
                    return

                print(f"\nFix Results:")
                print(
                    f"- Entries with missing series numbers before: {fix_results['missing_before']}")
                print(f"- Entries updated: {fix_results['updated_count']}")
                if fix_results['error_count'] > 0:
                    print(
                        f"- Errors encountered: {fix_results['error_count']}")

                if fix_results['missing_isins']:
                    print("\nISINs with missing series records:")
                    for isin in fix_results['missing_isins']:
                        print(f"- {isin}")

                # Verify the fix for these specific ISINs
                print("\nVerifying fix for specified ISINs:")
                null_counts_after = dict(
                    session.query(NAVEntry.isin, func.count())
                    .filter(NAVEntry.isin.in_(check_isins))
                    .filter(NAVEntry.series_number.is_(None))
                    .group_by(NAVEntry.isin)
                    .all()
                )

                if null_counts_after:
                    print("\nRemaining null series numbers after fix:")
                    for isin, count in null_counts_after.items():
                        print(
                            f"- ISIN {isin}: {count} entries still have null series numbers")
                else:
                    print("All specified ISINs have been fixed successfully!")
            return

        # Count total series
        series_count = session.query(Series).count()
        print(f"\nSeries Information:")
        print(f"Total series imported: {series_count}")

        # Count custodians
        custodian_count = session.query(Custodian).count()
        print(f"Total custodian relationships: {custodian_count}")

        # Count fee structures
        fee_count = session.query(FeeStructure).count()
        print(f"Total fee structures: {fee_count}")

        # Sample a series with its relationships
        sample_series = session.query(Series).first()
        if sample_series:
            print(f"\nSample Series:")
            print(f"ISIN: {sample_series.isin}")
            print(f"Name: {sample_series.series_name}")
            print(f"Status: {sample_series.status}")

            print("\nCustodians:")
            for custodian in sample_series.custodians:
                print(
                    f"- {custodian.custodian_name} (Account: {custodian.account_number})")

            print("\nFee Structures:")
            for fee in sample_series.fee_structures:
                if fee.fee_type_category.value == "AUM Based":
                    print(
                        f"- {fee.fee_type}: {fee.fee_percentage*100}% (AUM threshold: {fee.aum_threshold}MM)")
                else:
                    if fee.fee_percentage:
                        print(f"- {fee.fee_type}: {fee.fee_percentage*100}%")
                    elif fee.fixed_amount:
                        print(
                            f"- {fee.fee_type}: {fee.fixed_amount} {fee.currency}")

        # Verify NAV entries
        print("\nNAV Entry Statistics:")
        nav_stats = db_service.verify_nav_entries()
        print(f"Total NAV entries: {nav_stats['total_entries']}")

        if nav_stats['distribution_stats']:
            print("\nDistribution by type:")
            for stat in nav_stats['distribution_stats']:
                print(
                    f"- {stat['type']} ({stat['emitter']}): {stat['count']} entries")

        if nav_stats['date_range']['earliest'] and nav_stats['date_range']['latest']:
            print(f"\nDate Range:")
            print(f"Earliest entry: {nav_stats['date_range']['earliest']}")
            print(f"Latest entry: {nav_stats['date_range']['latest']}")

        if nav_stats['missing_series_numbers'] > 0:
            print(
                f"\nWarning: {nav_stats['missing_series_numbers']} NAV entries are missing series numbers")
            print("\nMissing series numbers by ISIN:")
            for info in nav_stats['missing_series_details']:
                status = "Series exists" if info['series_exists'] else "Series not found"
                series_number = f", Series number: {info['series_number']}" if info['series_exists'] else ""
                print(
                    f"- ISIN: {info['isin']}, Count: {info['count']} ({status}{series_number})")

            if fix_missing:
                print("\nAttempting to fix missing series numbers...")
                fix_results = db_service.fix_missing_series_numbers()

                if 'error' in fix_results:
                    print("\nError:", fix_results['error'])
                    print("Affected ISINs:")
                    for isin in fix_results['affected_isins']:
                        print(f"- {isin}")
                    return

                print(f"\nFix Results:")
                print(
                    f"- Entries with missing series numbers before: {fix_results['missing_before']}")
                print(f"- Entries updated: {fix_results['updated_count']}")
                if fix_results['error_count'] > 0:
                    print(
                        f"- Errors encountered: {fix_results['error_count']}")

                if fix_results['missing_isins']:
                    print("\nISINs with missing series records:")
                    for isin in fix_results['missing_isins']:
                        print(f"- {isin}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Verify database import and optionally fix missing series numbers')
    parser.add_argument('--fix', action='store_true',
                        help='Fix missing series numbers')
    parser.add_argument('--isins', nargs='+', help='Check specific ISINs')
    args = parser.parse_args()

    verify_import(fix_missing=args.fix, check_isins=args.isins)
