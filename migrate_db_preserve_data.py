#!/usr/bin/env python
"""
Advanced database migration script for Trade model changes.
This script preserves existing trade data while updating the schema.
"""
import os
import sys
from sqlalchemy import create_engine, MetaData, Table, Column, String, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from models import Base, Trade
import sqlite3
import pandas as pd


def migrate_database_with_data_preservation(db_path='sqlite:///nav_data.db', backup=True):
    """Update the database schema for the trades table while preserving existing data"""

    # Extract file path from SQLAlchemy URL
    db_file = db_path.replace('sqlite:///', '')

    # Check if database exists
    if not os.path.exists(db_file):
        print(f"Database file {db_file} not found.")
        return False

    # Create backup if requested
    if backup:
        backup_file = f"{db_file}.backup"
        print(f"Creating backup of database at {backup_file}")
        try:
            import shutil
            shutil.copy2(db_file, backup_file)
            print("Backup created successfully.")
        except Exception as e:
            print(f"Error creating backup: {str(e)}")
            choice = input("Continue without backup? (y/n): ")
            if choice.lower() != 'y':
                print("Migration aborted.")
                return False

    # Connect to database
    engine = create_engine(db_path)

    try:
        # Check if trades table exists
        inspector = inspect(engine)
        if 'trades' not in inspector.get_table_names():
            print("Trades table does not exist. Creating with new schema...")
            Base.metadata.create_all(engine, tables=[Trade.__table__])
            return True

        print("Checking if migration is needed...")

        # Get existing columns
        columns = inspector.get_columns('trades')
        col_names = [col['name'] for col in columns]
        col_dict = {col['name']: col for col in columns}

        # Check if migration is needed
        broker_needs_nullable = False
        account_needs_nullable = False

        if 'broker' in col_dict and not col_dict['broker'].get('nullable', False):
            broker_needs_nullable = True

        if 'account' in col_dict and not col_dict['account'].get('nullable', False):
            account_needs_nullable = True

        if not (broker_needs_nullable or account_needs_nullable):
            print("No migration needed. Schema is already up to date.")
            return True

        print("Migration needed. Backing up existing data...")

        # Use pandas to read existing data
        trades_df = pd.read_sql_table('trades', engine)
        row_count = len(trades_df)
        print(f"Backed up {row_count} trade records.")

        print("Dropping existing trades table...")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS trades")
        conn.commit()
        conn.close()

        print("Creating new trades table with updated schema...")
        Base.metadata.create_all(engine, tables=[Trade.__table__])

        if row_count > 0:
            print(f"Restoring {row_count} trade records...")

            # Convert None or NaN values to appropriate defaults
            if 'broker' in trades_df.columns:
                trades_df['broker'] = trades_df['broker'].fillna(None)

            if 'account' in trades_df.columns:
                trades_df['account'] = trades_df['account'].fillna(
                    'DEFAULT_ACCOUNT')

            # Create a session
            Session = sessionmaker(bind=engine)
            session = Session()

            # Insert data in batches
            batch_size = 100
            for i in range(0, row_count, batch_size):
                batch = trades_df.iloc[i:i+batch_size]
                trades = []

                for _, row in batch.iterrows():
                    # Convert row to dict and filter out columns that don't exist in model
                    trade_data = {
                        k: v for k, v in row.to_dict().items() if hasattr(Trade, k)}
                    trades.append(Trade(**trade_data))

                session.add_all(trades)
                session.commit()
                print(f"Restored records {i+1}-{min(i+batch_size, row_count)}")

            session.close()
            print(f"Successfully restored all {row_count} trade records.")

        print("Migration completed successfully.")
        return True

    except Exception as e:
        print(f"Error during migration: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Allow specifying database path from command line
    db_path = sys.argv[1] if len(sys.argv) > 1 else 'sqlite:///nav_data.db'

    print(f"Starting migration for database: {db_path}")
    result = migrate_database_with_data_preservation(db_path)

    if result:
        print("Migration completed successfully.")
    else:
        print("Migration failed.")
