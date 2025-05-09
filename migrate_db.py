#!/usr/bin/env python
"""
Simple database migration script to update the schema for the Trade model.
Use this if you don't have important data in your database or have already backed it up.
"""
import os
import sys
from sqlalchemy import create_engine
from models import Base, Trade
import sqlite3


def migrate_database(db_path='sqlite:///nav_data.db', backup=True):
    """Update the database schema by dropping and recreating the trades table"""

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
        # Connect using sqlite3 to drop table
        print("Dropping trades table...")
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS trades")
        conn.commit()
        conn.close()

        # Recreate table with updated schema
        print("Recreating trades table with new schema...")
        Base.metadata.create_all(engine, tables=[Trade.__table__])

        print("Migration completed successfully.")
        return True
    except Exception as e:
        print(f"Error during migration: {str(e)}")
        return False


if __name__ == "__main__":
    # Allow specifying database path from command line
    db_path = sys.argv[1] if len(sys.argv) > 1 else 'sqlite:///nav_data.db'

    print(f"Starting migration for database: {db_path}")
    result = migrate_database(db_path)

    if result:
        print("Migration completed successfully.")
    else:
        print("Migration failed.")
