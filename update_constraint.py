from sqlalchemy import create_engine, text
from models import init_db, Base, NAVEntry
import sqlite3


def update_nav_entries_constraint():
    """
    Update the unique constraint on nav_entries table to only include isin and nav_date
    """
    # Create connection using sqlite3 directly for simplicity
    conn = sqlite3.connect('nav_data.db')
    cursor = conn.cursor()

    try:
        print("Backing up current data...")

        # Get data from current table
        cursor.execute(
            "SELECT id, isin, series_number, nav_date, nav_value, distribution_type, emitter, created_at FROM nav_entries")
        nav_entries = cursor.fetchall()
        print(f"Backed up {len(nav_entries)} records")

        # Drop the table
        print("Dropping old table...")
        cursor.execute("DROP TABLE IF EXISTS nav_entries")
        conn.commit()

        # Create the table with new schema using SQLAlchemy
        print("Creating new table with updated constraint...")
        engine = create_engine('sqlite:///nav_data.db')
        Base.metadata.create_all(engine)

        # Re-insert the data using sqlite3
        print("Restoring data...")
        if nav_entries:
            cursor.executemany(
                "INSERT INTO nav_entries (id, isin, series_number, nav_date, nav_value, distribution_type, emitter, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                nav_entries
            )
            conn.commit()
            print(f"Restored {len(nav_entries)} records")

        print("Constraint update completed successfully!")
    except Exception as e:
        print(f"Error updating constraint: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    update_nav_entries_constraint()
