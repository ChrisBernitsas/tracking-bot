import sqlite3
import os

# --- Configuration ---
DATABASE_PATH = "bedwars_database.db"

def count_players_in_tables():
    """
    Connects to the specified SQLite database, finds all user-created tables,
    and prints the number of rows in each table.
    """
    if not os.path.exists(DATABASE_PATH):
        print(f"Error: Database file not found at '{DATABASE_PATH}'")
        return

    try:
        # Connect to the database in read-only mode to be safe
        conn = sqlite3.connect(f'file:{DATABASE_PATH}?mode=ro', uri=True)
        cursor = conn.cursor()

        # Get the names of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()

        if not tables:
            print(f"No tables found in the database '{DATABASE_PATH}'.")
            return

        print(f"Player counts in '{DATABASE_PATH}':")
        print("-" * 30)

        for table_name_tuple in tables:
            table_name = table_name_tuple[0]
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                count = cursor.fetchone()[0]
                print(f"- {table_name}: {count} players")
            except sqlite3.Error as e:
                print(f"- Could not count rows in '{table_name}': {e}")

        print("-" * 30)

    except sqlite3.Error as e:
        print(f"An error occurred while accessing the database: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    count_players_in_tables()
