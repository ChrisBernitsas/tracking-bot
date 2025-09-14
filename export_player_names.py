import sqlite3
import os

DATABASE_PATH = "bedwars_database.db"
OUTPUT_FILE = "all_player_names.txt"

def export_player_names():
    if not os.path.exists(DATABASE_PATH):
        print(f"Error: Database file not found at '{DATABASE_PATH}'")
        return

    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT username FROM players")
        player_names = cursor.fetchall()

        if not player_names:
            print("No player names found in the database.")
            return

        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            for name_tuple in player_names:
                f.write(name_tuple[0] + '\n')
        
        print(f"Successfully exported {len(player_names)} player names to '{OUTPUT_FILE}'.")

    except sqlite3.Error as e:
        print(f"An error occurred during database operation: {e}")
    except IOError as e:
        print(f"An error occurred during file writing: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    export_player_names()
