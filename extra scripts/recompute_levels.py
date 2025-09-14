import sqlite3
import os

DATABASE_PATH = "bedwars_database.db"

def calculate_bedwars_level(experience: int) -> int:
    """
    Calculates Bedwars level based on the official Hypixel formula.
    See: https://hypixel.net/threads/bed-wars-prestige-guide.702408/
    """
    xp = int(experience)
    
    # XP required for each prestige (100 levels)
    # 4 levels at reduced cost: 500 + 1000 + 2000 + 3500 = 7000
    # 96 levels at normal cost: 96 * 5000 = 480000
    xp_for_prestige = 487000
    
    prestiges = xp // xp_for_prestige
    level = prestiges * 100
    
    remaining_xp = xp % xp_for_prestige
    
    # XP costs for the first 4 levels of any prestige
    costs = [500, 1000, 2000, 3500]
    
    levels_in_prestige = 0
    for cost in costs:
        if remaining_xp >= cost:
            remaining_xp -= cost
            levels_in_prestige += 1
        else:
            break
    
    # For levels 5 and beyond in the current prestige
    levels_from_normal_xp = remaining_xp // 5000
    levels_in_prestige += levels_from_normal_xp
    
    level += levels_in_prestige
            
    return level

def recompute_all_levels():
    """
    Iterates through all players, recalculates their Bedwars level based on
    their latest experience points, and updates the database.
    """
    if not os.path.exists(DATABASE_PATH):
        print(f"Error: Database file not found at '{DATABASE_PATH}'")
        return

    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()

        # Get all players that need updating
        cursor.execute("SELECT uuid, username, bedwars_level FROM players")
        players_to_update = cursor.fetchall()

        if not players_to_update:
            print("No players found in the database.")
            return

        print(f"Found {len(players_to_update)} players to check for re-computation...")
        
        updates_to_perform = []
        for uuid, username, old_level in players_to_update:
            # Find the most recent experience points for the player
            cursor.execute(
                "SELECT experience FROM bedwars_stats WHERE uuid = ? ORDER BY timestamp DESC LIMIT 1",
                (uuid,)
            )
            result = cursor.fetchone()
            
            if result:
                experience = result[0]
                new_level = calculate_bedwars_level(experience)
                
                if new_level != old_level:
                    updates_to_perform.append((new_level, uuid))
                    print(f"- {username}: Level {old_level} -> {new_level} (XP: {experience})")
            else:
                print(f"- {username}: No stats found in bedwars_stats. Skipping.")

        if not updates_to_perform:
            print("\nAll player levels are already up-to-date!")
            return
            
        print(f"\nFound {len(updates_to_perform)} players with incorrect levels. Updating now...")

        # Perform all updates in a single transaction
        cursor.executemany(
            "UPDATE players SET bedwars_level = ? WHERE uuid = ?",
            updates_to_perform
        )
        conn.commit()
        
        print(f"\nSuccessfully recomputed and updated {len(updates_to_perform)} player levels.")

    except sqlite3.Error as e:
        print(f"\nAn error occurred during the database operation: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    recompute_all_levels()
