### Step 0: Scrape
*   **Script:** `bot.mjs`
*   **Action:** The Mineflayer bot sees a player's name in-game and writes it to the `scraped_names_to_process.txt` file.

---

### Step 1: Ingest (Text File -> Table 1)
*   **Script:** `uuid_ingestor.py`
*   **Action:** This script reads the names from the text file, gets their UUIDs from the Mojang API, and adds them to the **`player_discovery`** table (our "to-do list").

---

### Step 2: Process (Table 1 -> Tables 2 & 3)
*   **Script:** `leaderboard_tracker.py`
*   **Action:** This is the main processing script. It takes a UUID from the `player_discovery` "to-do list", makes **one** call to the Hypixel API to get all stats, and then:
    1.  Adds the player's permanent info (UUID, name, calculated level) to the **`players`** table.
    2.  Adds the detailed stats snapshot to the **`bedwars_stats`** table.
    3.  Marks the player as "processed" in the `player_discovery` table.

---

### Step 3: Track (Ongoing Monitoring)
*   **Script:** `bedwars_stats.py`
*   **Action:** This script reads from the final **`players`** table to get its list of who to watch. It then periodically checks those players for new game sessions and logs their progress.
