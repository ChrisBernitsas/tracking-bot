import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import random

import requests

class Config:
    API_KEY = "e90f0bca-1575-4639-a684-1089240e1bfb" # Placeholder, use your actual Hypixel API key
    DATABASE_PATH = "bedwars_database.db"
    NAMES_DIR = "player_names"
    SCRAPED_NAMES_FILE = os.path.join(NAMES_DIR, "scraped_names_to_process.txt")
    PROGRESS_FILE = os.path.join(NAMES_DIR, "ingestor_progress.txt")
    NAME_CHANGES_FILE = os.path.join(NAMES_DIR, "name_changes.json")
    MOJANG_UUID_URL = "https://api.mojang.com/users/profiles/minecraft/{player_name}"
    MOJANG_NAME_URL = "https://sessionserver.mojang.com/session/minecraft/profile/{uuid}"
    REQUEST_DELAY = 2.0 # Delay between Mojang API requests
    RATE_LIMIT_BACKOFF_SECONDS = 10 # Initial backoff for rate limits (seconds)
    MAX_RATE_LIMIT_BACKOFF_SECONDS = 600 # Max backoff for rate limits (seconds)

class UUIDIngestor:
    def __init__(self):
        self.conn = self._get_db_connection()
        self._setup_database()
        self.name_changes = self._load_json_file(Config.NAME_CHANGES_FILE) or {}
        self.uuid_cache = self._load_uuid_cache() # Load existing UUIDs from players table

    def _get_db_connection(self):
        conn = sqlite3.connect(Config.DATABASE_PATH)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        return conn

    def _setup_database(self):
        # Ensure player_discovery table exists (it should from leaderboard_tracker)
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS player_discovery (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discovered_uuid TEXT,
                source_uuid TEXT,
                discovery_method TEXT,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE
            )
        ''')
        self.conn.execute('CREATE INDEX IF NOT EXISTS idx_discovery_processed ON player_discovery(processed)')
        self.conn.commit()

    def _load_json_file(self, filepath: str) -> Optional[Dict[str, Any]]:
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    content = f.read()
                    if not content:
                        return None
                    return json.loads(content)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading {filepath}: {e}")
        return None

    def _save_json_file(self, filepath: str, data: Dict[str, Any]) -> None:
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except IOError as e:
            print(f"Error saving {filepath}: {e}")

    def _get_last_processed_line(self) -> int:
        if not os.path.exists(Config.PROGRESS_FILE):
            return 0
        try:
            with open(Config.PROGRESS_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                return int(content) if content else 0
        except (IOError, ValueError) as e:
            print(f"Error reading progress file, starting from beginning. Error: {e}")
            return 0

    def _update_last_processed_line(self, line_number: int) -> None:
        try:
            with open(Config.PROGRESS_FILE, 'w', encoding='utf-8') as f:
                f.write(str(line_number))
        except IOError as e:
            print(f"Error updating progress file: {e}")

    def _load_uuid_cache(self) -> Dict[str, str]:
        # Load existing UUIDs from the players table for quick lookup
        cursor = self.conn.execute("SELECT username, uuid FROM players")
        return {row[0]: row[1] for row in cursor.fetchall()}

    def _is_uuid_in_db(self, uuid: str) -> bool:
        cursor = self.conn.execute("SELECT 1 FROM players WHERE uuid = ?", (uuid,))
        return cursor.fetchone() is not None

    def _is_uuid_in_discovery_queue(self, uuid: str) -> bool:
        cursor = self.conn.execute("SELECT 1 FROM player_discovery WHERE discovered_uuid = ? AND processed = FALSE", (uuid,))
        return cursor.fetchone() is not None

    def get_player_uuid_and_name(self, player_name: str) -> Tuple[Optional[str], Optional[str], str]:
        # Check cache first
        if player_name in self.uuid_cache:
            uuid = self.uuid_cache[player_name]
            # Verify current name for potential changes
            current_name = self._get_username_from_uuid_mojang(uuid)
            if current_name and current_name.lower() != player_name.lower():
                print(f"Name change detected for {player_name}: new name is {current_name}")
                self._log_name_change(player_name, current_name)
                # Update cache with new name
                self.uuid_cache[current_name] = uuid
                del self.uuid_cache[player_name] # Remove old name from cache
                return uuid, current_name, "SUCCESS"
            return uuid, player_name, "SUCCESS"

        # Fetch from Mojang API
        current_backoff = Config.RATE_LIMIT_BACKOFF_SECONDS
        retries = 0
        while retries < 3: # Allow a few retries for transient network issues
            try:
                url = Config.MOJANG_UUID_URL.format(player_name=player_name)
                response = requests.get(url, timeout=10)
                time.sleep(Config.REQUEST_DELAY) # Respect rate limits

                if response.status_code == 200:
                    data = response.json()
                    uuid = data.get("id")
                    if uuid:
                        clean_uuid = uuid.replace('-', '')
                        formatted_uuid = f"{clean_uuid[:8]}-{clean_uuid[8:12]}-{clean_uuid[12:16]}-{clean_uuid[16:20]}-{clean_uuid[20:]}"
                        self.uuid_cache[player_name] = formatted_uuid # Add to cache
                        return formatted_uuid, player_name, "SUCCESS"
                elif response.status_code == 404:
                    print(f"Player '{player_name}' not found on Mojang API. Skipping permanently.")
                    return None, None, "PLAYER_NOT_FOUND"
                elif response.status_code == 429:
                    sleep_time = min(current_backoff, Config.MAX_RATE_LIMIT_BACKOFF_SECONDS) + random.uniform(0, 1) # Add jitter
                    print(f"Rate limited for {player_name}. Sleeping for {sleep_time:.2f} seconds before retrying.")
                    time.sleep(sleep_time)
                    current_backoff *= 2 # Exponential backoff
                    retries += 1
                    continue # Retry the same request
                else:
                    print(f"Error fetching UUID for {player_name}: {response.status_code}. Retrying later.")
                    retries += 1
                    continue # Retry the same request

            except requests.RequestException as e:
                print(f"Mojang API request for {player_name} failed: {e}. Retrying later.")
                retries += 1
                continue # Retry the same request
        
        # If all retries fail
        print(f"Failed to get UUID for {player_name} after multiple retries. Skipping for this cycle.")
        return None, None, "ERROR"

    def _get_username_from_uuid_mojang(self, uuid: str) -> Optional[str]:
        try:
            url = Config.MOJANG_NAME_URL.format(uuid=uuid.replace('-', ''))
            response = requests.get(url, timeout=10)
            time.sleep(Config.REQUEST_DELAY) # Respect rate limits
            if response.status_code == 200:
                data = response.json()
                return data.get("name")
        except requests.RequestException as e:
            print(f"Error fetching username for {uuid}: {e}")
        return None

    def _log_name_change(self, old_name: str, new_name: str) -> None:
        change_record = {"old_name": old_name, "new_name": new_name, "date": datetime.now().isoformat()}
        if old_name not in self.name_changes:
            self.name_changes[old_name] = []
        self.name_changes[old_name].append(change_record)
        self._save_json_file(Config.NAME_CHANGES_FILE, self.name_changes)

    def add_uuid_to_discovery_queue(self, uuid: str, player_ign: str, source_name: str, discovery_method: str) -> bool:
        if self._is_uuid_in_db(uuid) or self._is_uuid_in_discovery_queue(uuid):
            print(f"IGN {player_ign} (UUID {uuid[:8]}...) already in DB or queue. Skipping.")
            return False

        max_retries = 5
        retry_delay = 0.1  # seconds

        for attempt in range(max_retries):
            try:
                self.conn.execute(
                    "INSERT INTO player_discovery (discovered_uuid, source_uuid, discovery_method) VALUES (?, ?, ?)",
                    (uuid, None, discovery_method) # source_uuid is None for scraped names
                )
                self.conn.commit()
                print(f"Added IGN {player_ign} (UUID {uuid[:8]}...) to discovery queue.")
                return True
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    print(f"Attempt {attempt + 1}/{max_retries}: Database is locked. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2 # Exponential backoff for retry delay
                else:
                    print(f"Error adding IGN {player_ign} (UUID {uuid[:8]}...) to discovery queue: {e}")
                    return False
            except sqlite3.Error as e:
                print(f"Error adding IGN {player_ign} (UUID {uuid[:8]}...) to discovery queue: {e}")
                return False
        
        print(f"Failed to add IGN {player_ign} (UUID {uuid[:8]}...) to discovery queue after {max_retries} attempts due to database lock.")
        return False

    def process_scraped_names(self):
        if not os.path.exists(Config.SCRAPED_NAMES_FILE):
            print("No scraped names file found. Skipping.")
            return

        last_processed_line = self._get_last_processed_line()
        print(f"Starting from line {last_processed_line + 1} in scraped names file.")

        processed_count = 0
        current_line_number = 0
        try:
            with open(Config.SCRAPED_NAMES_FILE, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, 1):
                    current_line_number = i
                    if current_line_number <= last_processed_line:
                        continue

                    name = line.strip()
                    if not name:
                        # If it's an empty line, just update progress and continue
                        self._update_last_processed_line(current_line_number)
                        continue

                    uuid, current_name, status = self.get_player_uuid_and_name(name)
                    
                    if status == "SUCCESS":
                        if self.add_uuid_to_discovery_queue(uuid, name, current_name or name, "mineflayer_scrape"):
                            processed_count += 1
                        # Always update progress for successful additions
                        self._update_last_processed_line(current_line_number)
                    elif status == "PLAYER_NOT_FOUND":
                        # Permanently skip names that are not found
                        self._update_last_processed_line(current_line_number)
                    elif status == "RATE_LIMITED" or status == "ERROR":
                        # Do NOT update progress for transient errors, so they are retried
                        print(f"Skipping {name} for now due to API error. Will retry later.")
                    
                    time.sleep(Config.REQUEST_DELAY) # Delay between Mojang API calls
        except Exception as e:
            print(f"An error occurred during name processing: {e}")
            print(f"Stopping at line {current_line_number}. Progress has been saved for processed lines.")
            return


        print(f"Finished processing scraped names. Added {processed_count} new UUIDs to discovery queue.")

    def run(self):
        print("Starting UUID Ingestor...")
        self.process_scraped_names()
        print("UUID Ingestor finished one cycle.")

if __name__ == "__main__":
    # Ensure player_names directory exists
    os.makedirs(Config.NAMES_DIR, exist_ok=True)
    ingestor = UUIDIngestor()
    ingestor.run()

    # Optional: Run in a loop
    # while True:
    #     ingestor.run()
    #     print(f"Sleeping for 5 minutes...")
    #     time.sleep(300)