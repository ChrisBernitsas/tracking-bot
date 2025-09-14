#!/usr/bin/env python3
"""
Minecraft Bedwars Stats Tracker

Tracks player statistics from Hypixel API and logs session data.
Now includes recent games tracking, name change detection, and smart cooldowns.

This version reads player UUIDs directly from the central database.
"""

import json
import os
import sqlite3
import time
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import requests
import urllib3

# Suppress the NotOpenSSLWarning, which can cause issues on some systems
warnings.filterwarnings('ignore', category=urllib3.exceptions.NotOpenSSLWarning)


class Config:
    """Configuration constants."""
    API_KEY = "e90f0bca-1575-4639-a684-1089240e1bfb"
    # PLAYERS list is no longer used, players are fetched from DB
    
    BASELINE_DIR = "bedwars_baseline"
    SESSION_DIR = "bedwars_sessions"
    RECENT_GAMES_DIR = "recent_games"
    PLAYER_NAMES_DIR = "player_names"
    SLEEP_TIME_SECONDS = 60
    PLAYER_COOLDOWN_MINUTES = 30

    # File paths
    NAME_CHANGES_FILE = os.path.join(PLAYER_NAMES_DIR, "name_changes.json")
    PLAYER_COOLDOWN_FILE = "player_cooldowns.json"
    DATABASE_PATH = "bedwars_database.db"

    # API endpoints
    HYPIXEL_PLAYER_URL = "https://api.hypixel.net/player?uuid={uuid}"
    HYPIXEL_RECENT_GAMES_URL = "https://api.hypixel.net/recentgames?uuid={uuid}"


class BedwarsStatsTracker:
    """Main class for tracking Bedwars statistics."""

    def __init__(self):
        self.conn = sqlite3.connect(Config.DATABASE_PATH, check_same_thread=False)
        self.player_cooldowns: Dict[str, Any] = {}
        self.name_changes: Dict[str, List[Dict[str, str]]] = {}
        self.rate_limit_remaining: Optional[int] = None
        self.rate_limit_reset: Optional[int] = None
        self._setup_directories()
        self._load_persistent_data()

    def _setup_directories(self) -> None:
        """Create necessary directories."""
        for dirname in [Config.BASELINE_DIR, Config.SESSION_DIR, Config.RECENT_GAMES_DIR, Config.PLAYER_NAMES_DIR]:
            os.makedirs(dirname, exist_ok=True)

    def _load_persistent_data(self) -> None:
        """Load persistent data from JSON files."""
        # uuid_mapping is no longer loaded/managed by this script
        self.player_cooldowns = self._load_json_file(Config.PLAYER_COOLDOWN_FILE) or {}
        self.name_changes = self._load_json_file(Config.NAME_CHANGES_FILE) or {}

    def _save_persistent_data(self) -> None:
        """Save persistent data to JSON files."""
        # uuid_mapping is no longer saved/managed by this script
        self._save_json_file(Config.PLAYER_COOLDOWN_FILE, self.player_cooldowns)
        self._save_json_file(Config.NAME_CHANGES_FILE, self.name_changes)

    @staticmethod
    def _load_json_file(filepath: str) -> Optional[Dict[str, Any]]:
        """Load JSON data from file."""
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    # Handle empty file
                    content = f.read()
                    if not content:
                        return None
                    return json.loads(content)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading {filepath}: {e}")
        return None

    @staticmethod
    def _save_json_file(filepath: str, data: Dict[str, Any]) -> None:
        """Save JSON data to file."""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except IOError as e:
            print(f"Error saving {filepath}: {e}")

    @staticmethod
    def calculate_wlr(wins: int, losses: int) -> float:
        """Calculate win/loss ratio."""
        return round(wins / losses, 3) if losses > 0 else float(wins)

    def get_player_uuid(self, player_name: str) -> Optional[str]:
        """Get UUID for a player from the 'players' table in the database."""
        cursor = self.conn.execute("SELECT uuid FROM players WHERE username = ? COLLATE NOCASE", (player_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        print(f"Could not find UUID for '{player_name}' in the database. Player may not be ingested yet.")
        return None

    def get_uuid_from_mojang(self, player_name: str) -> Optional[str]:
        """Get UUID from Mojang API."""
        url = f"https://api.mojang.com/users/profiles/minecraft/{player_name}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()["id"]
            elif response.status_code == 404:
                return None
            else:
                print(f"Mojang API error for {player_name}: {response.status_code}")
                return None
        except requests.RequestException as e:
            print(f"Error fetching UUID from Mojang for {player_name}: {e}")
            return None

    def make_api_request(self, url: str, description: str) -> Optional[Dict[str, Any]]:
        """Make a request to Hypixel API with rate limit handling."""
        headers = {"API-Key": Config.API_KEY}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if "RateLimit-Remaining" in response.headers:
                self.rate_limit_remaining = int(response.headers["RateLimit-Remaining"])
            if "RateLimit-Reset" in response.headers:
                self.rate_limit_reset = int(response.headers["RateLimit-Reset"])
            if response.status_code == 429:
                reset_time = self.rate_limit_reset or 60
                print(f"Rate limited! Waiting {reset_time} seconds...")
                time.sleep(reset_time + 5)
                return self.make_api_request(url, description)
            if response.status_code != 200:
                print(f"{description} request failed with status {response.status_code}")
                return None
            return response.json()
        except requests.RequestException as e:
            print(f"Error with {description} for URL {url}: {e}")
            return None

    def fetch_player_stats(self, original_name: str, uuid: str) -> Optional[Dict[str, Any]]:
        """Fetch player statistics, handling potential name changes case-insensitively."""
        url = Config.HYPIXEL_PLAYER_URL.format(uuid=uuid)
        data = self.make_api_request(url, "Player stats")
        if not data or not data.get("player"):
            return None
        player_data = data["player"]
        current_name = player_data.get("displayname")
        if current_name and current_name.lower() != original_name.lower():
            print(f"Name change detected for {original_name}: new name is {current_name}")
            change_record = {"new_name": current_name, "date": datetime.now().isoformat()}
            if original_name not in self.name_changes:
                self.name_changes[original_name] = []
            
            # Check if this specific name change (new_name) is already recorded
            already_recorded = False
            for record in self.name_changes[original_name]:
                if record.get("new_name") == current_name:
                    already_recorded = True
                    break
            
            if not already_recorded:
                self.name_changes[original_name].append(change_record)
                self._save_json_file(Config.NAME_CHANGES_FILE, self.name_changes)
                print(f"Recorded new name change for {original_name} to {current_name}.")
            else:
                print(f"Name change {original_name} to {current_name} already recorded. Skipping.")

            # UUID mapping is now handled by uuid_ingestor.py and leaderboard_tracker.py
            # This script no longer updates the name_uuid_mapping.json file

        bedwars_stats = player_data.get("stats", {}).get("Bedwars", {})
        return self._parse_bedwars_stats(bedwars_stats)

    def fetch_recent_games(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Fetch recent games data from Hypixel API."""
        url = Config.HYPIXEL_RECENT_GAMES_URL.format(uuid=uuid)
        data = self.make_api_request(url, "Recent games")
        if not data:
            return None
        games = data.get("games", [])
        bedwars_games = [self._parse_bedwars_game(game) for game in games if game.get("gameType") == "BEDWARS"]
        return {"api_enabled": len(games) > 0, "games": [g for g in bedwars_games if g]}

    def _parse_bedwars_game(self, game: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single Bedwars game from recent games data."""
        try:
            date_timestamp = game.get("date")
            mode = game.get("mode", "UNKNOWN")
            mode_mapping = {
                "BEDWARS_EIGHT_ONE": "solos", "BEDWARS_EIGHT_TWO": "doubles",
                "BEDWARS_FOUR_THREE": "threes", "BEDWARS_FOUR_FOUR": "fours"
            }
            readable_mode = mode_mapping.get(mode, mode)
            game_id = f"{date_timestamp}_{mode}_{game.get('map', 'UNKNOWN')}"
            return {"game_id": game_id, "timestamp": date_timestamp, "mode": readable_mode, "map": game.get("map", "Unknown Map")}
        except Exception as e:
            print(f"Error parsing game data: {e}")
            return None

    def _parse_bedwars_stats(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Bedwars statistics from API response."""
        overall_wins = stats.get("wins_bedwars", 0)
        overall_losses = stats.get("losses_bedwars", 0)
        return {
            "wins": overall_wins, "losses": overall_losses,
            "WLR": self.calculate_wlr(overall_wins, overall_losses),
            "winstreak": stats.get("winstreak"),
            "modes": {
                "solos": self._parse_mode_stats(stats, "eight_one"),
                "doubles": self._parse_mode_stats(stats, "eight_two"),
                "threes": self._parse_mode_stats(stats, "four_three"),
                "fours": self._parse_mode_stats(stats, "four_four")
            }
        }

    def _parse_mode_stats(self, stats: Dict[str, Any], mode_prefix: str) -> Dict[str, Any]:
        """Parse statistics for a specific game mode."""
        wins = stats.get(f"{mode_prefix}_wins_bedwars", 0)
        losses = stats.get(f"{mode_prefix}_losses_bedwars", 0)
        return {
            "wins": wins, "losses": losses,
            "WLR": self.calculate_wlr(wins, losses),
            "winstreak": stats.get(f"{mode_prefix}_winstreak")
        }

    def calculate_session_diff(self, current_stats: Dict[str, Any], baseline_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate the difference between current and baseline stats."""
        overall_diff = {"wins": current_stats["wins"] - baseline_stats["wins"], "losses": current_stats["losses"] - baseline_stats["losses"]}
        overall_diff["WLR"] = self.calculate_wlr(overall_diff["wins"], overall_diff["losses"])
        modes_diff = {}
        for mode, current_mode_stats in current_stats["modes"].items():
            baseline_mode_stats = baseline_stats["modes"].get(mode, {"wins": 0, "losses": 0})
            wins_diff = current_mode_stats["wins"] - baseline_mode_stats["wins"]
            losses_diff = current_mode_stats["losses"] - baseline_mode_stats["losses"]
            if wins_diff != 0 or losses_diff != 0:
                modes_diff[mode] = {"wins": wins_diff, "losses": losses_diff, "WLR": self.calculate_wlr(wins_diff, losses_diff)}
        return {"overall": overall_diff, "modes": modes_diff}

    def update_winstreak_estimates(self, winstreak_data: Dict[str, Any], diff: Dict[str, Any], current_stats: Dict[str, Any]) -> None:
        """Update winstreak estimates based on session differences."""
        scopes = ["overall"] + list(diff.get("modes", {}).keys())
        for scope in scopes:
            wins, losses, api_winstreak = 0, 0, None
            if scope == "overall":
                wins, losses = diff["overall"]["wins"], diff["overall"]["losses"]
                api_winstreak = current_stats.get("winstreak")
            elif scope in diff["modes"]:
                wins, losses = diff["overall"]["wins"], diff["overall"]["losses"]
                api_winstreak = current_stats["modes"].get(scope, {}).get("winstreak")
            if wins == 0 and losses == 0: continue
            if scope not in winstreak_data: winstreak_data[scope] = {"min_possible": 0, "max_possible": 0, "likely": 0.0}
            if isinstance(api_winstreak, int):
                winstreak_data[scope] = {"api_value": api_winstreak, "min_possible": api_winstreak, "max_possible": api_winstreak, "likely": float(api_winstreak)}
            else:
                if losses > 0:
                    winstreak_data[scope]["min_possible"] = 0
                    winstreak_data[scope]["max_possible"] = wins
                    winstreak_data[scope]["likely"] = wins / (losses + 1)
                else:
                    winstreak_data[scope]["min_possible"] += wins
                    winstreak_data[scope]["max_possible"] += wins
                    winstreak_data[scope]["likely"] += wins

    def build_session_summary(self, session_data: Dict[str, Any]) -> List[str]:
        """Build a summary of all sessions."""
        summary = []
        session_keys = sorted([k for k in session_data.keys() if k.startswith("session_")], key=lambda k: int(k.split("_")[1]))
        for key in session_keys: # Iterate directly over sorted keys
            session = session_data[key]
            session_num = key.split("_")[1] # Get the actual session number from the key
            parts = [f"Session {session_num}:"] # Use the actual session number
            for mode, data in session.get("modes", {}).items():
                parts.append(f"{mode.capitalize()} W/L: {data['wins']}/{data['losses']}")
            summary.append(" ".join(parts))
        return summary

    def process_recent_games(self, player_name: str, uuid: str) -> None:
        """Process and save recent games data for a player."""
        recent_games_response = self.fetch_recent_games(uuid)
        if not recent_games_response:
            print(f"Could not fetch recent games for {player_name}")
            return
        recent_games_path = os.path.join(Config.RECENT_GAMES_DIR, f"{player_name}.json")
        existing_data = self._load_json_file(recent_games_path) or {"recent_games": []}
        existing_game_ids = {game["game_id"] for game in existing_data["recent_games"]}
        new_games = [game for game in recent_games_response["games"] if game["game_id"] not in existing_game_ids]
        if new_games:
            all_games = new_games + existing_data["recent_games"]
            all_games.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            existing_data["recent_games"] = all_games[:50]
        existing_data["api_enabled"] = recent_games_response["api_enabled"]
        self._save_json_file(recent_games_path, existing_data)
        status_icon = "✓" if recent_games_response["api_enabled"] else "✗"
        print(f"{status_icon} {player_name}: {len(new_games)} new games found.")

    def process_player_session(self, player_name: str, uuid: str) -> None:
        """Process a single player's session data."""
        current_stats = self.fetch_player_stats(player_name, uuid)
        if not current_stats:
            print(f"Could not fetch stats for {player_name}")
            return
        baseline_path = os.path.join(Config.BASELINE_DIR, f"{player_name}.json")
        baseline_stats = self._load_json_file(baseline_path)
        if not baseline_stats:
            self._save_json_file(baseline_path, current_stats)
            print(f"Created baseline for {player_name}")
            return
        if current_stats["wins"] == baseline_stats["wins"] and current_stats["losses"] == baseline_stats["losses"]:
            return
        session_path = os.path.join(Config.SESSION_DIR, f"{player_name}.json")
        session_data = self._load_json_file(session_path) or {}
        diff = self.calculate_session_diff(current_stats, baseline_stats)
        games_played = diff["overall"]["wins"] + diff["overall"]["losses"]
        if 1 <= games_played <= 2:
            self.player_cooldowns[player_name] = {"last_check": datetime.now().isoformat(), "api_on": True}
            self._save_json_file(Config.PLAYER_COOLDOWN_FILE, self.player_cooldowns)
            print(f"Player {player_name} has API on, adding to cooldown list.")
        session_num = sum(1 for k in session_data if k.startswith("session_")) + 1
        session_key = f"session_{session_num}"
        session_data.setdefault("winstreak", {})
        self.update_winstreak_estimates(session_data["winstreak"], diff, current_stats)
        session_data[session_key] = diff
        new_session_data = {"winstreak": session_data.get("winstreak", {})}
        for k, v in sorted([item for item in session_data.items() if item[0].startswith("session_")], key=lambda item: int(item[0].split('_')[1])):
            new_session_data[k] = v
        new_session_data["summary"] = self.build_session_summary(new_session_data)
        self._save_json_file(session_path, new_session_data)
        self._save_json_file(baseline_path, current_stats)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] Session {session_num} logged for {player_name}")

    def run(self, player_names_manual_list: Optional[List[str]] = None) -> None:
        """Main tracking loop."""
        print("Tracking Bedwars sessions and recent games...")
        print("Loading player UUIDs...")

        players_to_process: List[str] = []
        if player_names_manual_list:
            print(f"Using manual list of {len(player_names_manual_list)} players.")
            players_to_process = player_names_manual_list
        else:
            print("Fetching players from database...")
            cursor = self.conn.execute("SELECT username FROM players")
            db_players = [row[0] for row in cursor.fetchall()]
            print(f"Found {len(db_players)} players in the database.")
            players_to_process = db_players

        valid_players: Dict[str, str] = {}
        for player_name in players_to_process:
            uuid = self.get_player_uuid(player_name)
            if not uuid and player_names_manual_list:
                print(f"Could not find UUID for '{player_name}' in the database. Trying Mojang API.")
                uuid = self.get_uuid_from_mojang(player_name)
                if uuid:
                    print(f"Found UUID for '{player_name}' from Mojang API: {uuid}")
                else:
                    print(f"Could not find UUID for '{player_name}' from Mojang API.")

            if uuid:
                valid_players[player_name] = uuid
            else:
                print(f"✗ Could not find UUID for {player_name}. Skipping.")

        if not valid_players:
            print("No valid players found to process. Exiting.")
            return
        print(f"Starting tracking for {len(valid_players)} players...\n")
        try:
            while True:
                print(f"--- Starting check cycle for {len(valid_players)} players... ---")
                if self.rate_limit_remaining is not None:
                    print(f"API requests remaining: {self.rate_limit_remaining}, resets in: {self.rate_limit_reset} seconds")
                
                for i, (player_name, uuid) in enumerate(valid_players.items(), 1):
                    if player_name in self.player_cooldowns:
                        cooldown_info = self.player_cooldowns[player_name]
                        last_check = datetime.fromisoformat(cooldown_info["last_check"])
                        if datetime.now() < last_check + timedelta(minutes=Config.PLAYER_COOLDOWN_MINUTES):
                            print(f"Skipping {player_name} due to cooldown (API on).")
                            continue
                    
                    print(f"Checking {player_name} ({i}/{len(valid_players)})...")
                    self.process_player_session(player_name, uuid)
                    time.sleep(1)
                    self.process_recent_games(player_name, uuid)
                    if i < len(valid_players): time.sleep(2)
                
                print(f"--- Check cycle complete. Waiting {Config.SLEEP_TIME_SECONDS} seconds ---")
                if self.rate_limit_remaining is not None:
                    print(f"API requests remaining: {self.rate_limit_remaining}, resets in: {self.rate_limit_reset} seconds")
                time.sleep(Config.SLEEP_TIME_SECONDS)

        except KeyboardInterrupt:
            print("\nTracking stopped by user.")
        finally:
            self._save_persistent_data()
            print("Persistent data saved.")

def main():
    """Entry point."""
    tracker = BedwarsStatsTracker()
    # Example of how to use a manual list:
    PLAYERS = ["TheNaturalOrder"]
    # PLAYERS = [
    #     "NosDaemon", "iB4NANA_", "Wlnks", "evelwn", "KDK0",
    #     "TheNaturalOrder", "Hashito", "_JBC_", "goodone_", "gublerbae",
    #     "lizerr", "thighteen", "vLonelyy", "Kayes", "Cozer", "PRAXZZ", 
    #     "Azik", "withoutu", "PatExE", "deskwondent", "Crawdead", "Litthowius", 
    #     "tiltings", "tiltingsson", "game_game_game31", "WarOG", "TURUHASHI", 
    #     "lelitzpanda", "Timness"
    # ]
    tracker.run(player_names_manual_list=PLAYERS)
    tracker.run() # Run with players from database by default

if __name__ == "__main__":
    main()
