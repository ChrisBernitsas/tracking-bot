#!/usr/bin/env python3
"""
Massive Minecraft Bedwars Database Tracker

Builds a comprehensive database of thousands of players to create leaderboards.
Uses Hypixel's leaderboard API to seed with top players, then discovers more through guilds and recent games.

This version focuses on processing UUIDs from the central database queue.
"""

import json
import os
import time
import sqlite3
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict

import requests


class Config:
    """Configuration constants."""
    API_KEY = "e90f0bca-1575-4639-a684-1089240e1bfb"
    
    # Database settings
    DATABASE_PATH = "bedwars_database.db"
    LEADERBOARDS_DIR = "leaderboards"
    NAMES_DIR = "player_names" # Used for scraped_names_to_process.txt
    
    # Tracking settings
    TARGET_PLAYER_COUNT = 1000000  # Aim for 1M players in database
    PLAYERS_PER_CYCLE = 200        # Reduced for better rate limit handling
    CYCLE_SLEEP_SECONDS = 300     # 5 minutes between cycles
    REQUEST_DELAY = 1.5          # 1.5 seconds between API requests (conservative)
    
    # Player discovery settings
    MIN_BEDWARS_WINS = 0       # Only track players with at least 0 wins
    DISCOVERY_BATCH_SIZE = 100    # How many new players to discover per cycle
    
    # API endpoints
    HYPIXEL_PLAYER_URL = "https://api.hypixel.net/player?uuid={uuid}"
    HYPIXEL_GUILD_URL = "https://api.hypixel.net/guild?player={uuid}"
    HYPIXEL_LEADERBOARDS_URL = "https://api.hypixel.net/leaderboards"


class MassiveBedwarsTracker:
    """Main class for building a massive Bedwars player database."""
    
    def __init__(self):
        self.conn = self._get_db_connection()
        self._setup_directories()
        self.rate_limit_remaining = 120
        self.last_leaderboard_fetch = 0

    def _get_db_connection(self):
        conn = sqlite3.connect(Config.DATABASE_PATH, check_same_thread=False)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        return conn

    def _setup_directories(self) -> None:
        """Create necessary directories."""
        os.makedirs(Config.LEADERBOARDS_DIR, exist_ok=True)
        os.makedirs(Config.NAMES_DIR, exist_ok=True)
    
    def _setup_database(self) -> None:
        """Initialize SQLite database with proper schema."""
        # This method is called by _get_db_connection in the new setup
        # Ensure all tables are created if they don't exist
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS players (
                uuid TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                discovery_method TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                bedwars_level INTEGER DEFAULT 0,
                last_login TIMESTAMP
            )
        ''')
        
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS bedwars_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                kills INTEGER DEFAULT 0,
                deaths INTEGER DEFAULT 0,
                final_kills INTEGER DEFAULT 0,
                final_deaths INTEGER DEFAULT 0,
                beds_broken INTEGER DEFAULT 0,
                beds_lost INTEGER DEFAULT 0,
                winstreak INTEGER,
                coins INTEGER DEFAULT 0,
                experience INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                -- Mode-specific stats
                solos_wins INTEGER DEFAULT 0,
                solos_losses INTEGER DEFAULT 0,
                solos_winstreak INTEGER,
                doubles_wins INTEGER DEFAULT 0,
                doubles_losses INTEGER DEFAULT 0,
                doubles_winstreak INTEGER,
                threes_wins INTEGER DEFAULT 0,
                threes_losses INTEGER DEFAULT 0,
                threes_winstreak INTEGER,
                fours_wins INTEGER DEFAULT 0,
                fours_losses INTEGER DEFAULT 0,
                fours_winstreak INTEGER,
                -- Derived stats
                wlr REAL GENERATED ALWAYS AS (CASE WHEN losses > 0 THEN CAST(wins AS REAL) / losses ELSE wins END) STORED,
                kdr REAL GENERATED ALWAYS AS (CASE WHEN deaths > 0 THEN CAST(kills AS REAL) / deaths ELSE kills END) STORED,
                fkdr REAL GENERATED ALWAYS AS (CASE WHEN final_deaths > 0 THEN CAST(final_kills AS REAL) / final_deaths ELSE final_kills END) STORED,
                bblr REAL GENERATED ALWAYS AS (CASE WHEN beds_lost > 0 THEN CAST(beds_broken AS REAL) / beds_lost ELSE beds_broken END) STORED,
                FOREIGN KEY (uuid) REFERENCES players (uuid)
            )
        ''')
        
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
        
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS leaderboard_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                leaderboard_type TEXT,
                game_type TEXT,
                period TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data TEXT
            )
        ''')
        
        # Create indexes for performance
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_stats_uuid ON bedwars_stats(uuid)',
            'CREATE INDEX IF NOT EXISTS idx_stats_timestamp ON bedwars_stats(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_stats_wins ON bedwars_stats(wins DESC)',
            'CREATE INDEX IF NOT EXISTS idx_stats_wlr ON bedwars_stats(wlr DESC)',
            'CREATE INDEX IF NOT EXISTS idx_stats_fkdr ON bedwars_stats(fkdr DESC)',
            'CREATE INDEX IF NOT EXISTS idx_discovery_processed ON player_discovery(processed)',
            'CREATE INDEX IF NOT EXISTS idx_players_active ON players(is_active, last_updated)',
        ]
        
        for idx in indexes:
            self.conn.execute(idx)
        
        self.conn.commit()

    def get_player_uuid_from_db(self, player_name: str) -> Optional[str]:
        """Get UUID for a player from the 'players' table in the database."""
        cursor = self.conn.execute("SELECT uuid FROM players WHERE username = ? COLLATE NOCASE", (player_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        return None
    
    def get_username_from_uuid_from_db(self, uuid: str) -> Optional[str]:
        """Get current username for a UUID from the 'players' table in the database."""
        cursor = self.conn.execute("SELECT username FROM players WHERE uuid = ?", (uuid,))
        result = cursor.fetchone()
        if result:
            return result[0]
        return None
    
    def make_api_request(self, url: str, description: str) -> Optional[Dict[str, Any]]:
        """Make a request to Hypixel API with improved rate limit handling."""
        headers = {"API-Key": Config.API_KEY}
        
        try:
            response = requests.get(url, headers=headers, timeout=15)
            
            # Update rate limit info from headers
            if "RateLimit-Remaining" in response.headers:
                self.rate_limit_remaining = int(response.headers["RateLimit-Remaining"])
                reset_time = response.headers.get("RateLimit-Reset", "0")
                print(f"API requests remaining: {self.rate_limit_remaining}, resets in: {reset_time} seconds")

                if self.rate_limit_remaining < 10:
                    print(f"🔴 CRITICAL: Only {self.rate_limit_remaining} requests remaining, resets in {reset_time}s")
                elif self.rate_limit_remaining < 30:
                    print(f"🟡 Warning: {self.rate_limit_remaining} requests remaining, resets in {reset_time}s")
            
            if response.status_code == 429:
                reset_time = int(response.headers.get("RateLimit-Reset", 60))
                print(f"🚫 Rate limited! Waiting {reset_time + 10} seconds...")
                time.sleep(reset_time + 10)
                return self.make_api_request(url, description)
            
            if response.status_code != 200:
                print(f"❌ {description} failed with status {response.status_code}")
                return None
            
            # Dynamic delay based on remaining rate limit
            if self.rate_limit_remaining <= 10:
                time.sleep(Config.REQUEST_DELAY * 4)  # Very conservative
            elif self.rate_limit_remaining <= 30:
                time.sleep(Config.REQUEST_DELAY * 2)  # Conservative
            elif self.rate_limit_remaining <= 60:
                time.sleep(Config.REQUEST_DELAY * 1.5)  # Moderate
            else:
                time.sleep(Config.REQUEST_DELAY)  # Normal
            
            return response.json()
            
        except requests.RequestException as e:
            print(f"❌ Error with {description}: {e}")
            return None

    def add_manual_players(self) -> int:
        """Allow manual input of player names and add them to scraped_names_to_process.txt."""
        print("\n🎯 Manual Player Input")
        print("Enter player names one by one (press Enter with empty name to finish):")
        
        added_count = 0
        scraped_names_file = os.path.join(Config.NAMES_DIR, "scraped_names_to_process.txt")

        while True:
            player_name = input("Player name: ").strip()
            if not player_name:
                break
            
            # Basic validation
            if not (3 <= len(player_name) <= 16 and player_name.replace('_', '').isalnum()):
                print(f"❌ Invalid player name format: {player_name}")
                continue

            try:
                with open(scraped_names_file, 'a', encoding='utf-8') as f:
                    f.write(f"{player_name}\n")
                print(f"📝 Added {player_name} to {scraped_names_file}")
                added_count += 1
            except IOError as e:
                print(f"⚠️  Error saving player name to {scraped_names_file}: {e}")

        print(f"✅ Manually added {added_count} new players to {scraped_names_file}")
        return added_count
    
    def seed_from_leaderboards(self) -> int:
        """Seed the database with top players from Hypixel leaderboards."""
        print("🌱 Seeding database from Hypixel leaderboards...")
        
        # Don't fetch too frequently
        if time.time() - self.last_leaderboard_fetch < 3600:  # 1 hour
            print("⏰ Leaderboards fetched recently, skipping...")
            return 0
        
        data = self.make_api_request(Config.HYPIXEL_LEADERBOARDS_URL, "Leaderboards")
        if not data or not data.get("success"):
            print("❌ Failed to fetch leaderboards")
            return 0
        
        self.last_leaderboard_fetch = time.time()
        added_count = 0
        
        # Process all available leaderboards
        for game_type, leaderboards in data.get("leaderboards", {}).items():
            if game_type != "BEDWARS":  # Focus on Bedwars for now
                continue
                
            for lb in leaderboards:
                lb_title = lb.get("title", "Unknown")
                lb_period = lb.get("prefix", "Unknown")
                leaders = lb.get("leaders", [])
                
                print(f"📊 Processing {lb_period} {lb_title} leaderboard ({len(leaders)} players)")
                
                for uuid in leaders:
                    if self.add_player_to_discovery(uuid, None, f"leaderboard_{game_type}_{lb_title}"):
                        added_count += 1
        
        print(f"✅ Added {added_count} new players from leaderboards")
        return added_count
    
    def add_player_to_discovery(self, uuid: str, source_uuid: Optional[str], method: str) -> bool:
        """Add a player to the discovery queue if not already processed."""
        # Clean UUID format
        clean_uuid = uuid.replace('-', '')
        formatted_uuid = f"{clean_uuid[:8]}-{clean_uuid[8:12]}-{clean_uuid[12:16]}-{clean_uuid[16:20]}-{clean_uuid[20:]}"
        
        # Check if already exists in players table
        cursor = self.conn.execute(
            "SELECT 1 FROM players WHERE uuid = ? OR uuid = ?", 
            (uuid, formatted_uuid)
        )
        if cursor.fetchone():
            return False
        
        # Check if already in discovery queue
        cursor = self.conn.execute(
            "SELECT 1 FROM player_discovery WHERE discovered_uuid = ? OR discovered_uuid = ? AND processed = FALSE", 
            (uuid, formatted_uuid)
        )
        if cursor.fetchone():
            return False
        
        # Add to discovery queue
        self.conn.execute(
            "INSERT INTO player_discovery (discovered_uuid, source_uuid, discovery_method) VALUES (?, ?, ?)",
            (formatted_uuid, source_uuid, method)
        )
        self.conn.commit()
        return True
    
    def fetch_player_stats(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Fetch comprehensive player statistics."""
        url = Config.HYPIXEL_PLAYER_URL.format(uuid=uuid)
        data = self.make_api_request(url, f"Player stats for {uuid[:8]}")
        
        if not data or not data.get("player"):
            return None
        
        player_data = data["player"]
        bedwars_stats = player_data.get("stats", {}).get("Bedwars", {})
        
        # Skip players with too few wins
        total_wins = bedwars_stats.get("wins_bedwars", 0)
        if total_wins < Config.MIN_BEDWARS_WINS:
            return None
        
        # Get username
        username = player_data.get("displayname", "Unknown")
        # No longer fetching username from Mojang API here, rely on displayname or DB
        if username == "Unknown":
            username = self.get_username_from_uuid_from_db(uuid) or "Unknown"
        
        # Parse last login
        last_login = player_data.get("lastLogin")
        last_login_dt = None
        if last_login:
            last_login_dt = datetime.fromtimestamp(last_login / 1000).isoformat()
        
        # Calculate bedwars level from experience
        experience = bedwars_stats.get("Experience", 0)
        level = self.calculate_bedwars_level(experience)
        
        # Extract comprehensive stats
        stats = {
            "uuid": uuid,
            "username": username,
            "bedwars_level": level,
            "last_login": last_login_dt,
            "wins": bedwars_stats.get("wins_bedwars", 0),
            "losses": bedwars_stats.get("losses_bedwars", 0),
            "kills": bedwars_stats.get("kills_bedwars", 0),
            "deaths": bedwars_stats.get("deaths_bedwars", 0),
            "final_kills": bedwars_stats.get("final_kills_bedwars", 0),
            "final_deaths": bedwars_stats.get("final_deaths_bedwars", 0),
            "beds_broken": bedwars_stats.get("beds_broken_bedwars", 0),
            "beds_lost": bedwars_stats.get("beds_lost_bedwars", 0),
            "coins": bedwars_stats.get("coins", 0),
            "experience": experience,
            "games_played": bedwars_stats.get("games_played_bedwars", 0),
            "winstreak": bedwars_stats.get("winstreak") if isinstance(bedwars_stats.get("winstreak"), int) else None,
            
            # Mode-specific stats
            "solos_wins": bedwars_stats.get("eight_one_wins_bedwars", 0),
            "solos_losses": bedwars_stats.get("eight_one_losses_bedwars", 0),
            "solos_winstreak": bedwars_stats.get("eight_one_winstreak") if isinstance(bedwars_stats.get("eight_one_winstreak"), int) else None,
            
            "doubles_wins": bedwars_stats.get("eight_two_wins_bedwars", 0),
            "doubles_losses": bedwars_stats.get("eight_two_losses_bedwars", 0),
            "doubles_winstreak": bedwars_stats.get("eight_two_winstreak") if isinstance(bedwars_stats.get("eight_two_winstreak"), int) else None,
            
            "threes_wins": bedwars_stats.get("four_three_wins_bedwars", 0),
            "threes_losses": bedwars_stats.get("four_three_losses_bedwars", 0),
            "threes_winstreak": bedwars_stats.get("four_three_winstreak") if isinstance(bedwars_stats.get("four_three_winstreak"), int) else None,
            
            "fours_wins": bedwars_stats.get("four_four_wins_bedwars", 0),
            "fours_losses": bedwars_stats.get("four_four_losses_bedwars", 0),
            "fours_winstreak": bedwars_stats.get("four_four_winstreak") if isinstance(bedwars_stats.get("four_four_winstreak"), int) else None,
        }
        
        return stats
    
    def calculate_bedwars_level(self, experience: int) -> int:
        """Calculates Bedwars level based on the official Hypixel formula.
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
    
    def discover_players_from_guild(self, uuid: str) -> int:
        """Discover new players from a player's guild."""
        url = Config.HYPIXEL_GUILD_URL.format(uuid=uuid)
        data = self.make_api_request(url, f"Guild for {uuid[:8]}")
        
        if not data or not data.get("guild"):
            return 0
        
        guild = data["guild"]
        members = guild.get("members", [])
        discovered = 0
        
        for member in members:
            member_uuid = member.get("uuid")
            if member_uuid and self.add_player_to_discovery(member_uuid, uuid, "guild"):
                discovered += 1
        
        return discovered
    
    def save_player_stats(self, stats: Dict[str, Any], discovery_method: str) -> None:
        """Save player stats to database."""
        uuid = stats["uuid"]
        
        # Insert/update player record
        self.conn.execute('''
            INSERT OR REPLACE INTO players 
            (uuid, username, discovery_method, bedwars_level, last_login, last_updated)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            uuid, stats["username"], discovery_method, 
            stats["bedwars_level"], stats["last_login"]
        ))
        
        # Insert stats record
        self.conn.execute('''
            INSERT INTO bedwars_stats 
            (uuid, wins, losses, kills, deaths, final_kills, final_deaths, 
             beds_broken, beds_lost, winstreak, coins, experience, games_played,
             solos_wins, solos_losses, solos_winstreak,
             doubles_wins, doubles_losses, doubles_winstreak,
             threes_wins, threes_losses, threes_winstreak,
             fours_wins, fours_losses, fours_winstreak)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            uuid, stats["wins"], stats["losses"], stats["kills"], stats["deaths"],
            stats["final_kills"], stats["final_deaths"], stats["beds_broken"], 
            stats["beds_lost"], stats["winstreak"], stats["coins"], stats["experience"],
            stats["games_played"], stats["solos_wins"], stats["solos_losses"], 
            stats["solos_winstreak"], stats["doubles_wins"], stats["doubles_losses"],
            stats["doubles_winstreak"], stats["threes_wins"], stats["threes_losses"],
            stats["threes_winstreak"], stats["fours_wins"], stats["fours_losses"],
            stats["fours_winstreak"]
        ))
        
        self.conn.commit()
    
    def process_discovery_queue(self, limit: int = None) -> int:
        """Process players from the discovery queue."""
        if limit is None:
            limit = Config.PLAYERS_PER_CYCLE
        
        cursor = self.conn.execute(
            "SELECT id, discovered_uuid, source_uuid, discovery_method FROM player_discovery "
            "WHERE processed = FALSE ORDER BY discovered_at LIMIT ?",
            (limit,)
        )
        
        queue_items = cursor.fetchall()
        processed = 0
        
        for item_id, uuid, source_uuid, discovery_method in queue_items:
            print(f"🔍 Processing {uuid[:8]}... (via {discovery_method}) | Rate limit: {self.rate_limit_remaining}")
            
            # Check rate limit before processing
            if self.rate_limit_remaining <= 5:
                print(f"⚠️  Rate limit too low ({self.rate_limit_remaining}), stopping processing")
                break
            
            # Mark as processed first to avoid reprocessing
            self.conn.execute(
                "UPDATE player_discovery SET processed = TRUE WHERE id = ?", 
                (item_id,)
            )
            
            # Fetch player stats
            stats = self.fetch_player_stats(uuid)
            if stats:
                self.save_player_stats(stats, discovery_method)
                processed += 1
                
                # Try to discover more players through this player's guild (if rate limit allows)
                if self.rate_limit_remaining > 20:
                    guild_discovered = self.discover_players_from_guild(uuid)
                    if guild_discovered > 0:
                        print(f"  └─ 🏰 Found {guild_discovered} guild members")
                else:
                    print(f"  └─ ⚠️  Skipping guild discovery (rate limit: {self.rate_limit_remaining})")
            else:
                print(f"  └─ ❌ No valid stats (likely <{Config.MIN_BEDWARS_WINS} wins)")
            
            self.conn.commit()
        
        return processed
    
    def generate_leaderboards(self) -> None:
        """Generate leaderboard files for various stats."""
        print("📈 Generating leaderboards...")
        
        leaderboards = [
            ("wins", "Top 100 Players by Wins", "wins DESC"),
            ("wlr", "Top 100 Players by W/L Ratio", "wlr DESC"),
            ("final_kills", "Top 100 Players by Final Kills", "final_kills DESC"),
            ("fkdr", "Top 100 Players by FKDR", "fkdr DESC"),
            ("beds_broken", "Top 100 Players by Beds Broken", "beds_broken DESC"),
            ("bblr", "Top 100 Players by BB/BL Ratio", "bblr DESC"),
            ("solos_wins", "Top 100 Solo Players", "solos_wins DESC"),
            ("doubles_wins", "Top 100 Doubles Players", "doubles_wins DESC"),
            ("threes_wins", "Top 100 Threes Players", "threes_wins DESC"),
            ("fours_wins", "Top 100 Fours Players", "fours_wins DESC"),
        ]
        
        for lb_key, title, order_by in leaderboards:
            cursor = self.conn.execute(f'''
                SELECT p.username, s.wins, s.losses, s.final_kills, s.final_deaths, 
                       s.beds_broken, s.beds_lost, s.wlr, s.fkdr, s.bblr,
                       s.solos_wins, s.doubles_wins, s.threes_wins, s.fours_wins
                FROM bedwars_stats s
                JOIN players p ON s.uuid = p.uuid
                WHERE s.timestamp = (
                    SELECT MAX(timestamp) FROM bedwars_stats s2 WHERE s2.uuid = s.uuid
                )
                ORDER BY s.{order_by}
                LIMIT 100
            ''')
            
            results = cursor.fetchall()
            
            leaderboard = {
                "title": title,
                "generated_at": datetime.now().isoformat(),
                "total_players": len(results),
                "players": []
            }
            
            for rank, row in enumerate(results, 1):
                leaderboard["players"].append({
                    "rank": rank,
                    "username": row[0],
                    "wins": row[1],
                    "losses": row[2],
                    "final_kills": row[3],
                    "final_deaths": row[4],
                    "beds_broken": row[5],
                    "beds_lost": row[6],
                    "wlr": round(row[7], 3) if row[7] else 0,
                    "fkdr": round(row[8], 3) if row[8] else 0,
                    "bblr": round(row[9], 3) if row[9] else 0,
                    "solos_wins": row[10],
                    "doubles_wins": row[11],
                    "threes_wins": row[12],
                    "fours_wins": row[13],
                })
            
            # Save leaderboard
            lb_file = os.path.join(Config.LEADERBOARDS_DIR, f"{lb_key}_leaderboard.json")
            with open(lb_file, 'w') as f:
                json.dump(leaderboard, f, indent=2)
        
        print("✅ Leaderboards generated!")
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get current database statistics."""
        stats = {}
        
        cursor = self.conn.execute("SELECT COUNT(*) FROM players WHERE is_active = TRUE")
        stats["total_players"] = cursor.fetchone()[0]
        
        cursor = self.conn.execute("SELECT COUNT(*) FROM bedwars_stats")
        stats["total_stat_records"] = cursor.fetchone()[0]
        
        cursor = self.conn.execute("SELECT COUNT(*) FROM player_discovery WHERE processed = FALSE")
        stats["discovery_queue"] = cursor.fetchone()[0]
        
        cursor = self.conn.execute('''
            SELECT COUNT(*) FROM players 
            WHERE last_updated > datetime('now', '-1 day')
        ''')
        stats["updated_today"] = cursor.fetchone()[0]
        
        return stats
    
    def run_discovery_cycle(self) -> None:
        """Run a single discovery and processing cycle."""
        print(f"🔄 Starting discovery cycle... (Rate limit: {self.rate_limit_remaining})")
        
        db_stats = self.get_database_stats()
        print(f"📊 Database: {db_stats['total_players']} players, "
              f"{db_stats['discovery_queue']} in queue, "
              f"{db_stats['updated_today']} updated today")
        
        # Seed from leaderboards if we don't have many players
        if db_stats["total_players"] < 1000:
            self.seed_from_leaderboards()
        
        # Process discovery queue
        processed = self.process_discovery_queue()
        print(f"✅ Processed {processed} players this cycle")
        
        # Generate leaderboards every 10 cycles or if we have significant new data
        if db_stats["total_players"] % 100 == 0 or processed > 20:
            self.generate_leaderboards()
    
    def show_menu(self) -> str:
        """Display simplified interactive menu and get user choice."""
        print("" + "="*60)
        print("🎮 MASSIVE BEDWARS DATABASE TRACKER")
        print("="*60)
        
        db_stats = self.get_database_stats()
        print(f"📊 Current Database: {db_stats['total_players']} players")
        print(f"📋 Discovery Queue: {db_stats['discovery_queue']} pending")
        print(f"🔄 Rate Limit: {self.rate_limit_remaining} requests remaining")
        
        print("Options:")
        print("1. 🔍 Find new players (from Hypixel API + process queue)")
        print("2. 🎯 Add players manually")
        print("3. 📊 View database statistics")
        print("4. 🚪 Exit")
        
        while True:
            choice = input("Enter your choice (1-4): ").strip()
            if choice in ['1', '2', '3', '4']:
                return choice
            print("❌ Invalid choice. Please enter a number between 1-4.")
    
    def find_new_players(self) -> None:
        """Find new players from Hypixel API and process discovery queue."""
        print("🔍 Finding new players...")
        
        # First, seed from leaderboards if needed
        db_stats = self.get_database_stats()
        if db_stats["total_players"] < 1000 or db_stats["discovery_queue"] < 100:
            print("🌱 Seeding from Hypixel leaderboards...")
            seeded = self.seed_from_leaderboards()
            print(f"✅ Added {seeded} players from leaderboards")
        
        # Process discovery queue
        print("🏗️  Processing discovery queue...")
        processed = self.process_discovery_queue()
        print(f"✅ Processed {processed} players from queue")
        
        # Auto-generate leaderboards if significant progress
        if processed > 20:
            print("📈 Generating updated leaderboards...")
            self.generate_leaderboards()
    
    def view_database_stats(self) -> None:
        """Display detailed database statistics."""
        print("📊 Database Statistics:")
        
        db_stats = self.get_database_stats()
        
        print(f"👥 Total Players: {db_stats['total_players']:,}")
        print(f"📈 Total Stat Records: {db_stats['total_stat_records']:,}")
        print(f"📋 Discovery Queue: {db_stats['discovery_queue']:,}")
        print(f"🔄 Updated Today: {db_stats['updated_today']:,}")
        
        # Top players by wins
        cursor = self.conn.execute('''
            SELECT p.username, s.wins, s.wlr, s.fkdr
            FROM bedwars_stats s
            JOIN players p ON s.uuid = p.uuid
            WHERE s.timestamp = (
                SELECT MAX(timestamp) FROM bedwars_stats s2 WHERE s2.uuid = s.uuid
            )
            ORDER BY s.wins DESC
            LIMIT 10
        ''')
        
        top_players = cursor.fetchall()
        if top_players:
            print("🏆 Top 10 Players by Wins:")
            for i, (username, wins, wlr, fkdr) in enumerate(top_players, 1):
                print(f"  {i:2d}. {username}: {wins:,} wins (WLR: {wlr:.2f}, FKDR: {fkdr:.2f})")
        
        # Discovery method breakdown
        cursor = self.conn.execute('''
            SELECT discovery_method, COUNT(*) 
            FROM players 
            GROUP BY discovery_method 
            ORDER BY COUNT(*) DESC
        ''')
        
        discovery_methods = cursor.fetchall()
        if discovery_methods:
            print("🔍 Players by Discovery Method:")
            for method, count in discovery_methods:
                print(f"  {method}: {count:,} players")
        
        input("Press Enter to continue...")
    
    def run_interactive(self) -> None:
        """Run the tracker in simplified interactive mode."""
        while True:
            choice = self.show_menu()
            
            if choice == '1':
                self.find_new_players()
            elif choice == '2':
                self.add_manual_players()
            elif choice == '3':
                self.view_database_stats()
            elif choice == '4':
                print("👋 Goodbye!")
                break
    
    def run_automatic(self) -> None:
        """Main automatic discovery and tracking loop."""
        print("🚀 Starting Massive Bedwars Database Tracker")
        print(f"🎯 Target: {Config.TARGET_PLAYER_COUNT:,} players")
        print(f"⏱️  Cycle: {Config.PLAYERS_PER_CYCLE} players every {Config.CYCLE_SLEEP_SECONDS//60} minutes")
        
        # Initial seeding
        self.seed_from_leaderboards()
        
        try:
            cycle = 0
            while True:
                cycle += 1
                print(f"{'='*60}")
                print(f"🔄 CYCLE {cycle} | Rate Limit: {self.rate_limit_remaining}")
                print(f"{'='*60}")
                
                self.run_discovery_cycle()
                
                db_stats = self.get_database_stats()
                if db_stats["total_players"] >= Config.TARGET_PLAYER_COUNT:
                    print(f"🎉 Target reached! Database has {db_stats['total_players']} players.")
                    print("🔄 Continuing with maintenance updates...")
                
                print(f"💤 Sleeping for {Config.CYCLE_SLEEP_SECONDS//60} minutes...")
                time.sleep(Config.CYCLE_SLEEP_SECONDS)
                
        except KeyboardInterrupt:
            print("👋 Tracker stopped by user.")
            self.generate_leaderboards()  # Final leaderboard generation
        except Exception as e:
            print(f"💥 Unexpected error: {e}")
            raise

    def run(self) -> None:
        """Entry point - choose between interactive and automatic mode."""
        # Check if we should run in automatic mode (for backwards compatibility)
        if len(os.sys.argv) > 1 and os.sys.argv[1] == "--auto":
            self.run_automatic()
        else:
            self.run_interactive()


def main():
    """Entry point."""
    tracker = MassiveBedwarsTracker()
    tracker.run()


if __name__ == "__main__":
    main()