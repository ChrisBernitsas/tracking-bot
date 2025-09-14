import os
import re
import sys

NAMES_FILE = os.path.join("player_names", "scraped_names_to_process.txt")
NPC_REGEX = re.compile(r"^[a-z0-9]{10}$")

def check_npc_names(show_names: bool = False):
    if not os.path.exists(NAMES_FILE):
        print(f"Error: Scraped names file not found at '{NAMES_FILE}'.")
        return

    total_names = 0
    npc_like_names = 0
    npc_names_list = []

    try:
        with open(NAMES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                name = line.strip()
                if not name:
                    continue

                total_names += 1
                if NPC_REGEX.match(name):
                    npc_like_names += 1
                    if show_names:
                        npc_names_list.append(name)

        if total_names == 0:
            print(f"No names found in '{NAMES_FILE}'.")
            return

        percentage_npc_like = (npc_like_names / total_names) * 100

        print(f"Analysis of '{NAMES_FILE}':")
        print(f"Total names: {total_names}")
        print(f"NPC-like names (10 lowercase alphanumeric chars): {npc_like_names}")
        print(f"Percentage of NPC-like names: {percentage_npc_like:.2f}%")

        if show_names and npc_names_list:
            print("\n--- NPC-like Names ---")
            for name in npc_names_list:
                print(name)

    except IOError as e:
        print(f"An error occurred while reading the file: {e}")

if __name__ == "__main__":
    show_flag = "--show" in sys.argv
    check_npc_names(show_names=show_flag)
