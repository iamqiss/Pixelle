#!/usr/bin/env python3
"""
Repo renamer script
Author: Neo Qiss
Year: 2025
Description: Renames repositories and references in a project root.
"""

import os
import re
from pathlib import Path

# === Configuration ===
RENAMES = {
    "messenger": "messenger",
    "maintable": "maintable"
}

ROOT_DIR = Path(".").resolve()  # Change to your project root if needed

def case_insensitive_replace(text, old, new):
    """
    Replace all case-insensitive occurrences of `old` with `new`,
    preserving the original case where possible.
    """
    def repl(match):
        matched_text = match.group(0)
        if matched_text.isupper():
            return new.upper()
        elif matched_text.islower():
            return new.lower()
        elif matched_text.istitle():
            return new.title()
        else:
            return new
    pattern = re.compile(re.escape(old), re.IGNORECASE)
    return pattern.sub(repl, text)

def rename_files_and_folders(root_dir: Path, old_name: str, new_name: str):
    """
    Rename files and folders recursively in the root_dir that match old_name.
    """
    # Walk from bottom-up so renaming folders won't break traversal
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        # Rename files
        for filename in filenames:
            old_path = Path(dirpath) / filename
            new_filename = case_insensitive_replace(filename, old_name, new_name)
            if new_filename != filename:
                new_path = Path(dirpath) / new_filename
                old_path.rename(new_path)
                print(f"Renamed file: {old_path} -> {new_path}")

        # Rename directories
        for dirname in dirnames:
            old_path = Path(dirpath) / dirname
            new_dirname = case_insensitive_replace(dirname, old_name, new_name)
            if new_dirname != dirname:
                new_path = Path(dirpath) / new_dirname
                old_path.rename(new_path)
                print(f"Renamed directory: {old_path} -> {new_path}")

def replace_in_file(file_path: Path, renames: dict):
    """
    Replace all occurrences of old repo names inside a file.
    """
    try:
        text = file_path.read_text(encoding="utf-8")
        new_text = text
        for old, new in renames.items():
            new_text = case_insensitive_replace(new_text, old, new)
        if new_text != text:
            file_path.write_text(new_text, encoding="utf-8")
            print(f"Updated content in file: {file_path}")
    except Exception as e:
        print(f"⚠️ Could not process {file_path}: {e}")

def main():
    for old_name, new_name in RENAMES.items():
        print(f"\n🔄 Renaming '{old_name}' -> '{new_name}'")

        # Rename files and folders
        rename_files_and_folders(ROOT_DIR, old_name, new_name)

        # Replace contents inside files
        for path in ROOT_DIR.rglob("*"):
            if path.is_file():
                replace_in_file(path, {old_name: new_name})

    print("\n✅ Repo renaming complete!")

if __name__ == "__main__":
    main()
