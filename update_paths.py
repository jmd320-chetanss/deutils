# Databricks notebook source
import os
import sys
from pathlib import Path

def parse_path_until(path: str, stop_name: str):
    path = Path(path)
    parts = []

    for part in path.parts:
        parts.append(part)
        if part == stop_name:
            break

    return Path(*parts)

cwd = os.getcwd()

# Name of the repo, used to calculate the absolute path to the repo dynamically
repo_name = "Data Platform"

# Absolute path to the root of the repo
repo_root = parse_path_until(cwd, repo_name)

# Index to insert the repo root at
repo_root_index = 1

if repo_root not in sys.path:
    print(f"Inserting repo_root '{repo_root}' in search paths at index 1...")
    sys.path.insert(repo_root_index, repo_root)

