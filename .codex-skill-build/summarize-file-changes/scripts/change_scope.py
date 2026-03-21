#!/usr/bin/env python3
"""Print a compact git change inventory for a working tree or ref range."""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections import Counter
from pathlib import Path


def run_git(repo: Path, args: list[str]) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "git command failed"
        raise RuntimeError(message)
    return result.stdout.rstrip("\n")


def resolve_repo(path: str) -> Path:
    candidate = Path(path).resolve()
    root = run_git(candidate, ["rev-parse", "--show-toplevel"])
    return Path(root)


def print_section(title: str, body: str) -> None:
    if not body.strip():
        return
    print(f"{title}:")
    for line in body.splitlines():
        print(f"  {line}")
    print()


def summarize_status(body: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not body.strip():
        return counts
    for line in body.splitlines():
        code = line[:2]
        if code == "!!":
            continue
        counts["listed"] += 1
        if code == "??":
            counts["untracked"] += 1
            continue
        if code[0] != " ":
            counts["staged"] += 1
        if code[1] != " ":
            counts["unstaged"] += 1
    return counts


def summarize_name_status(body: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for line in body.splitlines():
        if not line:
            continue
        counts[line[0]] += 1
        counts["listed"] += 1
    return counts


def print_working_tree(repo: Path) -> int:
    branch = run_git(repo, ["branch", "--show-current"]) or "(detached HEAD)"
    status = run_git(repo, ["status", "--short", "--untracked-files=all"])
    staged_name_status = run_git(repo, ["diff", "--cached", "--name-status", "-M"])
    staged_numstat = run_git(repo, ["diff", "--cached", "--numstat", "-M"])
    unstaged_name_status = run_git(repo, ["diff", "--name-status", "-M"])
    unstaged_numstat = run_git(repo, ["diff", "--numstat", "-M"])

    print(f"repo_root: {repo}")
    print("mode: working-tree")
    print(f"branch: {branch}")
    print()

    if not any(
        section.strip()
        for section in [status, staged_name_status, staged_numstat, unstaged_name_status, unstaged_numstat]
    ):
        print("summary:")
        print("  clean working tree")
        return 0

    counts = summarize_status(status)
    print("summary:")
    print(f"  listed_files: {counts.get('listed', 0)}")
    print(f"  staged_entries: {counts.get('staged', 0)}")
    print(f"  unstaged_entries: {counts.get('unstaged', 0)}")
    print(f"  untracked_entries: {counts.get('untracked', 0)}")
    print()

    print_section("status", status)
    print_section("staged_name_status", staged_name_status)
    print_section("staged_numstat", staged_numstat)
    print_section("unstaged_name_status", unstaged_name_status)
    print_section("unstaged_numstat", unstaged_numstat)
    return 0


def print_range(repo: Path, base: str, head: str) -> int:
    name_status = run_git(repo, ["diff", "--name-status", "-M", base, head])
    numstat = run_git(repo, ["diff", "--numstat", "-M", base, head])
    stat = run_git(repo, ["diff", "--stat", "-M", base, head])
    counts = summarize_name_status(name_status)

    print(f"repo_root: {repo}")
    print("mode: range")
    print(f"base: {base}")
    print(f"head: {head}")
    print()

    if not any(section.strip() for section in [name_status, numstat, stat]):
        print("summary:")
        print("  no differences")
        return 0

    print("summary:")
    print(f"  listed_files: {counts.get('listed', 0)}")
    print(f"  added: {counts.get('A', 0)}")
    print(f"  modified: {counts.get('M', 0)}")
    print(f"  deleted: {counts.get('D', 0)}")
    print(f"  renamed: {counts.get('R', 0)}")
    print(f"  copied: {counts.get('C', 0)}")
    print()

    print_section("name_status", name_status)
    print_section("numstat", numstat)
    print_section("stat", stat)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print a compact git change inventory for summarizing diffs.",
    )
    parser.add_argument(
        "--repo",
        default=".",
        help="Path inside the target git repository. Defaults to the current directory.",
    )
    parser.add_argument(
        "--base",
        help="Base ref for a commit-to-commit comparison. If omitted, inspect the working tree.",
    )
    parser.add_argument(
        "--head",
        default="HEAD",
        help="Head ref for a commit-to-commit comparison. Defaults to HEAD.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        repo = resolve_repo(args.repo)
        if args.base:
            return print_range(repo, args.base, args.head)
        return print_working_tree(repo)
    except RuntimeError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
