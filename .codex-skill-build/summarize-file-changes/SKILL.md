---
name: summarize-file-changes
description: Summarize staged, unstaged, untracked, or commit-to-commit file changes in a git repository with a step-by-step, reviewable walkthrough. Use when the user wants a detailed explanation of what changed, why it changed, how the important logic works, which files matter most, or how to review a diff safely.
---

# Summarize File Changes

## Overview

Explain diffs in an order a reviewer can follow without rereading every hunk first. Start with a complete inventory, then walk through the changes by theme or execution flow, and spend most of the detail budget on the logic that changes behavior.

## Workflow

1. Establish the comparison scope.
- If the user names explicit refs, compare those refs.
- Otherwise inspect the working tree: staged, unstaged, and untracked changes.
- Run `scripts/change_scope.py` first to inventory the change set and rough churn.
- Fall back to direct `git diff` commands when the script cannot represent a special case.

2. Read important diffs before summarizing.
- Never infer behavior from filenames alone.
- Inspect the highest-impact files first: runtime code, SQL, contracts, schemas, migrations, auth, configuration, CI, and deletions.
- Read tests and docs after the source changes so the explanation ties them back to the logic they verify or document.

3. Build a reviewable narrative.
- Start with scope: working tree or ref range, file count, and the main change themes.
- Then give a step-by-step walkthrough ordered by execution flow or change theme rather than raw diff order.
- Account for every changed file. Compress low-signal files, but do not omit them.
- Use file references when possible so the walkthrough is easy to check.

4. Elaborate important logic.
- For material behavior changes, explain the old behavior, new behavior, trigger or condition, data flow or state change, and why the change matters.
- Translate dense code into plain language or light pseudocode when that improves reviewability.
- Highlight invariants, branching rules, ordering guarantees, thresholds, and boundary conditions.

5. Close the review cleanly.
- Call out open questions, possible regressions, missing tests, or places where docs and code diverge.
- State clearly when a change is docs-only, formatting-only, generated, or otherwise low risk.
- If some files were unreadable or intentionally skipped, say so explicitly.

## Output Shape

Follow `references/review-structure.md` for the response structure and importance rubric.

Default order:
1. Scope and change themes.
2. Step-by-step walkthrough.
3. Important logic explained in more depth.
4. Risks, gaps, or open questions.

If the user asks for a shorter answer, keep the same order and compress low-impact files.

## Commands

Run the helper script from the skill directory or by absolute path.

```bash
python3 scripts/change_scope.py
python3 scripts/change_scope.py --base origin/main --head HEAD
git diff -- path/to/file
git diff --cached -- path/to/file
```
