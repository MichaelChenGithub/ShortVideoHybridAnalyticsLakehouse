# Review Structure

Use this structure when the user asks for a full walkthrough of a diff.

## Recommended response shape

1. Scope
- State whether the summary covers the working tree or a ref range.
- State the number of files and the main change themes in 1-3 sentences.

2. Step-by-step walkthrough
- Order the walkthrough by execution flow or change theme.
- For each step, name the files involved, explain what changed, and why that step matters.
- Mention every changed file at least once, even if some are grouped together as low-signal edits.

3. Important logic
- Spend extra detail on edits that change behavior, data shape, ordering, thresholds, permissions, retries, or failure handling.
- When useful, explain the logic in terms of:
  - input or trigger,
  - branch or transformation,
  - output or side effect,
  - important edge case.

4. Risks or open questions
- Call out missing tests, partial migrations, config drift, suspicious asymmetry, or docs that no longer match code.
- If the diff is low risk, say why it is low risk.

## Importance rubric

High importance:
- Runtime logic, SQL semantics, contracts, schemas, state transitions, auth, networking, deployment behavior, deletes, and migrations.

Medium importance:
- Tests that lock in behavior, config changes with limited blast radius, docs tied to runtime behavior, and nontrivial refactors.

Low importance:
- Formatting, comments, snapshots, generated outputs, simple renames, and mechanical file moves with no semantic change.

## Completeness checklist

- Verify that every changed file is mentioned.
- Verify that renamed and deleted paths are accounted for.
- Verify that tests are mapped to the source or behavior they cover.
- Verify that both staged and unstaged changes were considered when summarizing a working tree.
- Verify that any skipped files, binary assets, or generated files are disclosed.
