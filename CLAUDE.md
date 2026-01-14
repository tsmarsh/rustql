# Moth Agent Guide

This guide helps LLM agents work effectively with moth, a git-based file issue tracker.

## Overview

Moth stores issues as markdown files in `.moth/` directories organized by status (ready, doing, done). Each issue has a unique ID, severity, and slug derived from the title.

## File Structure

```
.moth/
├── config.yml          # Project configuration
├── .current            # Current issue ID (when working on an issue)
├── ready/              # Issues ready to start
│   └── {id}-{severity}-{slug}.md
├── doing/              # Issues in progress
│   └── {id}-{severity}-{slug}.md
└── done/               # Completed issues
    └── {id}-{severity}-{slug}.md
```

Prioritized issues have a numeric prefix: `001-{id}-{severity}-{slug}.md`

## Workflow Commands

### Viewing Issues

```bash
# List all active issues (excludes done)
moth ls

# List issues in specific status
moth ls -t ready
moth ls -t doing

# List all issues including done
moth ls -a

# Filter by severity
moth ls -s high
moth ls -s crit

# Show current issue details
moth show

# Show specific issue
moth show {id}
```

### Working on Issues

```bash
# Start working on an issue (moves to doing, sets as current)
moth start {id}

# Mark issue as done
moth done {id}

# Mark current issue as done
moth done

# Move issue to any status
moth mv {id} {status}
```

### Creating Issues

```bash
# Create new issue (opens editor)
moth new "Fix login bug"

# Create with severity
moth new "Critical security fix" -s crit

# Create without opening editor
moth new "Quick fix" --no-edit

# Create and immediately start working
moth new "Urgent task" --start
```

### Issue Management

```bash
# Edit issue content
moth edit {id}

# Delete issue
moth rm {id}

# Change severity
moth severity {id} high
```

### Priority Management

```bash
# Set priority number
moth priority {id} 1

# Move to top priority
moth priority {id} top

# Move to bottom (removes priority)
moth priority {id} bottom

# Position relative to another issue
moth priority {id} above {other_id}
moth priority {id} below {other_id}

# Renumber priorities sequentially
moth compact
moth compact ready
```

## Severity Levels

From highest to lowest:
- `crit` - Critical, must fix immediately
- `high` - High priority
- `med` - Medium priority (default)
- `low` - Low priority

## Partial ID Matching

All commands accept partial IDs. If you have issue `abc12`, you can use:
- `moth show abc12` (full)
- `moth show abc1` (partial)
- `moth show a` (if unambiguous)

## Git Integration

### Commit Hook

Moth can auto-prefix commit messages with the current issue ID:

```bash
# Install the hook
moth hook install

# With existing hook
moth hook install --append

# Remove hook
moth hook uninstall
```

When active, commits are prefixed: `[abc12] Your commit message`

### Commit Message Format

When committing changes related to an issue, prefix with the issue ID:

```bash
git commit -m "[abc12] Fix authentication bypass"
```

This links commits to issues in the report.

## Generating Reports

```bash
# Full history as CSV
moth report

# From specific commit
moth report --since abc123

# Between commits
moth report --since abc123 --until def456
```

Output includes: commit info, story changes (created, moved, edited, deleted), and code commits referencing issues.

## Repository Notes

- `sqlite3/src/parse.y` is a Lemon grammar (not yacc/bison). Rust cannot consume Lemon directly.
- If translating the parser, prefer a Rust-native generator like `lalrpop` or a hand-written parser; keep `parse.y` as the source of truth.
- SQLite’s “Why C?” rationale (`https://sqlite.org/whyc.html`) is important context; we intentionally diverge by using Rust while preserving behavior and design intent.
- For FTS and other extensions, **do not** implement placeholder or approximate behavior. Port directly from the SQLite C sources in `sqlite3/ext/` and keep semantics aligned.

## Agent Best Practices (Team Workflow)

This is a team project. Multiple agents may work on issues concurrently. Following this workflow ensures coordination and prevents conflicts.

### Before Starting ANY Implementation

**MANDATORY: Always sync with remote and claim your work before coding.**

```bash
# 1. Fetch latest changes from remote
git fetch origin && git pull origin main

# 2. Check if you have work in progress
moth ls -t doing

# 3a. If you have a moth in "doing", resume that work
moth show

# 3b. If no work in progress, check what's available
moth ls -t ready -s crit   # Check critical issues first
moth ls -t ready -s high   # Then high priority
moth ls -t ready           # All ready issues

# 4. Start the moth BEFORE writing any code
moth start {id}

# 5. Push the status change so teammates know you're working on it
git add .moth/ && git commit -m "[{id}] Started work on: {title}"
git push origin main
```

**Why this matters:** If you don't push the moth status, another agent may start the same work, causing merge conflicts and wasted effort.

### During Development

1. Make changes and commit frequently
2. **Always prefix commits with issue ID:** `[{id}] description`
3. Push regularly to share progress: `git push origin main`
4. Keep issue content updated if requirements change

```bash
# Example commit workflow
git add src/
git commit -m "[abc12] Implement btree page split logic"
git push origin main
```

### Completing Work

**MANDATORY: Push completion status so teammates can pick up new work.**

```bash
# 1. Ensure all changes are committed
git status

# 2. Mark the moth as done
moth done

# 3. Commit and push the completion
git add .moth/ && git commit -m "[{id}] Completed: {title}"
git push origin main

# 4. Fetch latest to see if others completed work
git fetch origin && git pull origin main

# 5. Pick up the next issue (go back to "Before Starting" workflow)
moth ls -t ready
```

### Resuming After Context Loss

If your session was interrupted or you're resuming work:

```bash
# 1. Always fetch first
git fetch origin && git pull origin main

# 2. Check what you were working on
moth ls -t doing

# 3. If something is in "doing", that's your current work
moth show

# 4. If nothing in "doing", start fresh with a new issue
moth ls -t ready
moth start {id}
git add .moth/ && git commit -m "[{id}] Started work on: {title}"
git push origin main
```

### Creating New Issues

When user requests new work:
```bash
# 1. Create issue
moth new "Title" -s {severity} --no-edit

# 2. Commit the new issue
git add .moth/ && git commit -m "Created moth: {title}"
git push origin main

# 3. Optionally start immediately
moth start {id}
git add .moth/ && git commit -m "[{id}] Started work on: {title}"
git push origin main
```

### Checking Status

```bash
# Quick status check
moth ls

# What am I working on?
moth show

# What are teammates working on?
moth ls -t doing

# Full project state
moth ls -a
```

### Conflict Resolution

If you encounter merge conflicts in `.moth/`:

```bash
# 1. Fetch and see the conflict
git fetch origin && git pull origin main

# 2. If another agent took your issue, pick a different one
moth ls -t ready

# 3. If you both completed the same work, coordinate with the team
```

### Summary: The Golden Rules

1. **Always `git pull` before starting work**
2. **Always `moth start` before writing code**
3. **Always push after `moth start`** - claim your work publicly
4. **Always push after `moth done`** - signal completion
5. **Always `git pull` after completing work** - stay in sync

## Configuration Reference

`.moth/config.yml`:

```yaml
statuses:
  - name: ready
    dir: ready
    prioritized: true    # Enable priority ordering
  - name: doing
    dir: doing
  - name: done
    dir: done

default_severity: med    # Default for new issues
editor: vi               # Editor for moth edit
id_length: 5             # Length of generated IDs
no_edit: false           # Skip editor on moth new

priority:
  auto_compact: false    # Auto-renumber after priority changes
```

## Common Patterns

### Pick up next priority issue
```bash
moth ls -t ready
moth start {first-id}
```

### Quick bug fix
```bash
moth new "Fix typo in header" -s low --no-edit --start
# make fix
git commit -m "[{id}] Fix typo"
moth done
```

### Triage incoming work
```bash
moth new "Investigate performance issue" -s med --no-edit
moth priority {id} top
```

### Review what was done
```bash
moth ls -t done
moth report --since HEAD~10
```
