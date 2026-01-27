# Problem
View expansion appears to bypass the normal SELECT resolver/planner path, leading to
"no such table"/"no such column" errors and incorrect column mappings in view.test.

# Scope
- Ensure view lookup returns a stored SELECT AST and expands into FROM terms
  using the same resolver/planner pipeline as direct SELECTs.
- Apply view expansion early (SQLite-style) before planning so name resolution
  and column mapping share the same codepath.

# Acceptance Criteria
- view.test passes for cases indicating expansion/resolution:
  view-1.2, view-1.4, view-1.6, view-2.1, view-3.2, view-3.4, view-3.5,
  view-5.2..view-5.9, view-6.1..view-6.2.
- Errors align with SQLite semantics (no such view/table vs no such column).
- No bespoke view-only resolver code paths remain (documented if any remain).

# Repro
`testfixture test/view.test`

# Notes
- View expansion should reuse the SELECT resolver/planner path, not parallel logic.
