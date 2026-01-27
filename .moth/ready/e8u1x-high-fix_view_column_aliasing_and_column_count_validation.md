# Problem
View column aliases and column-count validation are not enforced, causing
mismatched row shapes and missing errors (e.g., view-3.3.5/3.3.6).

# Scope
- Apply explicit view column list mapping to expanded SELECT result columns.
- Enforce SQLite-style column count validation for CREATE VIEW.
- Preserve column naming/alias rules during expansion and projection.

# Acceptance Criteria
- view.test passes for column mapping/error cases:
  view-2.1, view-3.3.2, view-3.3.3, view-3.3.5, view-3.3.6, view-8.4, view-8.5,
  view-9.3..view-9.6, view-10.2, view-11.1.
- Error messages match SQLite ("expected N columns for 'view' but got M").

# Repro
`testfixture test/view.test`
