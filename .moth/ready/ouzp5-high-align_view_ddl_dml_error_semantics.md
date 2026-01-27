# Problem
View DDL/DML errors diverge from SQLite (DROP TABLE vs DROP VIEW, DML against
views). Missing checks allow invalid operations or wrong error text.

# Scope
- Ensure DML against views yields "cannot modify <view> because it is a view".
- Enforce DROP TABLE vs DROP VIEW error messaging and object-type detection.
- Validate "parameters are not allowed in views" and cross-db restrictions.

# Acceptance Criteria
- view.test passes for error semantic cases:
  view-1.1.100, view-2.2..view-2.4, view-4.1..view-4.5, view-12.1..view-12.2,
  view-13.1.
- Error messages match SQLite for the above cases.

# Repro
`testfixture test/view.test`
