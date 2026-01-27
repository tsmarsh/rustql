# Problem
Planner/resolver mismatch yields missing tables/columns and schema-level
violations across where*.test and select3/4/7. This indicates incorrect
name resolution or plan generation in complex WHERE/SELECT cases.

# Scope
- Name resolution and table alias handling in WHERE.
- Planner choices for multi-table/compound selects and subqueries.

# Acceptance Criteria
- where/where2/where3 pass for these representative cases:
  where-1.8.2, where-4.2, where-5.2, where-5.3a, where-5.3b, where-5.3c,
  where2-1.1, where2-2.1, where2-2.3, where2-3.1, where2-3.2, where2-4.1,
  where3-1.1, where3-1.2.
- select3/4/7 pass for these representative cases:
  select3-2.3.2, select3-2.4, select3-2.5, select3-2.10, select3-2.11,
  select3-2.12, select4-1.1d, select4-1.1e, select4-1.1g, select4-1.2,
  select4-2.2, select4-3.1.2, select7-5.1..select7-5.4.
- Errors no longer include "no such database", "no such column", or
  "misuse of aggregate" for the above cases.

# Repro
`testfixture test/where.test`
`testfixture test/where2.test`
`testfixture test/where3.test`
`testfixture test/select3.test`
`testfixture test/select4.test`
`testfixture test/select7.test`

# Observed Errors
- Error: no such database: db
- Error: no such column: x
- Error: no such column: n
- Error: misuse of aggregate: count()
- Error: no such function: avg
- Error: near "NULL": syntax error
