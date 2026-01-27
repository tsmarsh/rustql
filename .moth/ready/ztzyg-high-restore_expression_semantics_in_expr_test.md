# Problem
Core expression semantics fail early in expr.test (invalid integer parsing,
transaction handling during expression evaluation), indicating resolver/VM
behavior drift.

# Scope
- Integer literal parsing and affinity rules used in expressions.
- Transaction state transitions triggered by expression evaluation.

# Acceptance Criteria
- expr.test passes for these representative cases:
  expr-1.2, expr-1.3, expr-1.4, expr-1.5, expr-1.6, expr-1.7.
- Errors no longer include "invalid integer" or "cannot start a transaction
  within a transaction" for the above cases.

# Repro
`testfixture test/expr.test`

# Observed Errors
- Error: invalid integer
- Error: cannot start a transaction within a transaction
