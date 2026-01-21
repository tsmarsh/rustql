# Implement test_expr TCL Binding For Expr Tests

## Problem

Many expression tests use a `test_expr` command that doesn't exist in our TCL extension.

## Failing Tests

```
expr-1.1 expected: [4]
Error: invalid command name "test_expr"

expr-1.2 expected: [-4]
Error: invalid command name "test_expr"
```

## Analysis

The `test_expr` command is a SQLite test utility that evaluates expressions. It appears to be used like:

```tcl
test_expr expr-1.1 {i1=10, i2=20} {i1 + i2}
```

This creates a context with variables and evaluates an expression against them.

## Implementation Approach

1. Add `test_expr` command to the TCL extension
2. The command should:
   - Parse variable bindings
   - Create a temporary context
   - Evaluate the expression using SELECT
   - Return the result

## Files to Investigate

- `src/tcl_ext.rs` - TCL extension implementation
- `sqlite3/src/test_func.c` - SQLite's test_expr implementation
