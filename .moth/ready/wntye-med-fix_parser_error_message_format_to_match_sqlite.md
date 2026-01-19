# Fix Parser Error Message Format to Match SQLite

## Problem

Parser error messages use a different format than SQLite. While our parser correctly rejects invalid SQL, the error messages don't match SQLite's format, causing test failures.

## Failing Tests (9 total)

### Format difference: "near X: syntax error" vs "expected Y at line Z, column W"
```
select1-7.1 expected: [1 {near ";": syntax error}]
select1-7.1 got:      [1 {expected expression at line 2, column 36}]

select1-7.2 expected: [1 {near "WHERE": syntax error}]
select1-7.2 got:      [1 {expected expression at line 2, column 40}]

select1-7.4 expected: [1 {near ";": syntax error}]
select1-7.4 got:      [1 {expected expression at line 2, column 35}]

select1-7.6 expected: [1 {near "FROM": syntax error}]
select1-7.6 got:      [1 {expected RParen at line 2, column 25}]

select1-7.7 expected: [1 {near ")": syntax error}]
select1-7.7 got:      [1 {expected expression at line 2, column 25}]

select1-7.8 expected: [1 {near ";": syntax error}]
select1-7.8 got:      [1 {expected expression at line 2, column 43}]
```

### "incomplete input" vs specific token expected
```
select1-7.3 expected: [1 {incomplete input}]
select1-7.3 got:      [1 {expected identifier or string at line 1, column 39}]
```

### Should error but doesn't
```
select1-7.5 expected: [1 {near "where": syntax error}]
select1-7.5 got:      [0 {33 11}]

select1-7.9 expected: [1 {near "ORDER": syntax error}]
select1-7.9 got:      [0 {}]
```

## SQLite Error Format

SQLite uses a consistent format:
- `near "TOKEN": syntax error` - when an unexpected token is encountered
- `incomplete input` - when the SQL ends unexpectedly

## Required Changes

1. **Error message formatting**: Change parser error output to use `near "TOKEN": syntax error` format
2. **Incomplete input detection**: Detect when SQL is incomplete vs having a syntax error
3. **Parser strictness**: select1-7.5 and 7.9 should be syntax errors but we accept them

## Severity: Medium

This is lower priority than the column naming and JOIN issues because:
- The parser is correctly rejecting invalid SQL (mostly)
- This is primarily a message format issue
- Two tests (7.5, 7.9) indicate actual parsing bugs where invalid SQL is accepted

## Files to Investigate

- `src/parser/mod.rs` - Error message generation
- `src/parser/lexer.rs` - Token information for error messages
