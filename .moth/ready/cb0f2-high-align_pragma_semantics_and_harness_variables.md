# Problem
pragma/pragma2 failures indicate missing pragma handlers and harness variables
(rootpage, parser_trace), plus script inclusion issues in pragma2.

# Scope
- Implement or stub expected PRAGMA handlers to match SQLite behavior.
- Ensure harness variables (e.g., rootpage) are populated where tests expect.
- Fix pragma2 include/extra script resolution.

# Acceptance Criteria
- pragma/pragma2 pass for these representative cases:
  pragma-1.9.1, pragma-1.9.2, pragma-1.10, pragma-1.11.1, pragma-1.11.2,
  pragma-1.12, pragma2-1.3, pragma2-1.4, pragma2-2.5, pragma2-3.1,
  pragma2-3.2, pragma2-3.3.
- Errors no longer include "unknown pragma", missing rootpage variable, or
  missing pragma2 include file.

# Repro
`testfixture test/pragma.test`
`testfixture test/pragma2.test`

# Observed Errors
- Error: unknown pragma: parser_trace
- Error: unknown pragma: bogus
- Error: can't read "rootpage": no such variable
- Error in pragma2.test: couldn't read file "pragma2": no such file or directory
