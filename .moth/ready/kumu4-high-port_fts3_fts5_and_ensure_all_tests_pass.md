# Epic: FTS3/FTS5 (True Port Parity)

## Intent
Port SQLite's FTS3/FTS5 implementations directly from `sqlite3/ext/` and wire
into the vtab framework with matching behavior, query semantics, and error
handling.

## Architectural Alignment
- FTS modules are vtabs, not special cases.
- Tokenizer registry and auxiliary functions align with SQLite semantics.

## Scope
- FTS3/FTS5 core logic, shadow tables, and segment storage.
- MATCH parsing, query planning hooks, and ranking functions.
- Tokenizer APIs and module registration via vtab registry.

## Out of Scope
- New FTS features not present in SQLite.
- Optimizations that change observable behavior.

## Acceptance Criteria (How We Assert This Is Done)
- FTS3/FTS5 module behavior matches SQLite semantics for:
  - MATCH expressions and phrase queries,
  - shadow table layout and updates,
  - ranking functions (bm25/highlight/snippet),
  - tokenizer behavior and error messages.
- No module-specific VDBE/executor dispatch for FTS.
- SQLite Tcl tests listed in this epic pass in RustQL, with no untracked
  exclusions.
- SQLite comparison runs show identical results for a curated FTS query suite
  (stored as RustQL integration tests with recorded expected output).
- `docs/differences.md` updated to remove FTS-related gaps when parity is met.

## Test Targets (SQLite Tcl)
Run each with `testfixture test/<name>.test`:
- `sqlite3/test/fts3.test`
- `sqlite3/test/fts3aa.test`
- `sqlite3/test/fts3ab.test`
- `sqlite3/test/fts3ac.test`
- `sqlite3/test/fts3ad.test`
- `sqlite3/test/fts3ae.test`
- `sqlite3/test/fts3af.test`
- `sqlite3/test/fts3ag.test`
- `sqlite3/test/fts3ah.test`
- `sqlite3/test/fts3ai.test`
- `sqlite3/test/fts3aj.test`
- `sqlite3/test/fts3ak.test`
- `sqlite3/test/fts3al.test`
- `sqlite3/test/fts3am.test`
- `sqlite3/test/fts3an.test`
- `sqlite3/test/fts3ao.test`
- `sqlite3/test/fts3atoken.test`
- `sqlite3/test/fts3atoken2.test`
- `sqlite3/test/fts3auto.test`
- `sqlite3/test/fts3aux1.test`
- `sqlite3/test/fts3aux2.test`
- `sqlite3/test/fts3b.test`
- `sqlite3/test/fts3c.test`
- `sqlite3/test/fts3comp1.test`
- `sqlite3/test/fts3conf.test`
- `sqlite3/test/fts3corrupt.test`
- `sqlite3/test/fts3corrupt2.test`
- `sqlite3/test/fts3corrupt3.test`
- `sqlite3/test/fts3corrupt4.test`
- `sqlite3/test/fts3corrupt5.test`
- `sqlite3/test/fts3corrupt6.test`
- `sqlite3/test/fts3corrupt7.test`
- `sqlite3/test/fts3cov.test`
- `sqlite3/test/fts3d.test`
- `sqlite3/test/fts3defer.test`
- `sqlite3/test/fts3defer2.test`
- `sqlite3/test/fts3defer3.test`
- `sqlite3/test/fts3drop.test`
- `sqlite3/test/fts3dropmod.test`
- `sqlite3/test/fts3e.test`
- `sqlite3/test/fts3expr.test`
- `sqlite3/test/fts3expr2.test`
- `sqlite3/test/fts3expr3.test`
- `sqlite3/test/fts3expr4.test`
- `sqlite3/test/fts3expr5.test`
- `sqlite3/test/fts3f.test`
- `sqlite3/test/fts3fault.test`
- `sqlite3/test/fts3fault2.test`
- `sqlite3/test/fts3fault3.test`
- `sqlite3/test/fts3first.test`
- `sqlite3/test/fts3fuzz001.test`
- `sqlite3/test/fts3integrity.test`
- `sqlite3/test/fts3join.test`
- `sqlite3/test/fts3malloc.test`
- `sqlite3/test/fts3matchinfo.test`
- `sqlite3/test/fts3matchinfo2.test`
- `sqlite3/test/fts3misc.test`
- `sqlite3/test/fts3near.test`
- `sqlite3/test/fts3offsets.test`
- `sqlite3/test/fts3prefix.test`
- `sqlite3/test/fts3prefix2.test`
- `sqlite3/test/fts3query.test`
- `sqlite3/test/fts3rank.test`
- `sqlite3/test/fts3rnd.test`
- `sqlite3/test/fts3shared.test`
- `sqlite3/test/fts3snippet.test`
- `sqlite3/test/fts3snippet2.test`
- `sqlite3/test/fts3sort.test`
- `sqlite3/test/fts3tok1.test`
- `sqlite3/test/fts3tok_err.test`
- `sqlite3/test/fts3varint.test`
- `sqlite3/test/fts4aa.test`
- `sqlite3/test/fts4check.test`
- `sqlite3/test/fts4content.test`
- `sqlite3/test/fts4docid.test`
- `sqlite3/test/fts4growth.test`
- `sqlite3/test/fts4growth2.test`
- `sqlite3/test/fts4incr.test`
- `sqlite3/test/fts4intck1.test`
- `sqlite3/test/fts4langid.test`
- `sqlite3/test/fts4lastrowid.test`
- `sqlite3/test/fts4merge.test`
- `sqlite3/test/fts4merge2.test`
- `sqlite3/test/fts4merge3.test`
- `sqlite3/test/fts4merge4.test`
- `sqlite3/test/fts4merge5.test`
- `sqlite3/test/fts4min.test`
- `sqlite3/test/fts4noti.test`
- `sqlite3/test/fts4onepass.test`
- `sqlite3/test/fts4opt.test`
- `sqlite3/test/fts4record.test`
- `sqlite3/test/fts4rename.test`
- `sqlite3/test/fts4umlaut.test`
- `sqlite3/test/fts4unicode.test`
- `sqlite3/test/fts4upfrom.test`
- `sqlite3/test/fts-9fd058691.test`

FTS5 tests live under `sqlite3/ext/fts5/test/` and must pass in full:
- `sqlite3/ext/fts5/test/*.test`

## Verification Steps
- Run FTS-focused Tcl tests and compare outputs to SQLite.
- Use a deterministic FTS query corpus (added to RustQL tests) and compare
  results with upstream SQLite CLI.
