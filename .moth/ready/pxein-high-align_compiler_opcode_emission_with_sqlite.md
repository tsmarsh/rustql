Ensure compiler emits SQLite-equivalent opcode sequences. Add a comparison harness that runs a query corpus and diffs opcode streams (SQLite EXPLAIN vs RustQL VDBE), with documented normalization rules for acceptable differences. Define pass criteria and integrate into tests/ or scripts/.

Architect notes (reset to ready):
- Current harness documents differences instead of enforcing parity. Tests log failures but never assert; they always pass. This is not acceptable.
- Normalization removes core control/transaction/cursor opcodes (Init, Goto, Transaction, Close) and treats AggStep1 vs AggStep as equivalent, which masks real divergences.
- "Semantic match" by opcode counts ignores order; this can hide control-flow differences.
- Docs explicitly bless Rust-specific opcodes (AggStep0/MaxOpcode/Unused). This violates the parity requirement.

Required fixes:
- Make opcode comparison tests fail on mismatches; no "document only" paths.
- Tighten normalization to only truly equivalent differences; justify each with SQLite behavior and tests.
- Compare ordered sequences after normalization, not just counts.
- Remove or align Rust-only opcodes; do not normalize them away.
