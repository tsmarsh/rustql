# RustQL Makefile for TCL Test Suite
#
# Usage:
#   make test                    - Run all TCL tests in parallel
#   make test -j8                - Run tests with 8 parallel jobs
#   make test-seq                - Run all TCL tests sequentially (old behavior)
#   make test-select1            - Run select1.test
#   make test-basic              - Run basic RustQL tests (quick)
#   make clean                   - Remove build artifacts and test results
#   make tcl-extension           - Build only the TCL extension
#
# Test results are stored in test-results/

CARGO := cargo
TCLSH := tclsh
TIMEOUT := 120

# Parallel jobs (can override with make test JOBS=8)
JOBS ?= $(shell nproc 2>/dev/null || echo 4)

# Build outputs
TCL_EXT := target/release/librustql.so
RUSTQL_BIN := target/release/rustql

# Test infrastructure
TEST_DIR := sqlite3/test
RESULT_DIR := test-results
TCL_WRAPPER := scripts/run_sqlite_test.tcl

# List of SQLite tests to run (add more as compatibility improves)
SQLITE_TESTS := \
	select1 select2 select3 select4 select5 select6 select7 \
	insert insert2 insert3 \
	update delete \
	expr \
	where where2 where3 \
	join join2 join3 \
	subquery \
	trigger trigger2 \
	view \
	index index2 \
	null \
	types types2 types3 \
	cast \
	coalesce \
	between \
	distinct \
	limit \
	orderby1 \
	func func2 func3 \
	date \
	printf \
	like like2 \
	glob \
	attach \
	vacuum \
	pragma pragma2 \
	trans trans2 \
	savepoint \
	collate1 collate2 collate3

# Source files for dependency tracking
RUST_SOURCES := $(shell find src -name '*.rs' 2>/dev/null)

# Generate test result targets
TEST_RESULTS := $(addprefix $(RESULT_DIR)/,$(addsuffix .result,$(SQLITE_TESTS)))

.PHONY: all test test-seq test-basic test-all clean clean-results tcl-extension rustql help list-tests test-report pass-rates test-summary

all: tcl-extension

help:
	@echo "RustQL TCL Test Suite"
	@echo ""
	@echo "Build Targets:"
	@echo "  make tcl-extension     - Build the TCL extension (librustql.so)"
	@echo "  make rustql            - Build the rustql binary"
	@echo ""
	@echo "Test Targets:"
	@echo "  make test              - Run all TCL tests in PARALLEL (default)"
	@echo "  make test -j8          - Run tests with 8 parallel jobs"
	@echo "  make test-seq          - Run all TCL tests sequentially"
	@echo "  make test-basic        - Run basic RustQL TCL tests (quick smoke test)"
	@echo "  make test-<name>       - Run a specific test (e.g., make test-select1)"
	@echo "  make pass-rates        - Show pass rates from existing logs"
	@echo "  make test-report       - Show pass/fail statistics from existing logs"
	@echo ""
	@echo "Other Targets:"
	@echo "  make list-tests        - List all available test targets"
	@echo "  make clean             - Remove build artifacts and test results"
	@echo "  make clean-results     - Remove only test results"
	@echo ""
	@echo "Test results are written to: $(RESULT_DIR)/"

list-tests:
	@echo "Available test targets:"
	@for t in $(SQLITE_TESTS); do echo "  make test-$$t"; done

# Build the TCL extension
tcl-extension: $(TCL_EXT)

$(TCL_EXT): $(RUST_SOURCES) Cargo.toml
	$(CARGO) build --release --features tcl --lib

# Build the rustql binary
rustql: $(RUSTQL_BIN)

$(RUSTQL_BIN): $(RUST_SOURCES) Cargo.toml
	$(CARGO) build --release

# Create result directory
$(RESULT_DIR):
	@mkdir -p $(RESULT_DIR)

# ============================================================================
# Parallel test execution (default)
# ============================================================================

# Run all tests in parallel, then show summary
test: $(TCL_EXT) | $(RESULT_DIR)
	@echo "Running SQLite TCL test suite in parallel ($(JOBS) jobs)..."
	@echo ""
	@$(MAKE) --no-print-directory -j$(JOBS) $(TEST_RESULTS)
	@$(MAKE) --no-print-directory test-summary

# Pattern rule for generating individual test results (used by parallel execution)
$(RESULT_DIR)/%.result: $(TCL_EXT) | $(RESULT_DIR)
	@if [ -f "$(TEST_DIR)/$*.test" ]; then \
		if timeout $(TIMEOUT) $(TCLSH) $(TCL_WRAPPER) $* > $(RESULT_DIR)/$*.log 2>&1; then \
			echo "PASSED" > $@; \
			printf "  %-20s PASSED\n" "$*"; \
		else \
			echo "FAILED" > $@; \
			printf "  %-20s FAILED\n" "$*"; \
		fi; \
	else \
		echo "SKIPPED" > $@; \
		printf "  %-20s SKIPPED (not found)\n" "$*"; \
	fi

# Print test summary after parallel execution
test-summary:
	@echo ""
	@echo "========================================"
	@echo "Test Summary"
	@echo "========================================"
	@passed=$$(grep -l "PASSED" $(RESULT_DIR)/*.result 2>/dev/null | wc -l); \
	failed=$$(grep -l "FAILED" $(RESULT_DIR)/*.result 2>/dev/null | wc -l); \
	skipped=$$(grep -l "SKIPPED" $(RESULT_DIR)/*.result 2>/dev/null | wc -l); \
	total=$$((passed + failed + skipped)); \
	echo "Passed:  $$passed"; \
	echo "Failed:  $$failed"; \
	echo "Skipped: $$skipped"; \
	echo "Total:   $$total"; \
	echo ""; \
	if [ $$failed -gt 0 ]; then \
		echo "Failed tests:"; \
		for f in $(RESULT_DIR)/*.result; do \
			if grep -q "FAILED" "$$f" 2>/dev/null; then \
				name=$$(basename "$$f" .result); \
				echo "  - $$name (see $(RESULT_DIR)/$$name.log)"; \
			fi; \
		done; \
	fi

# ============================================================================
# Sequential test execution (old behavior)
# ============================================================================

test-seq: $(TCL_EXT) $(RESULT_DIR)
	@echo "Running SQLite TCL test suite sequentially..."
	@echo ""
	@passed=0; failed=0; skipped=0; \
	for t in $(SQLITE_TESTS); do \
		if [ -f "$(TEST_DIR)/$$t.test" ]; then \
			printf "%-20s " "$$t"; \
			if timeout $(TIMEOUT) $(TCLSH) $(TCL_WRAPPER) $$t > $(RESULT_DIR)/$$t.log 2>&1; then \
				echo "PASSED"; \
				echo "PASSED" > $(RESULT_DIR)/$$t.result; \
				passed=$$((passed + 1)); \
			else \
				echo "FAILED"; \
				echo "FAILED" > $(RESULT_DIR)/$$t.result; \
				failed=$$((failed + 1)); \
			fi; \
		else \
			printf "%-20s SKIPPED (not found)\n" "$$t"; \
			echo "SKIPPED" > $(RESULT_DIR)/$$t.result; \
			skipped=$$((skipped + 1)); \
		fi; \
	done; \
	echo ""; \
	echo "========================================"; \
	echo "Test Summary"; \
	echo "========================================"; \
	echo "Passed:  $$passed"; \
	echo "Failed:  $$failed"; \
	echo "Skipped: $$skipped"; \
	echo "Total:   $$((passed + failed + skipped))"; \
	echo ""; \
	if [ $$failed -gt 0 ]; then \
		echo "Failed tests:"; \
		for t in $(SQLITE_TESTS); do \
			if [ -f "$(RESULT_DIR)/$$t.result" ] && grep -q "FAILED" "$(RESULT_DIR)/$$t.result"; then \
				echo "  - $$t (see $(RESULT_DIR)/$$t.log)"; \
			fi; \
		done; \
	fi

# ============================================================================
# Individual test targets
# ============================================================================

# Run basic RustQL tests (quick smoke test)
test-basic: $(TCL_EXT)
	@echo "Running basic RustQL TCL tests..."
	@$(TCLSH) tests/run_tcl_test.tcl tests/basic_tcl.test

# Pattern rule for running individual tests interactively (with output to terminal)
test-%: $(TCL_EXT) $(RESULT_DIR)
	@if [ -f "$(TEST_DIR)/$*.test" ]; then \
		echo "Running $*.test..."; \
		echo ""; \
		if timeout $(TIMEOUT) $(TCLSH) $(TCL_WRAPPER) $* 2>&1 | tee $(RESULT_DIR)/$*.log; then \
			echo "PASSED" > $(RESULT_DIR)/$*.result; \
		else \
			echo "FAILED" > $(RESULT_DIR)/$*.result; \
		fi; \
	else \
		echo "Error: $(TEST_DIR)/$*.test not found"; \
		echo ""; \
		echo "Available tests in $(TEST_DIR):"; \
		ls $(TEST_DIR)/*.test 2>/dev/null | head -20 | sed 's|.*/||; s|\.test$$||' | column; \
		exit 1; \
	fi

# ============================================================================
# Cleanup
# ============================================================================

clean:
	rm -rf $(RESULT_DIR)
	$(CARGO) clean

clean-results:
	rm -rf $(RESULT_DIR)

# ============================================================================
# Reporting
# ============================================================================

test-report:
	@if [ ! -d "$(RESULT_DIR)" ]; then \
		echo "No test results found. Run 'make test' first."; \
		exit 1; \
	fi
	@echo "=== Test Results by File ==="
	@echo ""
	@for log in $(RESULT_DIR)/*.log; do \
		if [ -f "$$log" ]; then \
			name=$$(basename "$$log" .log); \
			pass=$$(grep -cE "^$${name}-.*\.\.\. Ok" "$$log" 2>/dev/null | tr -d '[:space:]'); \
			total=$$(grep -cE "^$${name}-[0-9]" "$$log" 2>/dev/null | tr -d '[:space:]'); \
			pass=$${pass:-0}; \
			total=$${total:-0}; \
			if [ "$$total" -gt 0 ] 2>/dev/null; then \
				pct=$$((pass * 100 / total)); \
				printf "%-12s %4d/%4d passed (%2d%%)\n" "$$name" "$$pass" "$$total" "$$pct"; \
			fi; \
		fi; \
	done | sort -t'(' -k2 -rn
	@echo ""
	@echo "=== Overall ==="
	@total_pass=0; total_tests=0; \
	for log in $(RESULT_DIR)/*.log; do \
		if [ -f "$$log" ]; then \
			name=$$(basename "$$log" .log); \
			pass=$$(grep -cE "^$${name}-.*\.\.\. Ok" "$$log" 2>/dev/null | tr -d '[:space:]'); \
			total=$$(grep -cE "^$${name}-[0-9]" "$$log" 2>/dev/null | tr -d '[:space:]'); \
			pass=$${pass:-0}; \
			total=$${total:-0}; \
			total_pass=$$((total_pass + pass)); \
			total_tests=$$((total_tests + total)); \
		fi; \
	done; \
	if [ "$$total_tests" -gt 0 ]; then \
		echo "Total: $$total_pass/$$total_tests passed ($$((total_pass * 100 / total_tests))%)"; \
	else \
		echo "No test data found."; \
	fi

pass-rates:
	@if [ ! -d "$(RESULT_DIR)" ]; then \
		echo "No test results found. Run 'make test' first."; \
		exit 1; \
	fi
	@total_pass=0; total_tests=0; \
	for t in $(SQLITE_TESTS); do \
		if [ -f "$(RESULT_DIR)/$$t.log" ]; then \
			pass=$$(grep -cE "^$$t-.*\.\.\. Ok$$" "$(RESULT_DIR)/$$t.log" 2>/dev/null | tr -d '[:space:]'); \
			total=$$(grep -cE "^$$t-[0-9].*\.\.\." "$(RESULT_DIR)/$$t.log" 2>/dev/null | tr -d '[:space:]'); \
			pass=$${pass:-0}; \
			total=$${total:-0}; \
			if [ "$$total" -gt 0 ] 2>/dev/null; then \
				pct=$$((pass * 100 / total)); \
				printf "%-15s %4d / %4d  (%3d%%)\n" "$$t" "$$pass" "$$total" "$$pct"; \
				total_pass=$$((total_pass + pass)); \
				total_tests=$$((total_tests + total)); \
			fi; \
		fi; \
	done; \
	echo ""; \
	echo "----------------------------------------"; \
	if [ "$$total_tests" -gt 0 ]; then \
		printf "%-15s %4d / %4d  (%3d%%)\n" "TOTAL" "$$total_pass" "$$total_tests" "$$((total_pass * 100 / total_tests))"; \
	fi
