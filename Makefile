# RustQL Makefile for TCL Test Suite
#
# Usage:
#   make test                    - Run all TCL tests
#   make test-select1            - Run select1.test
#   make test-basic              - Run basic RustQL tests (quick)
#   make clean                   - Remove build artifacts and test results
#   make tcl-extension           - Build only the TCL extension
#
# Test results are stored in test-results/

CARGO := cargo
TCLSH := tclsh
TIMEOUT := 120

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

.PHONY: all test test-basic test-all clean clean-results tcl-extension rustql help list-tests

all: tcl-extension

help:
	@echo "RustQL TCL Test Suite"
	@echo ""
	@echo "Build Targets:"
	@echo "  make tcl-extension     - Build the TCL extension (librustql.so)"
	@echo "  make rustql            - Build the rustql binary"
	@echo ""
	@echo "Test Targets:"
	@echo "  make test              - Run common SQLite TCL tests"
	@echo "  make test-basic        - Run basic RustQL TCL tests (quick smoke test)"
	@echo "  make test-<name>       - Run a specific test (e.g., make test-select1)"
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
	$(CARGO) build --release --features tcl

# Build the rustql binary
rustql: $(RUSTQL_BIN)

$(RUSTQL_BIN): $(RUST_SOURCES) Cargo.toml
	$(CARGO) build --release

# Create result directory
$(RESULT_DIR):
	@mkdir -p $(RESULT_DIR)

# Run all configured tests
test: $(TCL_EXT) $(RESULT_DIR)
	@echo "Running SQLite TCL test suite..."
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

# Run basic RustQL tests (quick smoke test)
test-basic: $(TCL_EXT)
	@echo "Running basic RustQL TCL tests..."
	@$(TCLSH) tests/run_tcl_test.tcl tests/basic_tcl.test

# Pattern rule for individual tests
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

# Clean up
clean:
	rm -rf $(RESULT_DIR)
	$(CARGO) clean

# Clean only test results (keep build)
clean-results:
	rm -rf $(RESULT_DIR)
