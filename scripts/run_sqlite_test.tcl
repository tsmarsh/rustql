#!/usr/bin/env tclsh
# SQLite TCL Test Wrapper for RustQL
# Usage: tclsh run_sqlite_test.tcl <test_name> [timeout_seconds]
#
# Runs a single SQLite TCL test against RustQL

if {$argc < 1} {
    puts stderr "Usage: tclsh run_sqlite_test.tcl <test_name>"
    puts stderr "Example: tclsh run_sqlite_test.tcl select1"
    exit 1
}

set test_name [lindex $argv 0]

# Determine root directory - handle relative and absolute paths
set script_path [info script]
if {$script_path eq "" || ![file exists $script_path]} {
    # Script path not available, try to find root from cwd
    set root_dir [pwd]
    # Walk up until we find Cargo.toml
    while {![file exists [file join $root_dir Cargo.toml]] && $root_dir ne "/"} {
        set root_dir [file dirname $root_dir]
    }
} else {
    set script_dir [file dirname [file normalize $script_path]]
    set root_dir [file dirname $script_dir]
}

set test_dir [file join $root_dir sqlite3 test]
set test_file [file join $test_dir "${test_name}.test"]

if {![file exists $test_file]} {
    puts stderr "Error: Test file not found: $test_file"
    exit 1
}

# Create a unique temp directory for this test run to avoid database locking
# when multiple tests run in parallel
set pid [pid]
set timestamp [clock milliseconds]
set work_dir [file join "/tmp" "rustql-test-${test_name}-${pid}-${timestamp}"]
file mkdir $work_dir

# Create symlinks to all .tcl files from the test directory
# This gives each test its own working directory for test.db
foreach f [glob -nocomplain -directory $test_dir *.tcl] {
    set fname [file tail $f]
    file link -symbolic [file join $work_dir $fname] $f
}

# Also symlink test data directories that tests may need
foreach subdir {data testdata} {
    set src [file join $test_dir $subdir]
    if {[file exists $src]} {
        file link -symbolic [file join $work_dir $subdir] $src
    }
}

# Change to the unique work directory
cd $work_dir
set testdir $test_dir

# Load RustQL TCL extension
set lib_path [file join $root_dir target release librustql.so]
if {![file exists $lib_path]} {
    # Try debug build
    set lib_path [file join $root_dir target debug librustql.so]
}
if {![file exists $lib_path]} {
    puts stderr "Error: TCL extension not found"
    puts stderr "Build with: cargo build --release --features tcl"
    exit 1
}
load $lib_path Rustql

# Additional TCL stubs for test infrastructure
proc sqlite3_memdebug_malloc_count {} { return 0 }
proc fpnum_compare {a b} { expr {$a == $b} }
proc set_test_counter {args} { return 0 }
proc sqlite_register_test_function {db name} { return }
proc abuse_create_function {db} { return }

# Set up sqlite_options that tester.tcl expects
array set sqlite_options {
    default_autovacuum 0
    autovacuum 1
    compound 1
    trigger 1
    view 1
    subquery 1
    memorydb 1
    attach 1
    progress 1
    vacuum 1
    tempdb 1
    integrityck 1
    conflict 1
    schema 1
    foreignkey 1
    incrblob 1
    datetime 1
    pager_pragmas 1
    utf16 1
    tcl 1
    windowfunc 1
    json 1
    fts3 1
    fts5 1
    rtree 1
    builtin_test 0
    memdebug 0
    lock_proxy_pragmas 0
    wal 1
    lookaside 1
    long_double 0
    threadsafe 1
    shared_cache 1
    stat4 1
    mem5 0
    secure_delete 1
    cursorhint 1
    diskio 1
    explain 1
    bloblit 1
    casesensitivelike 0
    debug 0
    update_delete_limit 0
    hidden_columns 0
    check 1
    authorization 1
    columncount 1
    complete 1
    crashtest 0
    hexlit 1
    like 1
    or_opt 1
    reindex 1
    tclvar 1
    trace 1
    pragma 1
    floatingpoint 1
    icu 0
    deprecated 0
    direct_read 0
}

# SQLite compile-time limits
set ::SQLITE_MAX_FUNCTION_ARG 127

# Source the test infrastructure
# Set guard flag to prevent double reset_db when test file re-sources tester.tcl
set ::TESTER_SOURCED 1
puts "DEBUG: About to source tester.tcl, pwd=[pwd]"
if {[catch {source tester.tcl} err]} {
    puts stderr "Error loading tester.tcl: $err"
    puts stderr $::errorInfo
    exit 1
}
puts "DEBUG: tester.tcl sourced, db=[info commands db]"

# Run the test
puts "Running ${test_name}.test..."
puts "=========================================="
set start_time [clock seconds]

# Set argv0 to the test file so that [file dirname $argv0] in the test
# file returns the test directory (many SQLite tests use this pattern)
set argv0 $test_file
puts "DEBUG: argv0=$argv0, test_file=$test_file"
puts "DEBUG: testdir=$testdir"

if {[catch {source $test_file} err]} {
    puts stderr ""
    puts stderr "Error in ${test_name}.test: $err"
    puts stderr $::errorInfo
    set exit_code 1
} else {
    set exit_code 0
}

set end_time [clock seconds]
set elapsed [expr {$end_time - $start_time}]

puts ""
puts "=========================================="
puts "Test: $test_name"
puts "Time: ${elapsed}s"

# Print summary if available
if {[info exists ::nErr]} {
    puts "Errors: $::nErr"
    if {$::nErr > 0} {
        set exit_code 1
    }
}

if {$exit_code == 0} {
    puts "Status: PASSED"
} else {
    puts "Status: FAILED"
}

# Clean up temp directory
if {[info exists work_dir] && [file exists $work_dir]} {
    cd /tmp
    file delete -force $work_dir
}

exit $exit_code
