#!/usr/bin/env tclsh
# Test runner for RustQL TCL extension
# Usage: tclsh run_tcl_test.tcl <test_file.test>

# Load the RustQL extension
set script_dir [file dirname [info script]]
set lib_path [file join $script_dir .. target debug librustql.so]

# Try release build first (more common), then debug
set lib_path [file join $script_dir .. target release librustql.so]
if {![file exists $lib_path]} {
    set lib_path [file join $script_dir .. target debug librustql.so]
}
if {[file exists $lib_path]} {
    load $lib_path rustql
} else {
    puts stderr "Error: Cannot find librustql.so"
    puts stderr "Build with: cargo build --release --features tcl"
    exit 1
}

# Minimal tester.tcl implementation for running basic tests
set ::ntest 0
set ::npass 0
set ::nfail 0
set ::skip_list {}

proc execsql {sql {db db}} {
    return [$db eval $sql]
}

proc catchsql {sql {db db}} {
    set rc [catch {$db eval $sql} msg]
    return [list $rc $msg]
}

proc do_test {name script expected} {
    incr ::ntest
    set result [uplevel 1 $script]
    if {$result eq $expected} {
        incr ::npass
        puts "ok $name"
    } else {
        incr ::nfail
        puts "FAILED $name"
        puts "  Expected: $expected"
        puts "  Got:      $result"
    }
}

proc do_execsql_test {name sql expected} {
    do_test $name [list execsql $sql] $expected
}

proc do_catchsql_test {name sql expected} {
    do_test $name [list catchsql $sql] $expected
}

proc finish_test {} {
    puts ""
    puts "Results: $::ntest tests, $::npass passed, $::nfail failed"
    if {$::nfail > 0} {
        exit 1
    }
    exit 0
}

proc ifcapable {expr code} {
    # For now, assume all capabilities are present
    uplevel 1 $code
}

proc capable {expr} {
    return 1
}

proc forcedelete {args} {
    foreach f $args {
        catch {file delete -force $f}
    }
}

proc reset_db {} {
    catch {db close}
    forcedelete test.db test.db-journal test.db-wal
    sqlite3 db :memory:
}

# Set up test directory
set testdir [file dirname $argv0]
if {$testdir eq "."} {
    set testdir [pwd]
}

# Create default database
sqlite3 db :memory:

# Run test file if specified
if {$argc > 0} {
    set testfile [lindex $argv 0]
    if {[file exists $testfile]} {
        source $testfile
    } else {
        puts stderr "Error: Test file not found: $testfile"
        exit 1
    }
} else {
    puts "RustQL TCL Extension Test Runner"
    puts "Usage: tclsh run_tcl_test.tcl <test_file.test>"
}
