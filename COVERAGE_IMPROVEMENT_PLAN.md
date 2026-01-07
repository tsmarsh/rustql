# RustQL Coverage Improvement Plan

## Current Coverage Status

**Overall Coverage**: 38.1% (Needs significant improvement)
- **Total Lines**: 12,057
- **Covered Lines**: 4,590
- **Branch Coverage**: 0.0% (Not measured yet)

## Coverage Analysis by Module

### 📊 Module Coverage Breakdown

| Module | Line Coverage | Status |
|--------|---------------|--------|
| `src/schema` | 61.6% | ⚠️ Good |
| `src/vdbe` | 57.2% | ⚠️ Good |
| `src/parser` | 52.6% | ⚠️ Good |
| `src/api` | 49.6% | ❌ Needs Work |
| `src/executor` | 42.1% | ❌ Needs Work |
| `src/functions` | 36.3% | ❌ Needs Work |
| `src/os` | 27.6% | ❌ Needs Work |
| `src/storage` | 7.5% | ❌ Critical |
| `src/util` | 0.0% | ❌ Critical |

## 🎯 Priority Areas for Improvement

### 1. **Critical: Storage Layer (7.5% coverage)**
**Files needing immediate attention:**
- `src/storage/btree.rs` (0.0%)
- `src/storage/pager.rs` (11.2%)
- `src/storage/pcache.rs` (0.0%)
- `src/storage/wal.rs` (40.9%)

**Action Plan:**
- ✅ **B-tree Tests**: Implement comprehensive tests for B-tree operations
  - Tree creation, insertion, deletion
  - Cursor navigation and traversal
  - Page splitting and balancing
  - Transaction rollback scenarios

- ✅ **Pager Tests**: Add tests for page cache management
  - Page allocation and deallocation
  - LRU eviction policies
  - Journaling and crash recovery
  - Memory vs. disk pager scenarios

- ✅ **WAL Tests**: Enhance Write-Ahead Logging coverage
  - WAL frame creation and validation
  - Checkpoint operations
  - Concurrent read/write scenarios
  - WAL recovery testing

### 2. **High Priority: OS Layer (27.6% coverage)**
**Files needing attention:**
- `src/os/vfs.rs` (9.9%)
- `src/os/mutex.rs` (12.8%)
- `src/os/unix.rs` (37.7%)

**Action Plan:**
- ✅ **VFS Tests**: Add comprehensive VFS interface tests
  - File open/close operations
  - Locking scenarios (shared, exclusive)
  - Error handling for file operations
  - Cross-platform compatibility tests

- ✅ **Mutex Tests**: Implement concurrency testing
  - Mutex acquisition and release
  - Deadlock detection
  - Priority inheritance scenarios
  - Stress testing with multiple threads

### 3. **High Priority: Executor Module (42.1% coverage)**
**Files needing attention:**
- `src/executor/wherecode.rs` (13.9%)
- `src/executor/select.rs` (31.5%)
- `src/executor/prepare.rs` (43.0%)
- `src/executor/insert.rs` (48.4%)

**Action Plan:**
- ✅ **WHERE Clause Tests**: Add comprehensive WHERE testing
  - Index selection algorithms
  - Query plan generation
  - Cost estimation functions
  - Complex predicate handling

- ✅ **SELECT Tests**: Enhance SELECT operation coverage
  - Various join types (INNER, LEFT, CROSS)
  - Subquery handling
  - Window functions
  - DISTINCT and GROUP BY operations

- ✅ **DML Tests**: Improve INSERT/UPDATE/DELETE coverage
  - Conflict resolution scenarios
  - Trigger integration
  - Bulk operations
  - Transaction boundary testing

### 4. **Medium Priority: API Layer (49.6% coverage)**
**Files needing attention:**
- `src/api/connection.rs` (35.4%)
- `src/api/stmt.rs` (51.6%)
- `src/api/config.rs` (54.3%)

**Action Plan:**
- ✅ **Connection Tests**: Add edge case testing
  - Concurrent connection scenarios
  - Error recovery paths
  - URI parsing edge cases
  - Memory database testing

- ✅ **Statement Tests**: Enhance prepared statement coverage
  - Parameter binding edge cases
  - Statement caching scenarios
  - Large result set handling
  - Statement finalization testing

### 5. **Medium Priority: Functions Module (36.3% coverage)**
**Files needing attention:**
- `src/functions/scalar.rs` (31.8%)
- `src/functions/aggregate.rs` (52.9%)

**Action Plan:**
- ✅ **Scalar Function Tests**: Add comprehensive function testing
  - Edge cases for mathematical functions
  - String manipulation functions
  - Date/time function testing
  - Type coercion scenarios

- ✅ **Aggregate Function Tests**: Enhance aggregate coverage
  - GROUP BY scenarios
  - Window function integration
  - NULL handling in aggregates
  - Custom aggregate functions

## 📅 Implementation Roadmap

### Phase 1: Critical Storage Layer (Week 1-2)
- **Goal**: Achieve 70%+ coverage for storage modules
- **Focus**: B-tree, pager, WAL core functionality
- **Success Criteria**: All storage components have basic test coverage

### Phase 2: OS and Executor Layers (Week 3-4)
- **Goal**: Achieve 60%+ coverage for OS and executor modules
- **Focus**: VFS interface, mutex testing, query execution
- **Success Criteria**: Core execution paths are well-tested

### Phase 3: API and Functions (Week 5-6)
- **Goal**: Achieve 70%+ coverage for API and functions
- **Focus**: Edge cases, error handling, function testing
- **Success Criteria**: Public API is robustly tested

### Phase 4: Integration and Regression (Week 7-8)
- **Goal**: Achieve 80%+ overall coverage
- **Focus**: Integration testing, regression test suite
- **Success Criteria**: Comprehensive test suite with good coverage

## 🔧 Testing Strategy Recommendations

### 1. **Property-Based Testing**
Implement property-based tests using `proptest` or `quickcheck` for:
- B-tree invariants
- Transactional properties
- Query result correctness
- Error handling consistency

### 2. **Fuzz Testing**
Add fuzz testing for:
- SQL parser edge cases
- Malformed input handling
- Memory safety scenarios
- Concurrency race conditions

### 3. **Integration Testing**
Develop integration tests that:
- Test complete query execution pipelines
- Validate cross-module interactions
- Test real-world usage patterns
- Include performance benchmarks

### 4. **Error Injection Testing**
Implement fault injection for:
- Disk I/O failures
- Memory allocation failures
- Network interruptions (for remote VFS)
- Corrupt database recovery

## 📊 Target Coverage Metrics

| Module | Current | Target | Status |
|--------|---------|--------|--------|
| Overall | 38.1% | 80%+ | ❌ Needs Work |
| Storage | 7.5% | 70%+ | ❌ Critical |
| OS Layer | 27.6% | 60%+ | ❌ High Priority |
| Executor | 42.1% | 70%+ | ⚠️ Medium Priority |
| API | 49.6% | 80%+ | ⚠️ Medium Priority |
| Functions | 36.3% | 70%+ | ❌ High Priority |

## 🎯 Success Criteria

**Short-term (4 weeks):**
- Achieve 60% overall coverage
- All critical modules have basic test coverage
- No modules with 0% coverage

**Medium-term (8 weeks):**
- Achieve 75% overall coverage
- All core functionality thoroughly tested
- Good error path coverage

**Long-term (12 weeks):**
- Achieve 85%+ overall coverage
- Comprehensive test suite
- Property-based and fuzz testing implemented
- Continuous integration with coverage gates

## 🔄 Continuous Integration

Add coverage checking to CI pipeline:
```yaml
- name: Check coverage
  run: |
    cargo tarpaulin --out Xml
    # Fail if coverage drops below threshold
    python3 check_coverage.py --min-overall 40 --min-storage 10
```

## 📚 Resources

- **Tarpaulin Docs**: https://github.com/xd009642/tarpaulin
- **Rust Testing Book**: https://doc.rust-lang.org/book/ch11-00-testing.html
- **Property Testing**: https://altsysrq.github.io/proptest-book/intro.html
- **Fuzz Testing**: https://rust-fuzz.github.io/book/

## 📝 Next Steps

1. **Immediate**: Start with storage layer tests (highest priority)
2. **Parallel**: Begin OS layer testing (VFS and mutex)
3. **Document**: Add test coverage comments to identify gaps
4. **Automate**: Set up CI coverage reporting
5. **Monitor**: Track coverage trends over time

**Let's get testing!** 🚀