---
name: run-tests
description: Build and run OpenIVM test suite (or a specific test file).
---

## Instructions

1. Build if needed: `GEN=ninja make 2>&1 | tail -5`
2. Run all tests: `build/release/test/unittest "test/sql/ivm_*" 2>&1`
   - Or a specific test: `build/release/test/unittest "test/sql/<test_name>.test" 2>&1`
3. Report: number of assertions passed/failed
4. If any fail, show the failing test name and expected vs actual values
5. If a test crashes (segfault, abort), investigate the crash — never skip or remove the test
