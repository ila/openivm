# OpenIVM codebase audit

Date: 2026-04-30

This audit focuses on code health, correctness risk, test quality, stale comments, and LLM-looking artifacts in `src/`,
`test/sql/`, and `docs/`. It intentionally does not refactor code yet; the goal is to make the next cleanup and
algorithm work explicit and testable.

## Executive assessment

OpenIVM is not throwaway code. It has a substantial sqllogictest suite and several correctness fixes that show real
understanding of IVM edge cases: NULL-safe `EXCEPT ALL` checks, outer-join transition tests, timestamp cursor handling,
crash-recovery flags, DuckLake snapshot tests, and aggregate decomposition tests.

The code is also visibly fragile. A lot of behavior is concentrated in a few very large functions, many comments encode
historical patch context, and the refresh path relies heavily on generated SQL strings. The highest risk is silent MV
drift in cases where the classifier, plan rewrite, metadata, and SQL compiler disagree. The next phase should harden
tests first, then split and simplify the orchestration code behind those tests.

## Major findings

### 1. MAJOR: MV creation and refresh are over-centralized

Locations:
- `src/core/parser.cpp:443`
- `src/upsert/refresh.cpp:486`

`IVMPlanFunction` handles catalog routing, PAC forwarding, parsing, planning, multiple plan rewrites, compatibility
classification, LPTS fallback, table DDL, metadata DML, top-k handling, DuckLake special cases, and output SQL file
generation in one function. `GenerateRefreshSQL` similarly owns crash recovery, adaptive cost decisions, strategy
dispatch, downstream refresh semantics, companion delta rows, timestamp advancement, DuckLake snapshot updates, cleanup,
and SQL file output.

Why it matters:
- Invariants are spread across comments and branch ordering instead of types or narrow interfaces.
- Small algorithm changes risk altering catalog routing, metadata updates, or fallback behavior.
- It is hard to verify that every `IVMType` follows the same lifecycle: decide strategy, build data SQL, update metadata,
clean deltas, update downstream delta views.

Fix direction:
- Split MV creation into distinct internal steps: session/catalog context, query planning, plan normalization,
classification, data-table/view DDL, metadata DML, and source-delta setup.
- Split refresh generation into: metadata load, strategy selection, delta-query compile, data-table apply,
downstream-delta handling, cursor/snapshot advance, cleanup.
- Add small immutable structs for classification output and refresh context instead of passing parallel strings/vectors.

### 2. MAJOR: The plan rewrite pipeline walks and mutates the same plan too many times

Location: `src/core/plan_rewrite.cpp:1134`

`IVMPlanRewrite` runs many recursive passes in sequence: CTE inlining, aggregate FILTER rewrite, DISTINCT rewrite,
derived aggregate rewrite, hidden count injection, hidden-column propagation, left-join key rewrite, match-count rewrite.
Some of these passes independently traverse the full tree and depend on prior pass ordering.

Why it matters:
- Repeated walks are not the main performance bottleneck, but they make rewrite ordering fragile.
- Hidden-column propagation and aggregate decomposition are especially easy to break because later passes infer meaning
from column names such as `_ivm_sum_*`, `_ivm_count_*`, and `_ivm_match_count`.
- The codebase has several comments explaining why a pass exists, which is a sign the invariant is not encoded locally.

Fix direction:
- Introduce a single rewrite context that records discovered aggregate/group/join metadata during traversal.
- Keep transformations separate, but centralize traversal utilities and make pass preconditions explicit.
- Add snapshot tests around rewritten plan classifications and stored metadata for AVG/STDDEV, HAVING, DISTINCT,
LEFT/FULL OUTER JOIN, and top-k.

### 3. MAJOR: Delta capture for DML is still string-serialization based in important paths

Location: `src/rules/refresh_insert_rule.cpp:315`

The insert/delete/update rule builds SQL strings to write delta rows. Simple constant inserts are handled directly, but
`INSERT ... SELECT`, complex DELETE, CSV import, and UPDATE paths depend on `LogicalPlanToAst`, `Expression::ToString`,
or table-filter serialization. Unsupported cases throw at DML time, for example `DELETE ... WHERE dept IN (SELECT ...)`
is still expected to error in `test/sql/insert_rule.test:969`.

Why it matters:
- Base-table DML behavior is part of user-facing correctness. If delta capture misses or mis-serializes a predicate, the
MV can drift even if refresh SQL is correct.
- Unsupported DML against an IVM-tracked table is a sharp edge: users can create an MV successfully and later hit DML
limitations unrelated to the MV query.
- `Expression::ToString()` is not a durable SQL serialization contract.

Fix direction:
- Prefer plan-native delta capture for UPDATE/DELETE where possible, or explicitly narrow supported DML and document it.
- Add regression tests for every accepted DML shape with bidirectional MV-vs-base `EXCEPT ALL`.
- Replace tests that only check delta-row counts with tests that run refresh and compare MV contents.

### 4. MAJOR: SQL assembly and metadata encoding are duplicated and under-typed

Locations:
- `src/upsert/refresh_compiler.cpp:127`
- `src/core/refresh_metadata.cpp:158`
- `src/core/parser.cpp:1986`

Refresh SQL, DDL, metadata values, group columns, aggregate type lists, DuckLake snapshot updates, and cleanup queries are
mostly built with string concatenation. Some metadata is comma-separated text (`group_columns`, `aggregate_types`) and
re-parsed later.

Why it matters:
- Comma-separated metadata is brittle for quoted identifiers and future expression-derived names.
- Reconstructing SQL prefixes and quoted names in multiple modules increases cross-catalog bugs.
- SQL injection is not the primary threat here, but malformed internal SQL from odd identifiers is a real reliability
risk.

Fix direction:
- Introduce small helpers for qualified names, metadata string/list encoding, and refresh SQL fragments.
- Store structured metadata where practical, or at least use a single escaping/encoding helper for list-valued metadata.
- Add identifier tests with spaces, mixed case, keywords, commas, and quoted names.

### 5. MAJOR: Outer-join aggregate behavior is complicated and still documents known incorrect cases

Location: `src/upsert/refresh_compiler.cpp:550`

The LEFT JOIN MERGE path contains detailed logic around `_ivm_match_count`, including a comment that left-side non-count
aggregates are "incorrect for those but rare in practice" and another known limitation for folded projections around
NULL-padded transitions.

Why it matters:
- This is not just style debt; it describes possible wrong results.
- The default settings enable `ivm_left_join_merge` and `ivm_full_outer_merge` in `src/openivm_extension.cpp:142`, so
MERGE paths are not merely experimental unless the classifier forces group-recompute.
- Some computed aggregate cases are forced to group-recompute, but the guarantee is spread across parser classification,
metadata flags, and compiler checks.

Fix direction:
- Turn each known incorrect comment into either a failing regression test or a documented unsupported shape that
classifier routes to group-recompute.
- For LEFT/FULL OUTER aggregate views, prefer correctness-first group-recompute unless the MERGE path can prove the
aggregate is right-side-only or otherwise safe.
- Add explicit metadata for aggregate input side instead of inferring from names and hidden columns.

### 6. MAJOR: View matching is compiled in but mostly a scaffold

Locations:
- `src/match/plan_canonical.cpp:11`
- `src/match/predicate_oracle.cpp:18`
- `src/match/equivalence_classes.cpp:8`
- `src/match/constraint_cache.cpp:12`
- `src/upsert/refresh_cost_model.cpp:752`

The match subsystem exposes settings and system tables, but core components return empty/default results or
`UNDECIDED`. This is gated by `ivm_enable_view_matching=false`, but the code looks production-adjacent.

Why it matters:
- Future work can accidentally rely on a subsystem that appears wired but has no useful semantics.
- Stubs add noise to audits and make production readiness harder to judge.

Fix direction:
- Either isolate this behind an explicit experimental namespace/build flag, or add a clear design document and tests for
the first useful tier.
- Remove TODO-only implementation files from normal mental load until they have behavior.

### 7. MAJOR: Tests are broad but uneven; some files do not verify MV equality

Evidence:
- Strong files: `aggregate.test`, `full_outer_join.test`, `projection.test`, `left_join.test`,
  `distinct.test`, and DuckLake aggregate/projection tests contain many bidirectional `EXCEPT ALL` checks.
- Weak files: `list.test` has one MV and no `EXCEPT ALL`; `refresh_hooks.test` and `metadata.test` mostly validate
  hooks/metadata; `parser.test` has many MVs but only a few equality checks.
- `insert_rule.test` has many delta-capture tests, but some sections check delta-row counts rather than refreshing
  an MV and comparing against base tables.

Why it matters:
- The most likely failure mode is not a crash; it is MV/base-table drift after a specific DML sequence.
- Tests that assert row counts in delta tables can pass while the MV is wrong.

Fix direction:
- Establish a test rule: every behavioral MV test should check both directions:
  `SELECT * FROM mv EXCEPT ALL <base query>` and `<base query> EXCEPT ALL SELECT * FROM mv`.
- Allow explicit exceptions for metadata/parser/hook-only tests, but mark them as such.
- Add a small reusable test idiom or generator for equality checks so coverage is easy to scan.

## Minor findings and cleanup targets

### Stale or contradictory docs/comments

- `docs/limitations.md:19` lists `ARG_MIN` and `ARG_MAX` as unsupported, but `src/core/incremental_checker.cpp:20` supports them
  and `test/sql/argminmax.test` exercises them.
- `docs/limitations.md:50` says projection top-k is "genuinely incremental", while `docs/operators/top-k.md:38` says it
  is full recompute on refresh.
- `src/upsert/refresh.cpp:1248` says `TOP_K` is unreachable, but top-k tests and docs indicate it is now a real
  classification.
- `docs/limitations.md:51` still references "see plan section 5 in repo"; `src/core/parser.cpp:1343` references
  "plan section 7". These should become issue links, docs links, or be removed.

### Comments are often patch history instead of invariants

Several comments explain specific benchmark query IDs, old failures, or why a previous patch was wrong. These are useful
while debugging but noisy as permanent code comments. Keep comments that state algebraic invariants, concurrency
invariants, and unsupported-shape rationale. Move historical notes into regression-test names or commit messages.

### Cost model is useful but should not drive correctness

Location: `src/upsert/refresh_cost_model.cpp:356`

The cost model is a heuristic with learned regression fallback. It is gated by `ivm_adaptive_refresh`, which is good.
However, the comments and estimates should be treated as operational tuning only. It should never compensate for
classifier uncertainty. If a shape is correctness-risky, route it to a correctness-preserving strategy first, then cost it.

### Lock helpers need RAII coverage

Location: `src/core/refresh_locks.cpp:27`

The lock map is simple and understandable, but direct `LockView`/`UnlockView` calls are exception-sensitive. Delta writes
use `DeltaLockGuard`; view locks should get the same RAII treatment everywhere cleanup or cascade code can throw.

## Recommended execution order

1. Test hardening:
   - Add bidirectional equality checks to `list.test`.
   - Audit `insert_rule.test` sections that only count delta rows and add refresh-plus-equality checks.
   - Add explicit regression tests for the LEFT/FULL OUTER aggregate shapes currently described as limitations.

2. Documentation cleanup:
   - Fix `ARG_MIN`/`ARG_MAX`, projection top-k, `TOP_K`, and stale plan-section references.
   - Convert historical comments into concise invariants or regression-test names.

3. Behavior-preserving refactor:
   - Split `IVMPlanFunction` and `GenerateRefreshSQL` into smaller helpers with typed context structs.
   - Centralize name quoting, metadata list encoding, and SQL fragment assembly.
   - Consolidate plan traversal utilities before changing incrementalization algorithms.

4. Correctness-first algorithm work:
   - Make outer-join aggregate MERGE opt-in per proven-safe aggregate shape, or route to group-recompute by default.
   - Replace string-based DML delta capture where possible.
   - Revisit DISTINCT auxiliary state and top-k with tests that prove MV/base equivalence under duplicate, delete, NULL,
     empty-delta, and chained-view cases.

## Bottom line

The codebase is functional but too easy to break. The tests show serious effort, yet the architecture still has too many
places where correctness depends on scattered comments and branch ordering. Before improving the incrementalization
algorithms, the most valuable work is to harden equality tests, remove stale documentation, and split the creation/refresh
orchestrators so algorithm changes have a smaller blast radius.
