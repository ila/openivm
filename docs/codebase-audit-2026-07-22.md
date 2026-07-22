# OpenIVM codebase audit and remediation plan

Date: 2026-07-22

This audit covers the current working tree, including the in-progress nonlocal refresh changes. Findings were checked
from correctness, reliability, performance, simplicity, and code-hygiene perspectives. Confirmed bugs were reproduced
with real materialized views and refreshes rather than inferred only from static analysis.

The remediation rule is correctness first: an incrementalizable query must retain a correct incremental or affected-domain
maintenance path. A bug must not be hidden by weakening a test or silently changing the view to `FULL_REFRESH`.

## P0: correctness and transaction safety

- [x] **Capture delta rows in the base DML transaction.** Delta capture is now a streaming logical/physical operator that
  appends through the caller's `ClientContext`, so base DML and delta rows share one commit or rollback. A resolved
  catalog-level lock prevents refresh from advancing its watermark past an uncommitted delta. Regression coverage includes
  rollback, constraint failure, defaults, generated columns, `RETURNING`, and an uncommitted DML/refresh race.
- [ ] **Capture actual `ON CONFLICT` outcomes.** `INSERT ... ON CONFLICT DO UPDATE` and `INSERT OR REPLACE` do not emit the
  required delete-old/insert-new delta pair. Derive deltas from rows actually inserted or updated, not merely the input to
  `LogicalInsert`.
- [ ] **Make MV lifecycle and native refresh transactional.** CREATE, REPLACE, DROP, ALTER, and `PRAGMA refresh` currently
  execute material work through helper autocommit connections. Caller rollback cannot restore a consistent collection of
  user view, backing table, metadata, and deltas; failed replacement can destroy the old MV. Use the caller transaction for
  native-catalog work and stage cross-catalog replacement before publication.
- [ ] **Continue cascade traversal when the current node has no source deltas.** The root empty-delta fast path returns
  before visiting downstream nodes. A child can have independent source deltas or pending parent-delta rows after an earlier
  failed cascade. Model `node skipped` separately from `graph traversal complete` and checkpoint each node independently.
- [ ] **Use NULL-safe affected-window partition matching.** `IN` does not select a NULL partition even though SQL window
  partitioning groups NULL values. Join the affected-key relation with `IS NOT DISTINCT FROM` for every partition key.
- [ ] **Preserve `SUM` NULL semantics.** Weighted SUM alone cannot distinguish numeric zero from no non-NULL inputs. Persist
  a hidden `COUNT(sum_argument)` and render NULL when that count reaches zero.
- [ ] **Use an unconditional row count for group existence.** `COUNT(nullable_expression)=0` does not mean a group is empty.
  Persist a hidden `COUNT(*)` solely for deciding whether the group row must be deleted.
- [ ] **Delete the heuristic group-measure update fast path.** `refresh_group_measure.cpp` infers aggregate lineage from SQL
  text, output-alias substrings, LPTS formatting, and value strings. A reproduced update to input `v` changed unrelated
  aliases `revenue` and `savings`. Use the existing affected-group recompute path until bound aggregate lineage can prove a
  safe fast path.
- [ ] **Validate the persisted `RefreshType` ABI.** Unknown values are raw-cast and can pass through refresh without applying
  data changes while still advancing the watermark. Give every persisted value an explicit ordinal and decode it through a
  single validating migration function; every dispatch must reject unknown values.
- [ ] **Give one refresh-node API ownership of hooks.** Scheduled hooks currently run twice, empty-delta skipping can suppress
  replace hooks, cascaded nodes bypass hooks, and hook errors are swallowed. Hooks, skip decisions, refresh, and error
  propagation must have exactly-once semantics.
- [ ] **Make crash recovery a durable state transition.** Some full-recompute paths bypass the in-progress journal, while
  recovery clears the journal before recovery succeeds. Persist an attempt/checkpoint before every data mutation and clear it
  only with successful post-refresh metadata.
- [ ] **Do not consume DuckLake structural-change identity before refresh succeeds.** Source-identity probing currently
  persists a new table ID immediately. Carry it as pending activity and commit it with the successful snapshot/watermark.

## P1: fail-closed metadata and authoritative planning

- [ ] **Distinguish metadata error, absence, and empty state.** Several getters convert query errors into `{}`, `false`, or a
  fallback strategy. Refresh must fail closed on schema/query errors; `optional` should mean genuinely absent metadata only.
- [ ] **Reject missing strategy contracts instead of falling through.** Missing DISTINCT or semi/anti metadata currently
  falls into unrelated compiler cases. Validate the selected strategy and all required metadata once before compilation.
- [ ] **Remove runtime type guessing from window lineage.** Bare-name `information_schema` lookup with `LIMIT 1` is ambiguous
  across schemas/catalogs, and coercing both comparison sides to VARCHAR changes bound comparison semantics. Persist exact
  bound cast chains and source identity at CREATE time.
- [ ] **Make source identity fully qualified.** DML classification, metadata lookup, locking, cleanup, and dependency handling
  rely too heavily on bare table or view names. Use catalog, schema, and stable object identity.
- [ ] **Centralize complete per-view DROP cleanup.** Hooks, refresh history/profile rows, dependency rows, and matcher state can
  survive DROP and be inherited by a newly created MV of the same name.
- [ ] **Preserve quoted output identifiers.** Planner output names are sanitized for internal use and then reused where the
  physical schema requires the original SQL identifier. Quoted names containing punctuation can fail during MV creation.
- [ ] **Classify only after required lineage is valid.** Window lineage can demote a finalized model to `FULL_REFRESH` after
  node maintenance and update semantics have already been derived, leaving contradictory model state.
- [ ] **Finish or reverse the `CURRENT_DIFF_RECOMPUTE` removal coherently.** Deterministic SAMPLE, POSITIONAL, and ASOF shapes
  are demoted to unconditional full refresh while stale nonlocal strategies, documentation, and benchmark paths remain.
  Preserve a typed affected/current-diff recompute path where correctness permits it; delete truly dead strategies.

## P2: remove SQL-text hacks and duplicate abstractions

- [ ] **Render Spark SQL by dialect instead of global regex replacement.** Whole-program replacement mutates literals,
  comments, and quoted identifiers, while early full-refresh paths bypass conversion entirely. Render casts, timestamps, and
  NULL-safe equality through dialect-aware builders/LPTS.
- [ ] **Replace affected-group source substitution with plan-node substitution.** Plain occurrence replacement can modify
  literals, comments, CTE references, and longer identifiers. Replace the exact `LogicalGet` occurrence before serialization.
- [ ] **Delete the workload-specific TPC-DI left-join shortcut.** General refresh code hardcodes `fact_market_history`, three
  source names, and `sk_company_id`. Retain the generic correct path unless typed lineage proves a generic optimization.
- [ ] **Replace `parser_sql_extractors.cpp` with bound-plan extraction.** Its approximately 1,250 lines duplicate tokenization,
  quote handling, clause parsing, alias rewriting, and parenthesis tracking for DISTINCT, filtered aggregates, and semi/anti
  metadata already represented by bound logical operators and expressions.
- [ ] **Delete the independent refresh SQL mini-parsers.** Aggregate-filter stripping, SCD2 predicate injection, running-window
  parsing, and LPTS CTE parsing belong in logical-plan/AST construction, not four inconsistent scanners of emitted SQL.
- [ ] **Make the delta IR authoritative or remove its diagnostic-only graph.** Per-node maintenance, update semantics, generic
  affected domains, lineage facts, and auxiliary-state annotations are populated and logged but do not select compilation.
  Keeping them beside parallel `RefreshType`/metadata dispatch creates drift.
- [ ] **Remove the nonfunctional view-matching product surface until it has behavior.** Candidate lookup, canonicalization,
  and predicate implication are stubs, but settings, metadata columns, logs, CMake targets, and estimator APIs remain public.
  Keep the FK constraint functionality that is actually used.
- [ ] **Replace handwritten JSON and pipe-delimited lineage encoding.** Important semantic metadata is manually scanned and
  malformed fields can be skipped while keeping an incomplete lineage arm. Use typed normalized rows or a versioned structured
  serializer and reject malformed records as a whole.
- [ ] **Split CREATE and refresh into typed phase programs.** `PlanFunction` and `GenerateRefreshSQL` each span roughly a
  thousand lines. Use immutable inputs and results such as `ViewDefinition`, `RefreshMetadataSnapshot`, `PendingDeltaBatch`,
  `RefreshStrategy`, and `RefreshProgram`; do not introduce a class hierarchy.
- [ ] **Replace boolean/argument soup with strategy input/result structs.** Compiler functions with 17 parameters, output
  booleans, and implicit reclassification hide invariants and make call-order mistakes likely.
- [ ] **Use tagged CREATE operations instead of magic strings.** Cleanup, profiling, schema derivation, and executable SQL are
  currently mixed in one string vector and reparsed by prefixes and tab-delimited fields.
- [ ] **Consolidate duplicated profiling and NULL-safe predicate helpers.** CREATE and refresh profiling independently implement
  IDs, retention, step collection, and persistence; SQL utilities contain parallel NULL-safe builders.

## P3: refresh performance and repeated work

- [ ] **Remove exact base-table counting from normal join compilation.** Standard join refresh performs `COUNT(*)` for every
  base table on every refresh after delta activity has already been computed. Use activity/statistics or remove the heuristic.
- [ ] **Avoid repeated parse/bind/plan/model reconstruction.** Refresh planning currently reparses and optimizes the stored
  query multiple times. Persist a versioned create-time typed model and rebuild only for migration or invalidation.
- [ ] **Load metadata once.** `GenerateRefreshSQL` issues many independent queries for one view and sometimes reloads the same
  field. Load one immutable snapshot; keep writes and recovery checkpoints separate.
- [ ] **Consolidate projection deletes once.** The projection path builds the same grouped/ranked net delta independently for
  DELETE and INSERT. Materialize or share the consolidated batch.
- [ ] **Carry delta activity through the full refresh.** Activity checks, join-term selection, max timestamp lookup, and cleanup
  repeatedly scan the same delta tables. Capture count/delete/watermark information once per transaction.
- [ ] **Price actual join work in the adaptive model.** The model counts exponential terms but underprices copied plan nodes,
  base-leaf appearances, compilation work, and generated SQL size.
- [ ] **Remove the fake `ConstraintCache` or make it a cache.** It repopulates a map that no read path consumes, while FK cost
  estimation separately parses textual constraint descriptions.

## Target refresh flow

At CREATE time, bind once and persist a versioned typed view definition, delta plan, output schema, exact source identities,
and concrete strategy metadata.

At refresh time:

1. Acquire the graph/node transaction and locks.
2. Load one immutable metadata snapshot.
3. Capture one pending delta batch and its watermark.
4. Choose one explicit correctness-preserving strategy; use cost only between valid strategies.
5. Compile a structured refresh program containing data, cascade, cleanup, and checkpoint phases.
6. Execute native work atomically; render SQL once only at a genuine external-dialect boundary.
7. Commit data, cursor/snapshot, source identity, and recovery state together.
