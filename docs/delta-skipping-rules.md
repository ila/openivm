# Delta Skipping Rules

`openivm_skip_empty_deltas` should gate all optimizations that prove a source delta
cannot affect a refresh, or that a smaller equivalent delta can be used.

Each rule lists its implementation status. See also
[Empty delta skip](optimizations/empty-delta-skip.md) and
[FK-aware pruning](optimizations/fk-aware-pruning.md).

## 1. DuckLake Table No-Op Skip

Problem: DuckLake snapshot ids are catalog-wide. A source table can have
`last_snapshot_id != current_snapshot_id` even when that table has no row changes.

Rule: before building a DuckLake join term for table `T`, check
`ducklake_table_insertions(catalog, schema, T, old, current)` and
`ducklake_table_deletions(catalog, schema, T, old, current)`. If both are empty,
skip the term for `T`.

Correctness: the delta relation for `T` is the zero Z-set, so every join term that
uses `delta(T)` is zero.

Status: implemented. OpenIVM first checks the `ducklake_snapshots()` change manifest for
`T`'s table id and falls back to counting `ducklake_table_insertions`/`ducklake_table_deletions`.
Unchanged tables skip their N-term join term, and when no source changed the refresh is skipped
and the stored snapshot ids are advanced.

## 2. Key-Domain Join Skip

Problem: a non-empty source delta may still have no matching join keys in the
other side.

Rule: for an equi-join `R.k = S.k`, skip the `delta(R)` term when
`SELECT DISTINCT k FROM delta(R)` has no intersection with `SELECT DISTINCT k FROM S`
under the relevant snapshot.

Correctness: the join term is empty if the delta key domain is disjoint from the
opposite input key domain.

Status: implemented for native inclusion-exclusion inner joins without LEFT JOINs. Each
equi-join key of a term's delta leaf is probed with an `EXISTS` query against the other
side's base table, or its delta when both sides are delta leaves in the term. Probing runs only
when exactly one input changed or every changed delta is tiny (at most max(8 rows, 5% of the
source table)).

## 3. Predicate-Disjoint Delta Skip

Problem: delta rows may fail filters or range predicates before they reach the
expensive part of the plan.

Rule: push view predicates and join range predicates into the delta probe. If the
filtered delta is empty, skip that delta term.

Correctness: selection is linear over Z-sets: `sigma_p(delta(R)) = 0` means every
term using that filtered delta is zero.

Status: partial. Scan filters pushed into a source are copied onto its delta scan and applied
inside the key-domain probes of rule 2. There is no separate filtered-emptiness probe; the
empty-term check counts unfiltered delta rows.

## 4. Unused-Right LEFT JOIN Rewrite

Problem: for `A LEFT JOIN B ON ...` where `B` contributes no visible output
columns, OpenIVM currently adds `openivm_left_key` and recomputes all output rows for
affected left keys. In TPC-DI `fact_market_history`, that key is `sk_company_id`,
which is too coarse.

Rule: when right-side columns are unused above the join, replace the right side by
a grouped multiplicity factor keyed by the join keys. If the right side is key
unique, the join is removable for multiplicity; otherwise the factor is
`GREATEST(COUNT(*), 1)` for LEFT JOIN semantics.

Correctness: this is valid only when the right side affects output exclusively
through bag multiplicity and join existence, not through projected values or
predicates above the join.

Status: not implemented. The workload-specific `fact_market_history` shortcut was removed.

## 5. Functional-Dependency / Uniqueness Skip

Problem: joins to key-unique dimensions can be irrelevant when no dimension
columns are projected and the join is guaranteed by constraints.

Rule: if constraints prove every left row has exactly one matching right row and
right columns are unused, remove or skip the right-side delta for that join.

Correctness: the join is multiplicity-preserving under the constraint, so changes
to non-output right columns cannot change the view.

Status: not implemented in refresh. The related FK rule prunes inclusion-exclusion terms
for insert-only PK-side deltas ([FK-aware pruning](optimizations/fk-aware-pruning.md)).

## 6. Append-Only Window Suffix Skip

Problem: ordered windows are often treated as partition recompute even when new
rows append after all existing rows in a partition.

Rule: for backward-looking frames, if delta rows are strictly after the previous
maximum order key for each touched partition, only compute the new suffix rows.

Correctness: previous rows' frames are unchanged when all new rows occur after
them and the frame only looks backward.

Status: implemented as an opt-in for cumulative running aggregates on insert-only batches
(`openivm_running_window_incremental`, default `false`). Other window refreshes recompute the
affected partitions.

## 7. Downstream Empty-Net-Delta Skip

Problem: a view refresh may produce an empty net delta, but downstream dependent
views still refresh.

Rule: after compiling or materializing a view delta, if the net delta is empty,
skip downstream refreshes that depend only on that delta.

Correctness: downstream delta rules are functions of upstream deltas. If the
upstream delta is zero, every downstream term containing it is zero.

Status: implemented. Downstream views are still visited, but each skips its refresh when its
own deltas are empty. Publication emits only changed visible rows, and an MV delta retained
for consumers is compacted to net rows, so an unchanged result yields an empty downstream delta.
