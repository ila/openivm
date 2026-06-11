# Delta Skipping Rules

Consolidated catalog of the rules under `openivm_skip_empty_deltas`: each proves a source
delta cannot affect a refresh, or that a smaller equivalent delta can be used, so the
corresponding work is skipped. Several rules have a dedicated page with the implemented
behavior and examples — this page is the unified reference and design rationale.

See also: [Empty delta skip](empty-delta-skip.md) (rules 1–3: the early-exit + per-term join
skip), [FK-aware pruning](fk-aware-pruning.md) (rule 5), [Append-only optimizations](append-only.md)
(rules 4 and 6), [DuckLake integration](../ducklake.md) (rule 1), and
[Pipelines](../refresh/pipelines.md) (rule 7, downstream cascade). The controlling setting is in
[Configuration](../configuration.md).

## 1. DuckLake Table No-Op Skip

Problem: DuckLake snapshot ids are catalog-wide. A source table can have
`last_snapshot_id != current_snapshot_id` even when that table has no row changes.

Rule: before building a DuckLake join term for table `T`, check
`ducklake_table_insertions(catalog, schema, T, old, current)` and
`ducklake_table_deletions(catalog, schema, T, old, current)`. If both are empty,
skip the term for `T`.

Correctness: the delta relation for `T` is the zero Z-set, so every join term that
uses `delta(T)` is zero.

## 2. Key-Domain Join Skip

Problem: a non-empty source delta may still have no matching join keys in the
other side.

Rule: for an equi-join `R.k = S.k`, skip the `delta(R)` term when
`SELECT DISTINCT k FROM delta(R)` has no intersection with `SELECT DISTINCT k FROM S`
under the relevant snapshot.

Correctness: the join term is empty if the delta key domain is disjoint from the
opposite input key domain.

## 3. Predicate-Disjoint Delta Skip

Problem: delta rows may fail filters or range predicates before they reach the
expensive part of the plan.

Rule: push view predicates and join range predicates into the delta probe. If the
filtered delta is empty, skip that delta term.

Correctness: selection is linear over Z-sets: `sigma_p(delta(R)) = 0` means every
term using that filtered delta is zero.

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

## 5. Functional-Dependency / Uniqueness Skip

Problem: joins to key-unique dimensions can be irrelevant when no dimension
columns are projected and the join is guaranteed by constraints.

Rule: if constraints prove every left row has exactly one matching right row and
right columns are unused, remove or skip the right-side delta for that join.

Correctness: the join is multiplicity-preserving under the constraint, so changes
to non-output right columns cannot change the view.

## 6. Append-Only Window Suffix Skip

Problem: ordered windows are often treated as partition recompute even when new
rows append after all existing rows in a partition.

Rule: for backward-looking frames, if delta rows are strictly after the previous
maximum order key for each touched partition, only compute the new suffix rows.

Correctness: previous rows' frames are unchanged when all new rows occur after
them and the frame only looks backward.

## 7. Downstream Empty-Net-Delta Skip

Problem: a view refresh may produce an empty net delta, but downstream dependent
views still refresh.

Rule: after compiling or materializing a view delta, if the net delta is empty,
skip downstream refreshes that depend only on that delta.

Correctness: downstream delta rules are functions of upstream deltas. If the
upstream delta is zero, every downstream term containing it is zero.
