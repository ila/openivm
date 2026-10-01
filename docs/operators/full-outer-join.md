# Full outer join

> Linearity: **BILINEAR** ([what does this mean?](../internals/linearity.md))

## Example

```sql
CREATE TABLE employees (id INT, name VARCHAR);
CREATE TABLE projects (id INT, emp_id INT, title VARCHAR);
INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
INSERT INTO projects VALUES (10, 1, 'Alpha'), (20, 1, 'Beta'), (30, 4, 'Gamma');

CREATE MATERIALIZED VIEW emp_projects AS
    SELECT e.name, p.title
    FROM employees e FULL OUTER JOIN projects p ON e.id = p.emp_id;
```

Initial result:

| name | title |
|---|---|
| Alice | Alpha |
| Alice | Beta |
| Bob | NULL |
| Charlie | NULL |
| NULL | Gamma |

```sql
-- Bob gets a project: his NULL row is replaced with real data
INSERT INTO projects VALUES (40, 2, 'Delta');
PRAGMA refresh('emp_projects');
```

| name | title |
|---|---|
| Alice | Alpha |
| Alice | Beta |
| Bob | Delta |
| Charlie | NULL |
| NULL | Gamma |

## How IVM handles it

**Algebraic rule (Zhang & Larson decomposition):**

A FULL OUTER JOIN decomposes into three components:
1. **Matched rows** (inner join)
2. **Dangling-left rows** (left rows with no right match, NULL-extended)
3. **Dangling-right rows** (right rows with no left match, NULL-extended)

The delta query uses inclusion-exclusion with FULL OUTER demoted to INNER for all terms (same as [inner join](inner-join.md)). This captures matched-row changes. Unmatched-row changes are handled separately in the upsert phase.

### Projection views (no GROUP BY)

Bidirectional key-based partial recompute using hidden `openivm_left_key` and `openivm_right_key` columns:

1. Get affected join keys from both base delta tables.
2. DELETE from the MV all rows where `openivm_left_key` or `openivm_right_key` matches an affected key.
3. Re-INSERT from the original FULL OUTER JOIN query, filtered to those keys.

The match predicate is **NULL-safe** — `EXISTS (SELECT 1 FROM openivm_affected WHERE _k IS NOT DISTINCT FROM openivm_left_key OR _k IS NOT DISTINCT FROM openivm_right_key)` rather than `IN`, so NULL keys on either side are still matched.

### Aggregate views (with GROUP BY)

Two modes are available, controlled by `openivm_full_outer_merge` (default: on):

**MERGE mode (default):** adds hidden `openivm_match_count` (`COUNT` of the right join key) and `openivm_right_match_count` (`COUNT` of the left join key) columns to track matches from each side. When a count transitions between 0 and positive, that side's aggregate columns transition between NULL and actual values. After the MERGE, OpenIVM additionally deletes and re-inserts the affected groups (the same affected-key set as group-recompute mode, below) plus the all-NULL group, to handle unmatched-row changes and cross-group transfers.

**Group-recompute mode** (`SET openivm_full_outer_merge = false`) identifies affected GROUP BY keys from these sources:
1. Delta view (matched-row group keys)
2. If a GROUP BY column matches a join column: groups of the view query whose key is a changed join key from either base delta table
3. With a single GROUP BY column: the left delta table (group column directly available for unmatched-left changes)
4. With a single GROUP BY column: a left base table lookup (maps right-side join keys to group keys)
5. The all-NULL group (always recomputed for unmatched-right changes)

DELETE + re-INSERT only the affected groups. Group keys are matched NULL-safely (`EXISTS ... IS NOT DISTINCT FROM`) rather than with tuple `IN`: SQL's `(a, b, NULL) IN (...)` returns NULL (not TRUE), so partially-NULL key tuples — common when COALESCE over JOIN-padded NULLs is the GROUP BY key — would be skipped. The all-NULL group (every key column NULL, produced by unmatched-right rows) is covered explicitly via an `OR (k1 IS NULL AND k2 IS NULL ...)` clause.

## Settings

| Setting | Default | Description |
|---|---|---|
| `openivm_full_outer_merge` | `true` | Use incremental MERGE for aggregate views instead of group-recompute |

## Limitations

- Maximum 16 tables in a join (same as all joins)
- GROUP BY columns are assumed to come from the left table for group-recompute key mapping
- The MERGE mode also recomputes the affected groups and the NULL group (unmatched-right rows) after the MERGE, so it is not fully incremental for those groups
