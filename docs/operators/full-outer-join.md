# Full outer join

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
PRAGMA ivm('emp_projects');
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

Bidirectional key-based partial recompute using hidden `_ivm_left_key` and `_ivm_right_key` columns:

1. Get affected join keys from both base delta tables.
2. DELETE from the MV all rows where `_ivm_left_key` or `_ivm_right_key` matches an affected key.
3. Re-INSERT from the original FULL OUTER JOIN query, filtered to those keys.

### Aggregate views (with GROUP BY)

Two modes, controlled by `ivm_full_outer_merge` (default: off):

**Group-recompute mode (default):** identifies affected GROUP BY keys from 4 sources:
1. Delta view (matched-row group keys)
2. Left delta table (group column directly available for unmatched-left changes)
3. Left base table lookup (maps right-side join keys to group keys)
4. NULL group (always recomputed for unmatched-right changes)

DELETE + re-INSERT only the affected groups.

**MERGE mode** (`SET ivm_full_outer_merge = true`): uses the Larson & Zhou `_ivm_match_count` column to track how many right rows match each left group. When the count transitions between 0 and positive, right-side aggregate columns transition between NULL and actual values. The NULL group (unmatched-right rows) is always recomputed separately to handle cross-group transfers.

## Settings

| Setting | Default | Description |
|---|---|---|
| `ivm_full_outer_merge` | `false` | Use incremental MERGE for aggregate views instead of group-recompute |

## Limitations

- Maximum 16 tables in a join (same as all joins)
- GROUP BY columns are assumed to come from the left table for group-recompute key mapping
- The MERGE mode always recomputes the NULL group (unmatched-right rows), so it is not fully incremental for that group
