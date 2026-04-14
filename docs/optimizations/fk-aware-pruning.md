# FK-Aware Inclusion-Exclusion Pruning

## Problem

For an N-table inner join, the inclusion-exclusion delta rule generates 2^N - 1 terms.
Each term replaces a subset of base table scans with delta scans and UNION ALLs all
terms together. For a 3-table star join (1 fact + 2 dimensions), this produces 7 terms.

Many of these terms are redundant when foreign key constraints hold and the referenced
(PK-side) table's delta is insert-only.

## Algebraic Rule

### Standard Inclusion-Exclusion (2-table join)

For a view `V = R ⋈ S`, the delta rule with current-state scans and XOR multiplicity:

    mask=01: ΔR ⋈ S_current    (mul = mul_ΔR)
    mask=10: R_current ⋈ ΔS    (mul = mul_ΔS)
    mask=11: ΔR ⋈ ΔS           (mul = mul_ΔR XOR mul_ΔS)

OpenIVM reads the CURRENT (post-batch) state for non-delta scans. The XOR in mask=11
corrects for the resulting double-counting of the cross-term `ΔR ⋈ ΔS`.

### FK Constraint: R.fk → S.pk, ΔS insert-only

When `R.fk` references `S.pk` and ΔS contains only inserts (ΔS⁺):

**All terms with S's bit set cancel to zero, regardless of ΔR:**

    mask=10: R_current ⋈ ΔS⁺ = (R_old + ΔR) ⋈ ΔS⁺ = R_old⋈ΔS⁺ + ΔR⋈ΔS⁺
    mask=11: ΔR ⋈ ΔS⁺ (XOR sign = -1)               = -ΔR⋈ΔS⁺
    ─────────────────────────────────────────────────────────────
    Net:                                                R_old⋈ΔS⁺

By FK integrity, no row in R_old references any newly-inserted PK value, so:

    R_old ⋈ ΔS⁺ = ∅

The `ΔR⋈ΔS⁺` parts cancel exactly between terms 10 and 11, so this works even
when ΔR is non-empty (new FK rows referencing new PKs are handled correctly).

**The delta rule simplifies to:**

    Δ(R ⋈ S) = ΔR ⋈ S_current    (just 1 term instead of 3)

### N-table Star Schema Example

For `V = F ⋈ D₁ ⋈ D₂` where `F.fk₁ → D₁.pk` and `F.fk₂ → D₂.pk`,
all dimension deltas insert-only:

Standard 7 terms (masks over {F, D₁, D₂}):

    001: ΔF  ⋈ D₁ ⋈ D₂           ✓ kept (no PK bit set)
    010: F   ⋈ ΔD₁ ⋈ D₂          ✗ pruned (D₁ bit set, D₁ insert-only)
    011: ΔF  ⋈ ΔD₁ ⋈ D₂          ✗ pruned (D₁ bit set)
    100: F   ⋈ D₁ ⋈ ΔD₂          ✗ pruned (D₂ bit set, D₂ insert-only)
    101: ΔF  ⋈ D₁ ⋈ ΔD₂          ✗ pruned (D₂ bit set)
    110: F   ⋈ ΔD₁ ⋈ ΔD₂         ✗ pruned (both PK bits set)
    111: ΔF  ⋈ ΔD₁ ⋈ ΔD₂         ✗ pruned (both PK bits set)

**Result: 1 term instead of 7.** The surviving term is `ΔF ⋈ D₁_current ⋈ D₂_current`.

For N+1 tables (1 fact + N dims, all dims insert-only): **2^(N+1) - 1 → 1 term.**

### When ΔS Contains Deletes or Updates

If `ΔS` contains deletions (`ΔS⁻`), then `R_old ⋈ ΔS⁻ ≠ ∅` because existing R rows
DO reference the deleted PK values. The cancellation does not hold, so no terms are
pruned. Deletes make the delta non-insert-only, disabling this optimization.

### Why DuckDB Cannot Do This

DuckDB's query optimizer does not use foreign key constraints for join optimization.
It cannot determine that the net contribution of terms involving an insert-only PK
delta is zero.

## Pruning Rule

A term with bitmask `mask` is pruned if:

    (mask & skip_bits) != 0

where `skip_bits` is the OR of all PK leaf bits that satisfy:
1. The PK leaf is the referenced side of a FOREIGN KEY declared in the join
2. The PK leaf's delta is insert-only (no `_duckdb_ivm_multiplicity = false` rows)

This is a single bitmask check per term — O(1).

## Impact

| Scenario | Standard terms | After FK pruning |
|---|---|---|
| 2 tables (F + D₁), D₁ insert-only | 3 | 1 |
| 3 tables (F + D₁ + D₂), both dims insert-only | 7 | 1 |
| N+1 tables (F + N dims), all dims insert-only | 2^(N+1) - 1 | 1 |
| Any table has deletes/updates | 2^N - 1 | 2^N - 1 (no pruning) |

Each pruned term avoids: plan copy + renumbering + delta scan replacement + hash join
build + hash join probe + UNION ALL branch.

## When It Applies

- Base tables have declared `FOREIGN KEY` constraints
- The referenced (PK-side) table's delta contains only inserts since last refresh
- The join is an inner join between the FK and PK tables

## When It Does Not Apply

- No FK constraints declared between join tables
- PK-side delta contains deletes or updates (multiplicity = false)
- LEFT/RIGHT joins (FK semantics don't guarantee empty results)
- Self-referencing FKs (both sides are the same table)
- **DuckLake tables**: DuckLake does not support `FOREIGN KEY` constraints. For DuckLake
  joins, the [N-term telescoping](../ducklake.md#n-term-telescoping-join-rule) rule is used
  instead, with [empty-delta term skipping](empty-delta-skip.md) covering the common case of
  unchanged dimension tables.

## How it works

1. **FK detection:** Walk the join tree, inspect each base table's declared constraints
   for `FOREIGN KEY` references to other tables in the join.
2. **Insert-only check:** For each referenced (PK-side) table, check whether its delta
   contains any deletions since the last refresh. If not, the delta is insert-only.
3. **Skip bits:** Build a bitmask of all insert-only PK leaves.
4. **Pruning:** Any inclusion-exclusion term whose bitmask overlaps with the skip bits
   is discarded — a single bitmask check per term.

## Example

```sql
CREATE TABLE dim_product(product_id INTEGER PRIMARY KEY, name VARCHAR);
CREATE TABLE fact_sales(
    sale_id INTEGER,
    product_id INTEGER REFERENCES dim_product(product_id),
    amount DECIMAL(10,2)
);

CREATE MATERIALIZED VIEW sales_by_product AS
    SELECT p.name, SUM(f.amount) as total
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.name;

-- Mixed batch: dim insert + fact insert + fact delete.
-- dim_product delta is insert-only → 2/3 terms pruned, 1 remaining.
-- The surviving term (ΔF ⋈ D_current) correctly picks up the new product.
INSERT INTO dim_product VALUES (99, 'NewProduct');
INSERT INTO fact_sales VALUES (100, 99, 500.0);
DELETE FROM fact_sales WHERE sale_id = 1;
PRAGMA ivm('sales_by_product');  -- 2 terms pruned, 1 remaining
```

## References

- Christoforos Svingos, Andre Hernich, Hinnerk Gildhoff, Yannis Papakonstantinou,
  Yannis E. Ioannidis. "Foreign Keys Open the Door for Faster Incremental View
  Maintenance." Proc. ACM Manag. Data 1(1), 2023 (SIGMOD).
  https://dl.acm.org/doi/10.1145/3588720
- Ahmet Kara, Milos Nikolic, Dan Olteanu, Haozhe Zhang. "Insert-Only versus
  Insert-Delete in Dynamic Query Evaluation." Proc. ACM Manag. Data 2(3), 2024
  (SIGMOD). https://dl.acm.org/doi/10.1145/3695837
