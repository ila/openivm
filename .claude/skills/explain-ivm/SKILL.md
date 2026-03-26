---
name: explain-ivm
description: Comprehensive reference for Incremental View Maintenance (IVM) algebraic rules, delta rules for all relational operators, multiplicity approaches, efficiency techniques, and correctness considerations. Auto-loaded when discussing IVM theory, delta computation, view maintenance algorithms, or operator incrementalization.
---

# Incremental View Maintenance: Comprehensive Reference

## 1. Foundations

IVM is the problem of efficiently updating materialized views when base tables change,
without recomputing the view from scratch. Given a view V = Q(T1, T2, ..., Tn), when
a base table Ti changes by DTi, compute DV such that V_new = V_old + DV. The cost of
computing DV should be proportional to |DTi|, not |Ti|.

### 1.1 Algebraic Framework: Relations Over Rings

The modern formalization (Olteanu, Koch, DBSP) models relations as functions from
tuples to values in a ring (D, +, *, 0, 1):

    R : Dom(S) -> D

where R(t) != 0 for finitely many tuples t. The most common ring is (Z, +, *, 0, 1),
the integers, where R(t) gives the multiplicity of tuple t. Positive values represent
tuple presence; negative values represent deletions.

**Why rings?** Ring structure gives us:
- Addition (+) for combining relations (union/merge)
- Multiplication (*) for join (multiplicity of join output = product of input multiplicities)
- Additive inverse (-) for computing differences (deltas)
- Commutativity of + means order of applying inserts/deletes does not matter

### 1.2 Updates as Ring Elements

An update (delta) DR to relation R maps each affected tuple to:
- +1 for an insertion
- -1 for a deletion
- +k/-k for batch inserts/deletes of k copies
- Updates are modeled as delete-old + insert-new: DR(t) = -1 (old value) + 1 (new value)

The new relation after update: R' = R + DR (pointwise addition of multiplicities).

### 1.3 The Delta Operator

The delta operator D applied to a query Q with respect to relation R:

    D_R[Q] = Q(R + DR, S, T, ...) - Q(R, S, T, ...)

This gives the change to the query result when R changes by DR.


## 2. Delta Rules for Each Relational Operator

### 2.1 Selection (sigma / Filter)

**Delta rule:**

    D(sigma_p(R)) = sigma_p(DR)

**Derivation:** Since sigma_p is a linear operator (it processes each tuple independently),
applying the same predicate p to the delta gives exactly the change to the filtered result.

**Subtleties:**
- The predicate p is evaluated on the delta tuples, not on the base table.
- For predicates referencing other tables (correlated subqueries), the rule is more
  complex -- see Section 2.10 on subqueries.
- Selection commutes with the delta operator: D and sigma_p can be applied in either order.
- If the predicate involves columns not in the delta, no special handling is needed because
  the delta tuple carries all columns of R.

### 2.2 Projection (pi)

**Bag semantics (no DISTINCT):**

    D(pi_A(R)) = pi_A(DR)

Projection under bag semantics is linear -- it passes through deltas unchanged. If tuple
t appears 3 times in R and we insert one more copy, pi_A(t) gains one more copy.

**Set semantics (with DISTINCT):**

Projection with DISTINCT is NOT linear. The problem: if R contains (a, 1) and (a, 2),
then pi_{col1}(R) = {a} (one copy). But if we insert (a, 3), the delta pi_{col1}(DR) = {a},
which would incorrectly suggest we should add another copy of 'a' to the view.

**Solution: The counting approach for DISTINCT projection.**
See Section 2.7 (DISTINCT) for the full counting algorithm. The key idea: maintain a
hidden count column tracking how many base tuples map to each projected tuple. Only
emit an insertion to the view when the count goes from 0 to 1, and only emit a deletion
when it goes from 1 to 0.

### 2.3 Join (natural join / equi-join)

This is the most important and complex delta rule. There are three main formulations.

#### 2.3.1 Single-Table Change (One Delta)

When only R changes (S is unchanged):

    D(R join S) = DR join S

This is the simplest case. The delta of the join is the delta of R joined with the
current (unchanged) state of S. This is exact and requires no correction terms.

#### 2.3.2 Two-Table Change: Three Formulations

When BOTH R and S change simultaneously (DR and DS), we need:

    (R + DR) join (S + DS) - R join S

Expanding this product:

    = R join S + R join DS + DR join S + DR join DS - R join S
    = R join DS + DR join S + DR join DS

This gives the **three-term inclusion-exclusion formula**:

    D(R join S) = DR join S + R join DS + DR join DS

**Problem:** This uses three join operations. Can we do better?

**Formulation 1: Old-State Approach**

    D(R join S) = DR join S_old + R_new join DS

Here S_old is the state of S before the update, and R_new = R + DR is the state after.
Equivalently:

    = DR join S_old + (R_old + DR) join DS
    = DR join S_old + R_old join DS + DR join DS

This is correct because it covers: (a) new R tuples matching old S tuples, (b) all R
tuples (including new ones) matching new S tuples. No double-counting because the
second term uses S's NEW tuples only (DS), not the full new S.

**Formulation 2: New-State Approach**

    D(R join S) = DR join S_new + R_old join DS

Here S_new = S + DS. This is also correct by symmetric reasoning.

**Formulation 3: Inclusion-Exclusion (Three Terms)**

    D(R join S) = DR join S_old + R_old join DS + DR join DS

Uses only old states of both tables but requires three terms.

**Comparison:**
- Formulations 1 and 2 require only TWO join operations but need access to both the
  old and new states of one table.
- Formulation 3 requires THREE join operations but only needs old states plus deltas.
- Formulations 1 and 2 are preferred when the system can access both pre-update and
  post-update table states (e.g., via transition tables in PostgreSQL).
- The DR join DS term is often negligible (small delta joined with small delta) and is
  sometimes dropped as an approximation when deltas are tiny, but this is NOT correct
  for exact maintenance.

**DBSP's bilinear join formula:**

    D(A join B) = DA join DB + A_prev join DB + DA join B_prev

where A_prev and B_prev are the accumulated states before the current tick. This is
equivalent to the inclusion-exclusion formulation.

#### 2.3.3 N-Way Join (Multi-Way Join Incrementalization)

For V = R1 join R2 join ... join Rn, when all tables change simultaneously:

**Naive expansion:** Expand (R1+DR1) join (R2+DR2) join ... join (Rn+DRn) - R1 join R2 join ... join Rn.
This gives 2^N - 1 terms (all non-empty subsets where at least one table is replaced
by its delta).

**Example: 3-table join V = R join S join T:**

    DV = DR join S join T
       + R join DS join T
       + R join S join DT
       + DR join DS join T
       + DR join S join DT
       + R join DS join DT
       + DR join DS join DT

That is 2^3 - 1 = 7 terms. But using pre/post-update states, this reduces to N terms:

    DV = DR join S_new join T_new
       + R_old join DS join T_new
       + R_old join S_old join DT

This works because each delta term uses the NEW state for all tables that come before it
in some fixed ordering, and the OLD state for all tables that come after. This ensures
each combination is counted exactly once. PostgreSQL's IVM implementation uses this
N-term approach.

**Materialize's Delta Query approach:**
For an N-way join, Materialize creates N dataflow paths, one per input relation. When
relation Ri changes, the delta is joined with the CURRENT STATE (arrangements) of all
other relations. This requires that arrangements (indexed representations) of all
relations are maintained. The key advantage: zero intermediate state -- the arrangements
are shared across many queries.

#### 2.3.4 Outer Join Incrementalization

Based on Larson & Zhou (2007), outer join maintenance is more complex than inner join
because of **dangling tuples** (null-extended tuples from unmatched rows).

**Left Outer Join: R LEFT JOIN S ON p**

When R changes (insert into R):
1. Compute directly affected tuples: DR join S (inner join part)
2. For each tuple in DR that has NO match in S, emit a null-extended tuple (dangling tuple)

When R changes (delete from R):
1. Delete matching tuples: remove DR join S results from view
2. Delete any dangling tuples for the deleted R rows

When S changes (insert into S):
1. Compute new join matches: R join DS
2. BUT: some of these matches may replace previously dangling R tuples. For each R tuple
   that now has a match in S but previously didn't, DELETE the old dangling tuple and
   INSERT the proper joined tuple.

When S changes (delete from S):
1. Remove join matches: delete R join DS from view
2. For R tuples that LOSE their last match in S, INSERT a new dangling (null-extended) tuple.

**Key insight:** Insertions to the null-supplying side (S in LEFT JOIN) can cause
DELETIONS of dangling tuples. Deletions from the null-supplying side can cause
INSERTIONS of dangling tuples. This bidirectional effect makes outer join maintenance
significantly more complex.

**Full Outer Join** combines both left and right outer join logic, handling dangling tuples
on both sides.

The PostgreSQL IVM implementation constructs a "view maintenance graph" based on Larson &
Zhou's algorithm.

#### 2.3.5 Semi-Join Delta Rules

**Semi-join R SEMI JOIN S (tuples in R that have at least one match in S):**

When R changes:
    D(R semi S) = DR semi S
    (New R tuples that have matches in the current S)

When S changes (insert into S):
    New R tuples may now qualify. Compute: R anti-semi S_old semi DS
    (R tuples that had NO match in old S but DO match new S tuples)

When S changes (delete from S):
    R tuples may lose their last match. If we maintain a count of matches per R tuple,
    decrement counts. If count reaches 0, delete from view.

**Practical approach:** Maintain a hidden count of how many S tuples match each R tuple.
The semi-join view emits a tuple when count goes 0->positive and removes it when count
goes positive->0.

#### 2.3.6 Anti-Join Delta Rules

**Anti-join R ANTI JOIN S (tuples in R with NO match in S):**

This is the complement of semi-join. The delta rules are the inverse:

When R changes (insert):
    D(R anti S) = DR anti S
    (New R tuples that have NO match in S)

When S changes (insert into S):
    R tuples that previously had no match may now have one. These must be DELETED from the
    anti-join result. Compute: R anti S_old semi DS, then delete these from the view.

When S changes (delete from S):
    R tuples that previously had exactly one match (now losing it) must be INSERTED into
    the anti-join result. Again, match counting is the practical approach.

**Key insight:** Anti-join is non-monotone: inserting into S can cause deletions from the
view. This is why anti-join and NOT EXISTS are among the hardest operators to incrementalize.

In Z-set / ring-based frameworks, anti-join is expressed as: R anti S = R - R semi S,
and the delta rule follows from linearity and the semi-join delta rule.

#### 2.3.7 Theta-Join Considerations

For theta-joins (joins with arbitrary predicates, not just equality), the delta rules have
the same algebraic form but the execution cost may be much higher because:
- Hash indexes cannot be used for non-equality predicates
- The join between DR and S may require a full scan of S
- Predicate filtering can help reduce this cost

### 2.4 Aggregation (gamma / GROUP BY + Aggregate Functions)

**General principle:** For a view V = gamma_{G, agg(A)}(R), the delta is computed
per-group. For each group key g in the delta:

    V_new[g] = combine(V_old[g], aggregate_delta[g])

where `combine` depends on the specific aggregate function.

**The delta rule for aggregation (from DBSP/ring framework):**

    D(SUM_x R) = SUM_x (DR)

Aggregation (summation over a variable) distributes over deltas for LINEAR aggregates.
This is exact for SUM and COUNT.

#### 2.4.1 SUM

    V_new[g].sum = V_old[g].sum + SUM(DR[g].value * DR[g].multiplicity)

For insertions (multiplicity +1): add the new values.
For deletions (multiplicity -1): subtract the deleted values.

SUM is fully **distributive**: SUM(A union B) = SUM(A) + SUM(B).

**Edge case:** If all tuples in a group are deleted, the group should be removed from the
view (not show SUM = 0), unless the query specifically includes empty groups.

**NULL handling:** SUM ignores NULLs. To track this incrementally, maintain an auxiliary
count of non-NULL values per group. If this count reaches 0, the SUM should be NULL, not 0.

#### 2.4.2 COUNT

    V_new[g].count = V_old[g].count + COUNT(inserts in DR[g]) - COUNT(deletes in DR[g])

COUNT is distributive: COUNT(A union B) = COUNT(A) + COUNT(B) under bag semantics.

If the count reaches 0, the group should be removed from the view.

**COUNT(*)** counts all rows including NULLs.
**COUNT(column)** counts non-NULL values; maintain an auxiliary non-NULL count.

#### 2.4.3 AVG

AVG is NOT distributive: AVG(A union B) != combine(AVG(A), AVG(B)) without auxiliary state.

**Decomposition into SUM and COUNT:**

    AVG(x) = SUM(x) / COUNT(x)

Maintain both SUM and COUNT as hidden auxiliary columns. On each delta:

    V_new[g].sum = V_old[g].sum + delta_sum
    V_new[g].count = V_old[g].count + delta_count
    V_new[g].avg = V_new[g].sum / V_new[g].count

This is the standard approach used in PostgreSQL IVM and OpenIVM.

#### 2.4.4 MIN / MAX

MIN and MAX are **non-distributive over deletions**. They are distributive over insertions:

    MIN(A union B) = min(MIN(A), MIN(B))   -- works for inserts

But for deletions, if the current MIN value is deleted, we CANNOT compute the new MIN
from just the old MIN and the delta -- we need to look at the remaining data.

**Approaches:**

**Approach 1: Full recomputation on delete of current extremum.**
- On insert: compare new value with current MIN; update if smaller. Cost: O(1).
- On delete: if deleted value equals current MIN, recompute MIN from base table for
  that group. Cost: O(|group|) worst case.
- Simple to implement, but worst-case expensive.
- Used by PostgreSQL IVM.

**Approach 2: Auxiliary sorted structure (e.g., sorted list, B-tree, or heap per group).**
- Maintain a sorted index per group key on the aggregated column.
- On insert: insert into sorted structure. Cost: O(log n).
- On delete: remove from sorted structure; new MIN/MAX is the first/last element. Cost: O(log n).
- Guarantees O(log n) per operation but requires O(n) auxiliary space per group.
- Feldera uses a radix tree for similar purposes.

**Approach 3: Counting-based (maintain full multiset per group).**
- Keep a count for each distinct value within each group.
- On delete: decrement count; if count reaches 0, the next value becomes the new MIN/MAX.
- Equivalent to Approach 2 but uses a hash map + tracking of extremum.
- Space: O(distinct values per group).

**Approach 4: Selective recomputation (Palpanas et al., VLDB 2002).**
- Only recompute groups where the MIN/MAX was actually affected by the deletion.
- Push down the group key predicate to limit the recomputation scope.
- Achieves 1-4% of full refresh time in practice for deletions.

#### 2.4.5 COUNT DISTINCT

COUNT DISTINCT is particularly difficult because it combines the challenges of both
DISTINCT (requiring counting) and aggregation.

**Approach: Nested counting.**
- For each group g, maintain a sub-count of how many times each distinct value appears.
- COUNT DISTINCT for group g = number of distinct values whose sub-count > 0.
- On insert of value v into group g:
  - Increment sub-count[g][v].
  - If sub-count went from 0 to 1: increment COUNT_DISTINCT[g].
- On delete of value v from group g:
  - Decrement sub-count[g][v].
  - If sub-count went from 1 to 0: decrement COUNT_DISTINCT[g].
- Space: O(sum of distinct values across all groups) for the sub-counts.

This is essentially maintaining a nested counting structure: one level for the group,
another level for the distinct values within the group.

#### 2.4.6 STDDEV, VARIANCE

Like AVG, these can be maintained with auxiliary state.

**VARIANCE** can be computed incrementally by maintaining three values per group:
- COUNT(x) = n
- SUM(x) = sum of values
- SUM(x^2) = sum of squares

Then: VAR(x) = SUM(x^2)/n - (SUM(x)/n)^2

On each delta, update all three auxiliary values. The variance is derived from them.

**STDDEV** = sqrt(VARIANCE), computed from the same auxiliary state.

#### 2.4.7 General Aggregate Framework

**Distributive aggregates:** agg(A union B) = combine(agg(A), agg(B))
- SUM, COUNT, MIN (insert-only), MAX (insert-only)
- Can be maintained with O(1) work per delta tuple.

**Algebraic aggregates:** Can be computed from a fixed set of auxiliary distributive values.
- AVG = SUM/COUNT (2 auxiliary values)
- VARIANCE = needs SUM, SUM_SQ, COUNT (3 auxiliary values)
- STDDEV = sqrt(VARIANCE) (same 3 auxiliary values)
- These can be maintained with O(1) work per delta tuple plus auxiliary storage.

**Holistic aggregates:** Cannot be computed from fixed-size auxiliary state.
- MEDIAN, MODE, PERCENTILE
- Require maintaining the full multiset (or a sorted structure) per group.
- Maintenance cost: O(log n) per operation with a sorted structure.

#### 2.4.8 The Empty Group Problem

After all tuples in a group are deleted, the group must be removed from the view
(COUNT becomes 0, SUM becomes undefined). The counting algorithm handles this:
- Maintain a COUNT per group.
- When COUNT reaches 0, delete the group's row from the view.
- This is separate from the aggregate's own value.

#### 2.4.9 NULL Handling in Aggregates

- SUM, AVG, MIN, MAX: ignore NULL inputs. To track correctly, maintain a count of
  non-NULL values per group as an auxiliary column.
- COUNT(*): counts all rows including NULLs.
- COUNT(column): counts non-NULL values only.
- If all non-NULL values in a group are deleted, SUM/AVG/MIN/MAX should return NULL,
  not 0. The non-NULL count auxiliary column enables this check.

### 2.5 Set Operations

#### 2.5.1 UNION ALL

Trivially linear:

    D(R UNION ALL S) = DR UNION ALL DS

Changes to either input propagate directly to the output.

#### 2.5.2 UNION (DISTINCT)

UNION = DISTINCT(UNION ALL). Uses the counting approach:
- Maintain a count for each tuple from both inputs combined.
- A tuple appears in the output iff its total count > 0.
- On insert: increment count; if was 0, insert into view.
- On delete: decrement count; if becomes 0, delete from view.

#### 2.5.3 INTERSECT ALL

Under bag semantics, INTERSECT ALL returns min(count_R(t), count_S(t)) copies of each tuple t.

    D(R INTERSECT ALL S):

For each tuple t in DR:
- If inserting into R: if count_R(t) <= count_S(t) before insert, then insert t into view.
- If deleting from R: if count_R(t) <= count_S(t) before delete, then delete t from view.

Requires maintaining per-tuple counts for both R and S.

#### 2.5.4 INTERSECT (DISTINCT)

Same as INTERSECT ALL but with boolean counts (present/absent):
- Tuple appears in output iff it exists in BOTH R and S.
- Insert into R: if tuple already in S, insert into view (if not already there).
- Delete from R: if tuple was in both R and S, and no longer in R, delete from view.

#### 2.5.5 EXCEPT ALL

Under bag semantics, R EXCEPT ALL S returns max(0, count_R(t) - count_S(t)) copies.

    D(R EXCEPT ALL S):

For each tuple t in DR (change to R):
- If inserting into R: if count_R(t) > count_S(t) after insert, insert t into view.
- If deleting from R: if count_R(t) >= count_S(t) before delete, delete t from view.

For each tuple t in DS (change to S):
- If inserting into S: if count_R(t) > count_S(t) before insert AND count_R(t) <= count_S(t) after, delete t from view.
- If deleting from S: if count_R(t) > count_S(t) after delete AND count_R(t) <= count_S(t) before, insert t into view.

**Key insight:** Insertions to S can cause deletions from the result (non-monotone).

#### 2.5.6 EXCEPT (DISTINCT)

R EXCEPT S = tuples in R but not in S (set difference).
- Insert into R: if tuple not in S, insert into view.
- Delete from R: if tuple was in view, delete from view.
- Insert into S: if tuple was in view (was in R, not previously in S), delete from view.
- Delete from S: if tuple is in R and was previously in S, insert into view.

### 2.6 DISTINCT

The DISTINCT operator eliminates duplicates, converting a bag to a set.

**In Z-set / ring terms:** DISTINCT(R)(t) = 1 if R(t) > 0, else 0.
This is a non-linear operation (it clamps positive values to 1 and sets everything else to 0).

#### The Counting Algorithm (Gupta & Mumick, 1993/1995)

Maintain a hidden column `__ivm_count__` storing the multiplicity of each tuple.

**On insertion of tuple t:**
1. Look up t in the view.
2. If t exists: increment __ivm_count__ by 1. Do NOT emit an output change.
3. If t does not exist (count was 0): insert t with __ivm_count__ = 1. Emit +1 to downstream.

**On deletion of tuple t:**
1. Look up t in the view.
2. Decrement __ivm_count__ by 1.
3. If __ivm_count__ is still > 0: do NOT emit an output change.
4. If __ivm_count__ reaches 0: delete t from view. Emit -1 to downstream.

**Properties:**
- Exactly optimal: emits a view change if and only if the DISTINCT output actually changes.
- Cost per operation: O(1) with a hash index on the tuple (or on the columns participating in DISTINCT).
- Space overhead: one integer column per distinct tuple.

**DBSP's distinct optimization:**
DBSP defines the distinct operator D as: D(R)(t) = 1 if R(t) > 0, else 0.
The incremental version requires comparing the new weight with the old weight:
- If old weight was 0 and new weight > 0: emit +1
- If old weight > 0 and new weight <= 0: emit -1
- Otherwise: no change

This is non-linear, so the incremental distinct operator must maintain the full accumulated
state (the counts) -- it cannot be computed from the delta alone.

### 2.7 LIMIT / ORDER BY / OFFSET

These operators are **fundamentally problematic** for IVM because they depend on the
global ordering of ALL rows, not just the affected rows.

**ORDER BY + LIMIT:**
- Inserting a single row could displace the Nth row from the top-N result.
- Deleting a row from the top-N requires finding the (N+1)th row to replace it.
- This requires maintaining a sorted index on the ORDER BY columns.

**OFFSET:**
- Even worse: any insertion/deletion before the offset point shifts all subsequent rows.
- A single change can cause O(LIMIT) changes to the view.

**Current status:** Most IVM systems (PostgreSQL, OpenIVM, pg_ivm) do NOT support
ORDER BY, LIMIT, or OFFSET in incrementally maintained views.

**Possible approaches:**
- Maintain a B-tree or sorted structure on the ORDER BY columns.
- For top-K queries, maintain K+buffer extra rows so that deletions from the top-K
  can be filled from the buffer.
- Accept O(K) maintenance cost per base table change.

### 2.8 Window Functions

Window functions partition data and compute aggregates within each partition, potentially
with ordering.

**PARTITION BY as grouping:**
The PARTITION BY clause creates groups similar to GROUP BY. The delta rule for the
partitioning itself is straightforward: route each delta tuple to the correct partition.

**Aggregate window functions (SUM OVER, COUNT OVER, AVG OVER):**
- For non-ordered (full-partition) aggregates: same as GROUP BY aggregates. Use the
  same delta rules as in Section 2.4.
- For ordered/running aggregates (e.g., SUM(x) OVER (PARTITION BY g ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)):
  Inserting or deleting a row with order value t affects the running sum for ALL rows
  with order value >= t in that partition. This can cause O(partition_size) changes.
- Feldera uses an auxiliary radix tree for efficient rolling aggregate computation.

**Ranking functions (ROW_NUMBER, RANK, DENSE_RANK):**
- ROW_NUMBER: Every insert/delete shifts the row numbers of all subsequent rows.
  Maintenance cost: O(partition_size) per change.
- RANK/DENSE_RANK: Similar issue. If a row with a particular rank value is inserted/deleted,
  all subsequent ranks shift.
- These are among the hardest operations to incrementalize efficiently.

**Current system support:**
- Databricks supports incremental refresh for window functions IF PARTITION BY is specified.
- Most other IVM systems do not support window functions.

### 2.9 Subqueries

#### 2.9.1 Correlated Subqueries

Correlated subqueries reference columns from an outer query. They are typically
decorrelated (rewritten as joins or semi-joins) before IVM delta rules are applied.

DBToaster decorrelates nested subqueries and then applies its higher-order delta processing.
DBSP handles correlated subqueries through its general framework by lifting the inner
query to operate on streams.

#### 2.9.2 EXISTS / NOT EXISTS

EXISTS is equivalent to a semi-join. NOT EXISTS is equivalent to an anti-join.
See Section 2.3.5 and 2.3.6 for their delta rules.

Key challenge: NOT EXISTS is non-monotone -- inserting into the subquery's source table
can cause deletions from the outer query's result.

#### 2.9.3 IN / NOT IN

IN (subquery) is equivalent to a semi-join on the subquery result.
NOT IN (subquery) is equivalent to an anti-join, with additional NULL complications:
- NOT IN returns UNKNOWN (not TRUE) if any value in the subquery result is NULL.
- This makes NOT IN even harder to incrementalize than NOT EXISTS.

**Best practice:** Rewrite IN/NOT IN as EXISTS/NOT EXISTS or semi-join/anti-join before
applying IVM rules.


## 3. Multiplicity / Z-Set Approaches

### 3.1 Integer Multiplicities (Z-sets, DBSP)

Each tuple t has an integer weight w(t) in Z:
- w(t) > 0: tuple is present with multiplicity w(t)
- w(t) = 0: tuple is absent
- w(t) < 0: represents a pending deletion (in a delta)

**Properties:**
- Z-sets form an abelian group under pointwise addition.
- Join is bilinear: w(t1 join t2) = w(t1) * w(t2).
- Union is addition: (R + S)(t) = R(t) + S(t).
- Difference is subtraction: (R - S)(t) = R(t) - S(t).
- The delta of any query is itself a Z-set (can have positive and negative weights).

**Advantages:**
- Uniform treatment of inserts and deletes.
- All operators compute on Z-sets, accepting both positive and negative changes.
- Order of applying deltas does not matter (commutativity of addition).
- Naturally handles batch updates (a batch is a Z-set with multiple non-zero entries).

### 3.2 Boolean Multiplicities (OpenIVM's Approach)

OpenIVM uses boolean multiplicities instead of integers:
- true = insertion (+1)
- false = deletion (-1)

**Advantages:**
- Simpler storage (1 bit per tuple instead of an integer).
- Sufficient for the common case where each delta represents a single insert or delete.
- Maps naturally to SQL (boolean column `_duckdb_ivm_multiplicity`).

**Limitations:**
- Cannot represent updates directly (must be decomposed into delete + insert).
- Cannot represent batch multiplicities (e.g., "insert 3 copies of this tuple").
- For aggregation, boolean multiplicity alone is insufficient -- the system pairs it
  with the aggregate value: (multiplicity_bool, aggregate_value).

### 3.3 Tradeoffs

| Aspect | Z-sets (integers) | Boolean multiplicity |
|--------|-------------------|---------------------|
| Space per tuple | 4-8 bytes (int32/64) | 1 bit |
| Batch updates | Native (weight = batch size) | Must enumerate |
| Updates (modify) | Single entry: -old + new | Two entries: false + true |
| Composition | Multiplicities multiply through joins | XOR-based for joins |
| Consolidation | Sum weights; cancel +1/-1 | Match true/false pairs |
| Aggregate compat | Natural (weight * value) | Needs pairing with value |

### 3.4 How Multiplicities Compose Through Operators

**Selection:** Multiplicity passes through unchanged. sigma_p(t, w) = (t, w) if p(t).

**Projection:** Under bag semantics, multiplicities pass through. Under DISTINCT, the
counting algorithm converts multiplicities to 0/1.

**Join:** Multiplicities multiply. If R has (t1, w1) and S has (t2, w2) where t1 and t2
join, the result has (t1 join t2, w1 * w2). For boolean multiplicities:
true * true = true (insert join insert = insert);
true * false = false (insert join delete = delete);
false * false = true (delete join delete = insert -- cancellation via XOR).

**Aggregation:** For SUM, the aggregate incorporates the multiplicity:
SUM(value * multiplicity) per group.

**Union:** Multiplicities add. (R + S)(t) = R(t) + S(t).

### 3.5 The Consolidation Step

Before applying a delta to the materialized view, it is often beneficial to
**consolidate** the delta: cancel out pairs of +1/-1 for the same tuple.

**Process:**
1. Group delta tuples by their key columns.
2. Sum multiplicities for each key.
3. Discard entries where the net multiplicity is 0 (insert + delete cancel out).
4. The remaining entries are the NET changes to apply.

**Benefits:**
- Reduces the number of view updates.
- Particularly valuable for batch deltas where the same tuple may be inserted and then
  deleted (or vice versa) within the same batch.
- OpenIVM implements this via EXCEPT ALL on the delta table before applying.

**With Z-sets:** Consolidation is simply summing the Z-set entries (automatic in the
algebraic framework).


## 4. Join Incrementalization Deep Dive

### 4.1 The T_old vs T_new Problem

The fundamental question: when computing the join delta, which version of the UNCHANGED
table should we use?

Consider D(R join S) when both R and S change:

**Using T_old everywhere (inclusion-exclusion):**
    DV = DR join S_old + R_old join DS + DR join DS
Requires 3 joins, but only needs pre-update states + deltas.

**Using T_new for one table:**
    DV = DR join S_new + R_old join DS    (or equivalently: DR join S_old + R_new join DS)
Requires 2 joins, but needs one table's post-update state.

**Key insight for implementation:** If the system uses triggers that fire AFTER the update
to a single table, the base table is already in its NEW state. So for single-table
updates, D(R join S) = DR join S_new is natural (S hasn't changed, so S_new = S_old = S).
For multi-table updates within a single transaction, the system must carefully manage
which table version is used.

### 4.2 The Double-Counting Problem

When expanding the product for multi-way joins, we must ensure each delta combination is
counted exactly once.

**Incorrect approach:**
    DV = DR join S + R join DS
This MISSES the DR join DS term and undercounts when both R and S have changes that
match each other.

**Incorrect approach (overcounting):**
    DV = DR join S_new + R_new join DS
This OVERCOUNTS because DR join DS is included in BOTH terms:
- DR join S_new includes DR join DS (since S_new = S_old + DS)
- R_new join DS includes DR join DS (since R_new = R_old + DR)

**Correct approaches:**
1. DR join S_old + R_old join DS + DR join DS (inclusion-exclusion: each subset counted once)
2. DR join S_new + R_old join DS (new-state on right: DR's contribution uses new S, R_old's contribution uses only DS)
3. DR join S_old + R_new join DS (new-state on left: symmetric)

### 4.3 Derivation for N-Way Joins

For V = R1 join R2 join ... join Rn, define Ri' = Ri + DRi (post-update state).

    DV = R1' join R2' join ... join Rn' - R1 join R2 join ... join Rn

Expand the product and cancel the original term:

    DV = SUM over all non-empty subsets T of {1,...,n}:
           (product of DRi for i in T) join (product of Rj for j not in T)

This gives 2^N - 1 terms. But using the telescoping trick:

    DV = SUM_{i=1}^{n} R1' join ... join R_{i-1}' join DRi join R_{i+1} join ... join Rn

This gives exactly N terms. Each term uses post-update states for tables 1..i-1, the delta
for table i, and pre-update states for tables i+1..n. This is correct because it
partitions the 2^N - 1 terms into N disjoint groups based on the "first" table (in the
fixed ordering) that has its delta included.

### 4.4 Efficiency: Small Delta, Large Table

The asymmetry |DR| << |S| is the key to IVM efficiency:
- DR join S requires probing S with each tuple in DR.
- With a hash index on S's join key: O(|DR| * avg_matches_per_key) time.
- Without index: O(|DR| * |S|) time (nested loop), which is no better than recompute.

**Indexes for efficient delta-join:**
- Hash index on join keys: O(1) lookup per probe. Best for equi-joins.
- B-tree on join keys: O(log n) lookup, supports range predicates.
- ART (Adaptive Radix Tree): O(k) lookup where k = key length. Used in DuckDB.
- Materialize's "arrangements": pre-indexed representations of collections, shared across queries.

### 4.5 Semijoin Reduction for Delta Propagation

Before joining DR with S, filter DR to only include tuples that have at least one join
partner in S. This is a semijoin: DR' = DR semi S.

**Benefits:**
- Reduces the size of DR before the expensive join.
- Particularly valuable when many delta tuples have no matches in S.

**Implementation options:**
- Bloom filter on S's join keys: approximate but fast. False positives are OK (just
  extra work), no false negatives.
- Semi-join with an index on S: exact, uses the same index as the eventual join.

### 4.6 Batch vs Single-Tuple Delta Processing

**Single-tuple deltas:**
- Each base table change processed immediately.
- DBToaster optimizes for this: O(1) per update for many query types.
- Low latency but high per-tuple overhead.

**Batch deltas:**
- Accumulate changes and process as a batch.
- Can be consolidated (cancel +1/-1) before processing.
- Join DR_batch with S once, rather than probing S once per tuple.
- Optimal batch size: 1,000-10,000 tuples in practice.
- Essential for distributed systems (amortizes network cost).

**Tradeoff:**
- Single-tuple: lower latency, higher amortized cost.
- Batch: higher latency, lower amortized cost, enables consolidation.


## 5. Aggregate Maintenance Deep Dive

### 5.1 The Counting Algorithm (Gupta & Mumick)

The standard approach for incrementally maintaining aggregated views with GROUP BY:

**Data structures:**
- The materialized view stores: (group_key, aggregate_value, __count__)
- __count__ tracks the number of base tuples contributing to each group.
- Additional hidden columns for auxiliary aggregate state (e.g., __sum__, __count_nonnull__
  for AVG).

**Algorithm for processing a delta tuple (key=g, value=v, multiplicity=m):**

1. Look up group g in the materialized view (using index on group key).
2. If group g EXISTS:
   a. Update aggregate: new_agg = combine(old_agg, v, m)
   b. Update count: new_count = old_count + m  (m is +1 for insert, -1 for delete)
   c. If new_count = 0: DELETE the row from the view (group is now empty).
   d. Else: UPDATE the row with new values.
3. If group g DOES NOT EXIST:
   a. If m > 0 (insertion): INSERT new row (g, agg(v), m).
   b. If m < 0 (deletion of non-existent group): this is an error or a no-op depending
      on the system's semantics.

**The `combine` function by aggregate type:**
- SUM: old_sum + (v * m)
- COUNT: old_count + m  (for COUNT(*))  or  old_count + m (for COUNT(col), only if v is non-NULL)
- MIN: if m > 0: min(old_min, v). If m < 0 and v = old_min: RECOMPUTE from base table.
- MAX: if m > 0: max(old_max, v). If m < 0 and v = old_max: RECOMPUTE from base table.
- AVG: update auxiliary SUM and COUNT, then divide.

### 5.2 Auxiliary Columns for Aggregate State

Different aggregates require different hidden columns in the materialized view:

| Aggregate | Hidden columns needed |
|-----------|----------------------|
| SUM(x)    | __count__ (to detect empty groups) |
| COUNT(x)  | (just the count itself) |
| AVG(x)    | __ivm_sum_avg__, __ivm_count_avg__ |
| STDDEV(x) | __count__, __sum__, __sum_sq__ |
| MIN(x)    | __count__ (no auxiliary for value -- recompute on delete) |
| MAX(x)    | __count__ (same as MIN) |

PostgreSQL's pg_ivm stores these as __ivm_count__, __ivm_sum_avg__, etc.

### 5.3 Decomposable vs Non-Decomposable Aggregates

**Distributive** (simplest): The aggregate of a union can be computed from the aggregates of
the parts. agg(A union B) = f(agg(A), agg(B)).
- SUM: SUM(A union B) = SUM(A) + SUM(B)
- COUNT: COUNT(A union B) = COUNT(A) + COUNT(B)
- MIN (insert-only): MIN(A union B) = min(MIN(A), MIN(B))
- MAX (insert-only): MAX(A union B) = max(MAX(A), MAX(B))

**Algebraic** (need fixed auxiliary state): Can be computed from a fixed number of
distributive sub-aggregates.
- AVG = SUM / COUNT (2 sub-aggregates)
- VARIANCE = (SUM_SQ / COUNT) - (SUM / COUNT)^2 (3 sub-aggregates: SUM, SUM_SQ, COUNT)
- STDDEV = sqrt(VARIANCE) (same 3 sub-aggregates)

**Holistic** (need unbounded auxiliary state): Cannot be computed from fixed-size state.
- MEDIAN: requires the entire sorted multiset per group.
- MODE: requires the full frequency distribution per group.
- PERCENTILE_CONT: requires the sorted multiset.

For holistic aggregates, incremental maintenance requires maintaining the full multiset
per group, with O(log n) update cost using a sorted structure.

### 5.4 The Problem of Empty Groups After Deletions

When all tuples in a group are deleted:
- COUNT reaches 0.
- SUM should become undefined (not 0), unless the query has a specific default.
- The group's row must be deleted from the materialized view.

**Detection:** The __count__ hidden column reaching 0 signals group emptiness.

**Implementation in OpenIVM:** After applying deltas, a cleanup step deletes rows where
the count is 0 (or where boolean multiplicity is false and there are no remaining
positive-multiplicity tuples).


## 6. Efficiency Techniques

### 6.1 Index-Based Delta Application

Use indexes on the materialized view to speed up delta application:
- **Hash index on GROUP BY keys:** O(1) lookup for aggregate updates.
- **Hash index on join keys:** O(1) lookup for joining deltas with base tables.
- **B-tree on ORDER BY keys:** O(log n) for ordered operations.
- In DuckDB/OpenIVM, ART indexes can be used on materialized tables.

### 6.2 Batch Delta Processing

Process multiple delta tuples together:
1. Consolidate the batch (cancel +1/-1 pairs).
2. Group by join key or group key.
3. Perform a single bulk join/update rather than per-tuple operations.
4. Benefits: amortized I/O, better cache behavior, enables vectorized execution.

### 6.3 Delta Consolidation / Compaction

Before applying deltas to the view:
1. Group delta tuples by their full key.
2. Sum multiplicities.
3. Remove entries with net multiplicity 0 (insert followed by delete of same tuple).
4. This reduces the number of view updates.

OpenIVM implements this using EXCEPT ALL: net_inserts = inserts EXCEPT ALL deletes.

### 6.4 Predicate Filtering (Predicate Pushdown)

If the view definition includes a filter sigma_p, push it into the delta computation:
- Only process delta tuples that satisfy p.
- Skip all delta processing for tuples that would be filtered out anyway.
- This is free (just apply the predicate to the delta) and can dramatically reduce work.

### 6.5 Semijoin Reduction

Filter deltas that have no join partners before the actual join:
- Compute DR' = DR semi S (only keep delta tuples that have matches in S).
- Then compute DR' join S.
- Reduces the effective delta size, especially when many delta tuples are "irrelevant"
  (no join partners).

### 6.6 Lazy vs Eager Maintenance

**Eager maintenance:**
- Update the view immediately when the base table changes (in the same transaction).
- Pro: view is always up-to-date; queries are free.
- Con: every update pays the maintenance cost; can slow down OLTP.

**Lazy (deferred) maintenance:**
- Save the delta but don't apply it to the view immediately.
- Apply accumulated deltas when the view is next queried (or on a schedule).
- Pro: updates are fast; maintenance cost is batched.
- Con: view may be stale; query latency includes maintenance time.

**When to prefer each:**
- Eager: low update frequency, high query frequency, low tolerance for staleness.
- Lazy: high update frequency, lower query frequency, tolerance for some staleness.

### 6.7 The "Small Delta, Large Table" Optimization

IVM's primary advantage comes from the asymmetry |delta| << |table|:
- Join: probe a small delta against a large indexed table = O(|delta| * selectivity).
- Aggregation: update O(|affected_groups|) rows instead of scanning the whole table.
- The speedup factor is approximately O(|T| / |DT|), which can be orders of magnitude.

### 6.8 Materialization of Intermediate Results (Higher-Order IVM)

**DBToaster's viewlet transform:**
Instead of materializing only the final view, also materialize intermediate subexpressions
(auxiliary views) that speed up delta processing.

**Example:** For V = SUM_{A,B,C} R(A,B) * S(B,C) * T(C,A):
- First-order delta (when R changes): DV = SUM_{A,B,C} DR(A,B) * S(B,C) * T(C,A)
  This still requires joining S and T, which is expensive.
- Higher-order approach: materialize V_ST(B,A) = SUM_C S(B,C) * T(C,A)
  Then: DV = SUM_{A,B} DR(A,B) * V_ST(B,A) -- just one lookup per delta tuple!
- But V_ST also needs to be maintained when S or T changes.
- The delta of V_ST when S changes: DV_ST = SUM_C DS(B,C) * T(C,A) -- also just a lookup.

**The recursion terminates** because each level of delta is simpler (one fewer relation in
the join). For a k-way join, the k-th order delta involves no joins at all -- it is purely
a function of the update tuple.

**When is higher-order IVM worth it?**
- For multi-way joins (3+ tables): the auxiliary views eliminate join processing during updates.
- The tradeoff: auxiliary views require O(N^2) space (roughly) but make updates O(1) time.
- Best for workloads with frequent small updates and complex multi-way joins.

### 6.9 Factorized Representations (F-IVM)

F-IVM (Olteanu et al.) uses factorized representations to avoid redundancy in the
materialized view:

**Key ideas:**
- The query result is stored in a view tree rather than a flat table.
- Each node in the tree corresponds to a variable, and the factorization makes explicit
  the independence between variables.
- Example: for Q(Y,X,Z) = R(Y,X) * S(Y,Z), the factorized form is:
  V(Y) = V_R(Y) * V_S(Y), where V_R(Y) = SUM_X R(Y,X), V_S(Y) = SUM_Z S(Y,Z)
- Updates propagate bottom-up with O(1) cost per view.

**Performance:**
- For q-hierarchical queries: O(1) update time, O(1) enumeration delay.
- F-IVM can outperform classical first-order IVM and DBToaster by orders of magnitude
  while using less memory.

**Complexity classification:**
- Q-hierarchical queries: admit O(1) update time and O(1) enumeration delay.
- Non-q-hierarchical queries: require at least O(N^(1/2)) update time (conditional
  lower bound via Online Matrix-Vector Multiplication conjecture).
- The heavy/light partitioning strategy achieves O(N^(1/2)) for the triangle query.


## 7. Correctness Considerations

### 7.1 Bag Semantics vs Set Semantics

**Bag semantics (multisets):** Most SQL operations use bag semantics by default (without
DISTINCT). Delta rules are simpler because multiplicities compose naturally:
- Join: multiply multiplicities.
- Union: add multiplicities.
- Projection: pass through multiplicities.

**Set semantics (with DISTINCT):** Requires the counting algorithm to detect when a tuple's
net multiplicity crosses the 0 boundary:
- 0 -> positive: insert into view.
- positive -> 0: delete from view.
- positive -> positive: no change to view (multiplicity changed but tuple is still present).

**Which rules apply where:** Most delta rules are defined for bag semantics. Set semantics
is handled by wrapping the result in a DISTINCT operator with the counting algorithm.

### 7.2 Order of Inserts and Deletes

**With Z-sets (integer multiplicities):** Order does NOT matter. Z-set addition is
commutative and associative. Applying deltas in any order gives the same result.

**With traditional (non-ring) approaches:** Order CAN matter:
- If we process a delete before an insert of the same tuple, we might try to delete
  a tuple that doesn't exist yet.
- If we process an insert before a delete, we might temporarily have an extra copy.

**Best practice:** Consolidate deltas first (cancel +1/-1 pairs) to avoid ordering issues.
Or use the Z-set framework where ordering is irrelevant.

### 7.3 NULL Handling

**In join predicates:** NULL = NULL evaluates to UNKNOWN (not TRUE) in SQL.
- Standard equi-join: NULL keys never match. This is correct for IVM -- delta tuples
  with NULL join keys will produce no matches.
- IS NOT DISTINCT FROM: NULLs match NULLs. If the join uses this predicate, the index
  must handle NULL equality.

**In aggregates:** See Section 2.4.9. Maintain auxiliary counts of non-NULL values.

**In DISTINCT:** Two NULLs are considered equal for DISTINCT purposes (unlike joins).
The counting algorithm must group NULLs together.

**In set operations:** NULLs are treated as equal for UNION, INTERSECT, EXCEPT.

### 7.4 Transaction Isolation

**Immediate maintenance (same transaction):**
- The view is updated within the same transaction that modifies the base table.
- Consistency: the view always reflects the committed state.
- Implementation: use AFTER triggers; the base table is already in its new state.
- Concurrency: take a lock on the materialized view early to serialize concurrent updates.

**PostgreSQL's approach:**
- READ COMMITTED: take an exclusive lock on the materialized view, wait for concurrent
  transactions, then re-read the latest snapshot.
- REPEATABLE READ / SERIALIZABLE: raise an error if concurrent modifications are detected,
  because the view cannot be maintained correctly under these isolation levels with AFTER triggers.

**Deferred maintenance (separate transaction):**
- The view is updated later, potentially seeing a different snapshot.
- Must use a consistent snapshot that includes all changes since the last refresh.
- Simpler concurrency (no contention during base table updates) but introduces staleness.

### 7.5 Concurrent Updates During Maintenance

**Problem:** If base table T is modified while DV is being computed from a previous DT,
the view may become inconsistent.

**Solutions:**
1. Lock the view during maintenance (serialization -- safe but limits concurrency).
2. Use MVCC: compute DV against a specific snapshot, apply atomically.
3. Use versioned deltas: track which deltas have been applied and ensure exactly-once processing.


## 8. The Adaptive Question: IVM vs Full Recompute

### 8.1 When is IVM Cheaper?

IVM is cheaper when: Cost(DV from DT) < Cost(recompute V from scratch).

**Cost model factors:**
- |DT| / |T| ratio: the smaller the delta relative to the table, the bigger the advantage.
- Join selectivity: high-selectivity joins amplify the IVM advantage (most delta tuples
  match few rows).
- Number of affected groups: for aggregation, cost proportional to affected groups, not
  total groups.
- View complexity: multi-way joins with high fan-out may make IVM expensive even for small deltas.
- Index availability: without indexes, IVM may degrade to full-table scans.

**Rules of thumb:**
- IVM wins when |DT| / |T| < 10% (rough threshold, query-dependent).
- IVM loses when the delta is large relative to the base data, or when the view is simple
  (e.g., just a filter on a single table -- full recompute is already cheap).
- IVM is most valuable for complex queries (multi-way joins, aggregations) on large tables
  with small, frequent updates.

### 8.2 Databricks Adaptive Approach

Databricks implements a cost-based decision:
- AUTO mode: runs a cost analysis to choose between incremental refresh and full recompute.
- Considers query structure, data change volume, and system cost modeling.
- Falls back to full recompute at runtime if the changeset size makes it cheaper.
- Users can override with REFRESH POLICY INCREMENTAL (prefer incremental) or
  REFRESH POLICY FULL (always recompute).

### 8.3 The Hybrid Approach

Use IVM for some operators and recompute for others within the same query:
- Apply IVM for the expensive parts (multi-way joins, large aggregations).
- Recompute for cheap parts (simple filters, small tables).
- This requires the query optimizer to estimate costs per operator and decide the
  maintenance strategy for each subexpression.


## 9. Key References

| Reference | Year | Contribution |
|-----------|------|-------------|
| Ceri & Widom | 1991 | Deriving production rules for IVM from SQL view definitions |
| Gupta, Mumick, Subrahmanian | 1993 | Counting algorithm for maintaining views with duplicates |
| Griffin & Libkin | 1995 | Incremental maintenance of views with duplicates (bag semantics) |
| Gupta & Mumick | 1995/1999 | Survey: "Maintenance of Materialized Views: Problems, Techniques, and Applications" |
| Palpanas et al. | 2002 | Incremental maintenance for non-distributive aggregates (MIN, MAX, STDDEV) |
| Gupta (H.) | 2006 | Incremental maintenance of aggregate and outerjoin expressions |
| Larson & Zhou | 2007 | Efficient maintenance of materialized outer-join views |
| Koch et al. | 2012/2014 | DBToaster: Higher-order delta processing, viewlet transform |
| Koch | 2016 | IVM for collection programming (nested relational calculus on bags) |
| Nikolic & Olteanu | 2018 | F-IVM: Incremental view maintenance with triple lock factorization benefits |
| Budiu et al. | 2023 | DBSP: Automatic IVM for rich query languages (Z-sets, streams) |
| Olteanu et al. | 2024 | "Recent Increments in IVM" survey (complexity, factorized representations) |
| OpenIVM | 2024 | SQL-to-SQL compiler for incremental computations (DuckDB extension) |


## 10. Related Systems

| System | Approach | Key Feature |
|--------|----------|-------------|
| **DBToaster** | Higher-order IVM | Recursive delta compilation, auxiliary views, O(1) updates for many queries |
| **DBSP / Feldera** | Algebraic (Z-sets) | Streams, uniform treatment of all operators, incremental circuits |
| **pg_ivm** | Trigger-based | PostgreSQL extension, counting algorithm, Larson-Zhou outer joins |
| **Snowflake** | Micro-partition tracking | Dynamic tables, automatic background maintenance |
| **Materialize** | Dataflow (Timely/DD) | Shared arrangements, delta joins, zero intermediate state |
| **Noria** | Dataflow | Partially-stateful operators, eviction, upquery |
| **Databricks** | Enzyme engine | Adaptive IVM vs recompute, supports joins + window functions |
| **F-IVM** | Factorized higher-order | View trees, q-hierarchical queries, O(1) update for tractable class |
| **OpenIVM** | SQL-to-SQL compilation | DuckDB extension, boolean multiplicities, DBSP-inspired |
