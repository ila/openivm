---
name: write-docs
description: Style guide for writing OpenIVM documentation. Auto-loaded when writing, editing, or reviewing docs, README, user guides, or SQL reference pages. Blends Snowflake's structured clarity with DuckDB's concise, example-first approach.
---

# OpenIVM Documentation Style Guide

This guide defines how to write documentation for OpenIVM. The style blends
**Snowflake's structured clarity** (hierarchical sections, callout boxes,
thorough parameter docs) with **DuckDB's concise, example-first approach**
(lead with code, short sentences, practical tone).

---

## 1. Core Principles

1. **Lead with examples.** Show what it does before explaining how it works.
   DuckDB puts Examples before Syntax. Follow that: example first, reference second.
2. **One idea per sentence.** Keep sentences to 15-25 words. Break longer thoughts
   into two sentences. Never nest more than one subordinate clause.
3. **Address the reader as "you".** Not "the user" or "one". Active voice, imperative
   mood for instructions: "Create a view" not "A view can be created".
4. **No filler.** Cut "In order to", "It should be noted that", "Please note that".
   Just state the fact.
5. **Be precise about behavior.** Don't say "may" when you mean "does". Don't say
   "should" when you mean "must". Reserve "may" for genuine uncertainty.
6. **Document every feature and every known limitation.** If a feature exists, it must
   appear in the docs. If a limitation exists, it must appear in a Limitations section.
   Undocumented features and silent limitations are bugs. When you add or change a
   feature in code, update the corresponding docs page in the same commit.

---

## 2. Page Structure

### 2.1 Conceptual / User Guide Pages

Follow Snowflake's overview page pattern:

```
# Page Title                          ← H1: noun phrase, no verb
                                      ← 2-3 sentence intro: what this is + why it matters
## How It Works                       ← H2: explain the mechanism
### Sub-concept                       ← H3: one per distinct idea
## When to Use This                   ← H2: use cases, decision criteria
## Limitations                        ← H2: always present, even if short
## Examples                           ← H2: 2-4 progressively complex examples
## See Also                           ← H2: links to related pages
```

**Intro paragraph:** State what the feature is and what problem it solves.
Two to three sentences maximum. No history, no motivation essay.

```
Good: "Delta tables track changes to base tables. OpenIVM creates them
automatically when you define a materialized view."

Bad:  "In the field of incremental view maintenance, it has long been
recognized that tracking changes is essential. To this end, OpenIVM
provides a mechanism called delta tables, which we will now describe."
```

### 2.2 SQL Reference Pages

Follow DuckDB's statement page pattern, with Snowflake's parameter rigor:

```
# STATEMENT NAME                      ← H1: the SQL command
                                      ← 1-2 sentence description
## Examples                           ← H2: FIRST, before syntax
## Syntax                             ← H2: formal syntax block
## Parameters                         ← H2: every parameter documented
### Required Parameters               ← H3: if many params
### Optional Parameters               ← H3: if many params
## Usage Notes                        ← H2: behavioral details, edge cases
## Limitations                        ← H2: what doesn't work
## See Also                           ← H2: related commands
```

### 2.3 Function / Pragma Reference Pages

Follow DuckDB's aggregate function pattern:

```
# Function Category                   ← H1
                                      ← Brief overview sentence
## Examples                           ← H2: common usage patterns
## Function List                      ← H2: table with Name | Description | Example
## Detailed Reference                 ← H2: per-function details if needed
### function_name(params)             ← H4: one heading per function
```

---

## 3. Syntax Blocks

Use fenced SQL code blocks. Follow Snowflake's notation for formal syntax:

```sql
CREATE MATERIALIZED VIEW <view_name> AS
    <select_statement>
```

**Notation conventions:**
- `<placeholder>` — user-supplied value (angle brackets)
- `[ optional ]` — optional clause (square brackets)
- `{ choice1 | choice2 }` — choose one (braces + pipe)
- `...` — repeatable element
- Keywords in UPPERCASE, identifiers in lowercase

**For informal examples** (the Examples section), use real values, not placeholders:

```sql
CREATE MATERIALIZED VIEW monthly_sales AS
    SELECT product, SUM(amount) AS total, COUNT(*) AS cnt
    FROM orders
    GROUP BY product;
```

---

## 4. Parameter Documentation

Use Snowflake's definition-list style. Each parameter gets:

1. **Name** in backtick code font
2. **Description** — what it does, in one sentence
3. **Type** and **Default** — on their own line if applicable
4. **Constraints** — what values are valid

Format:

```markdown
`view_name`

The name of the materialized view to create.

`select_statement`

The SQL query that defines the view contents. Must be a SELECT statement.
Supported: projections, filters, GROUP BY, SUM, COUNT, inner joins.
Not supported: outer joins, window functions, DISTINCT, LIMIT.

`ivm_refresh_mode`

Controls how the view is refreshed.

- `'auto'` — Choose incremental or full based on cost model. **Default.**
- `'incremental'` — Always use incremental maintenance.
- `'full'` — Always recompute from scratch.
```

---

## 5. Examples

### 5.1 Structure

Every page with examples must follow this pattern:

1. **Start simple.** First example: the most common, minimal use case.
2. **Build up.** Each subsequent example adds one concept (filter, join, config option).
3. **Show setup AND result.** Include the CREATE, INSERT, PRAGMA, and SELECT output.
4. **Use realistic data.** Not `foo/bar/baz`. Use `orders`, `products`, `employees`.

### 5.2 Format

```sql
-- Create a materialized view with a grouped aggregate
CREATE MATERIALIZED VIEW product_totals AS
    SELECT product, SUM(amount) AS total
    FROM orders
    GROUP BY product;

-- Insert data into the base table
INSERT INTO orders VALUES ('Widget', 100), ('Gadget', 200), ('Widget', 50);

-- Check the materialized view
SELECT * FROM product_totals;
```

```text
┌─────────┬───────┐
│ product │ total │
│ varchar │ int32 │
├─────────┼───────┤
│ Gadget  │   200 │
│ Widget  │   150 │
└─────────┴───────┘
```

```sql
-- Insert more data and refresh incrementally
INSERT INTO orders VALUES ('Widget', 75);
PRAGMA ivm('product_totals');

SELECT * FROM product_totals;
```

```text
┌─────────┬───────┐
│ product │ total │
│ varchar │ int32 │
├─────────┼───────┤
│ Gadget  │   200 │
│ Widget  │   225 │
└─────────┴───────┘
```

### 5.3 SQL Comment Style

- Use `--` comments, not `/* */`
- Comment above the statement, not inline
- Describe *what* and *why*, not the SQL syntax itself
- Start with a verb: "Create...", "Insert...", "Refresh..."

```sql
-- Good:
-- Refresh the view after inserting new orders
PRAGMA ivm('product_totals');

-- Bad:
PRAGMA ivm('product_totals'); -- this calls the ivm pragma with the view name
```

---

## 6. Callout Boxes

Use Markdown blockquote callouts. Three types, used sparingly:

### Note — Additional context that helps understanding

```markdown
> **Note:** Delta tables are created automatically. You do not need to
> create them manually.
```

Use when: the reader might otherwise miss a helpful detail.

### Important — Something that can cause incorrect results if ignored

```markdown
> **Important:** The multiplicity column must be BOOLEAN. Using INTEGER
> multiplicities will produce incorrect aggregate results.
```

Use when: ignoring this causes silent data corruption or wrong results.

### Tip — A non-obvious efficiency or convenience trick

```markdown
> **Tip:** Set `ivm_adaptive_refresh = true` to let OpenIVM automatically choose
> between incremental refresh and full recompute based on delta size.
```

Use when: there's a better way that the reader might not discover on their own.

**Rules:**
- Maximum 2-3 callouts per page. More than that means the prose needs rewriting.
- Never put critical information *only* in a callout. The main text must stand alone.
- Never stack two callouts back-to-back.

---

## 7. Tables

Use tables for structured reference data. Follow DuckDB's function table pattern.

### 7.1 When to Use Tables

- **Function/pragma lists**: Name | Description | Example
- **Parameter options**: Value | Behavior
- **Feature comparison**: Feature | Supported? | Notes
- **Configuration options**: Setting | Type | Default | Description

### 7.2 When NOT to Use Tables

- Explanations of how something works (use prose)
- Step-by-step instructions (use numbered lists)
- Anything with more than 5 columns (too wide)

### 7.3 Format

```markdown
| Setting | Type | Default | Description |
|---|---|---|---|
| `ivm_refresh_mode` | VARCHAR | `'auto'` | Refresh strategy |
| `ivm_adaptive_refresh` | BOOLEAN | `false` | Enable adaptive cost model |
```

- Left-align text columns, right-align numbers
- Use backticks for code values in cells
- Keep descriptions to one line

---

## 8. Cross-References

### 8.1 "See Also" Sections

Place at the bottom of every page. List 3-7 related pages:

```markdown
## See Also

- [CREATE MATERIALIZED VIEW](create-materialized-view.md) — Define a new materialized view
- [PRAGMA ivm](pragma-ivm.md) — Refresh a materialized view incrementally
- [Delta Tables](delta-tables.md) — How change tracking works
```

Format: `[Page Title](link) — one-line description`.

### 8.2 Inline References

Use sparingly in prose. Link on first mention only:

```markdown
OpenIVM tracks changes in [delta tables](delta-tables.md).
Subsequent mentions of delta tables are not linked.
```

---

## 9. Limitations Sections

Every feature page must have a Limitations section. Follow Snowflake's pattern:

```markdown
## Limitations

- Only inner joins are supported. Outer, cross, and semi joins are not
  incrementally maintained.
- MIN and MAX aggregates require group recomputation on delete.
  SUM and COUNT are fully incremental.
- Maximum 16 tables in a single join (`ivm::MAX_JOIN_TABLES`).
- Views using window functions, DISTINCT, or LIMIT are classified as
  `FULL_REFRESH` and always recomputed.
```

**Rules:**
- Use a bulleted list, one limitation per bullet
- State what is NOT supported, then (optionally) what happens instead
- Don't apologize or promise future fixes
- Order by importance (most impactful first)

---

## 10. Writing Checklist

Before publishing any documentation page, verify:

- [ ] First section is Examples (for reference pages) or a 2-3 sentence intro (for guides)
- [ ] Every code block uses `sql` or `text` language tags
- [ ] Every example shows both the command AND the result
- [ ] Callouts are Note/Important/Tip only, max 3 per page
- [ ] Limitations section exists and is accurate
- [ ] See Also section has 3-7 links
- [ ] No sentence exceeds 30 words
- [ ] "You/your" used, never "the user" or "one"
- [ ] All parameters documented with type, default, and valid values
- [ ] Syntax block uses `<placeholder>`, `[ optional ]`, `{ choice | choice }` notation
- [ ] No filler phrases ("In order to", "It should be noted", "Please note")
- [ ] Active voice throughout ("Create a view", not "A view is created")

---

## 11. File Organization

The project has two documentation locations:

**`README.md`** (project root) — The landing page. Contains:
- One-paragraph project description
- Feature highlights (bulleted)
- Quick-start example (create view, insert, refresh, select)
- Build instructions
- Link to `docs/` for detailed documentation

**`docs/`** — Detailed documentation, organized by topic:

```
docs/
├── getting-started.md          ← Quick start guide (install, first MV, first refresh)
├── user-guide/
│   ├── materialized-views.md   ← Conceptual: what MVs are, how they work
│   ├── delta-tables.md         ← Conceptual: change tracking
│   ├── refresh-modes.md        ← Conceptual: auto/incremental/full
│   └── cost-model.md           ← Conceptual: adaptive decisions
├── sql-reference/
│   ├── create-materialized-view.md
│   ├── pragma-ivm.md
│   ├── pragma-ivm-cost.md
│   └── configuration.md
└── limitations.md              ← Global limitations page
```

**Rules:**
- Each docs page must be self-contained: a reader landing from search should
  understand it without reading other pages first.
- The README must stay concise (under 150 lines). Detailed content goes in `docs/`.
- When you add a feature, add or update the corresponding page in `docs/`.
- When you discover a limitation, add it to `docs/limitations.md` AND to the
  relevant feature page's Limitations section.
