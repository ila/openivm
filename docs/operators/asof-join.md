# ASOF JOIN

OpenIVM maintains views that use `ASOF JOIN` — a "join to the most recent matching row" used for
time-series alignment (e.g. enrich each trade with the quote in effect at that moment).

```sql
CREATE TABLE trades(sym VARCHAR, ts INT, price INT);
CREATE TABLE quotes(sym VARCHAR, ts INT, bid INT);
INSERT INTO trades VALUES ('A', 10, 100), ('A', 20, 110);
INSERT INTO quotes VALUES ('A', 5, 99),  ('A', 15, 108);

CREATE MATERIALIZED VIEW tq AS
  SELECT t.sym, t.ts, t.price, q.bid
  FROM trades t ASOF JOIN quotes q
  ON t.sym = q.sym AND t.ts >= q.ts;

INSERT INTO trades VALUES ('A', 30, 120);
INSERT INTO quotes VALUES ('A', 25, 115);
DELETE FROM trades WHERE ts = 10;
PRAGMA refresh('tq');
-- ('A',20,110,108), ('A',30,120,115)
```

## How it's maintained

An ASOF match depends on the *ordering* of the right side: which quote a trade pairs with can
change when a row is inserted or deleted between them. Because of that, OpenIVM does not try to
patch individual matched rows. Instead, **it recomputes only the rows affected by the change** —
the keys touched by the delta are re-evaluated against the current data and the view rows for
those keys are replaced. Unaffected keys are left untouched, so a refresh still costs far less
than recomputing the whole view.

## Caveats

- Because matching is order-sensitive, a single insert or delete on the probe side can shift which
  right-side row a key pairs with; the affected scope is recomputed to keep the result exact.
- Standard ASOF semantics apply: a probe row with no qualifying match produces no output row
  (inner ASOF) — use the outer form if you need NULL-padding.
