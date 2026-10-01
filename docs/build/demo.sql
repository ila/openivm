-- Run from the repository root; see building.md.
SET openivm_files_path = 'openivm_generated_sql';

CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INT);
INSERT INTO sales VALUES ('US', 'Widget', 100), ('EU', 'Gadget', 200);

CREATE MATERIALIZED VIEW regional_totals AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
    FROM sales GROUP BY region;

-- Batch changes before refreshing once.
INSERT INTO sales VALUES ('US', 'Bolt', 50), ('JP', 'Gear', 300);
UPDATE sales SET amount = 110 WHERE product = 'Widget';
DELETE FROM sales WHERE product = 'Gadget';
PRAGMA refresh('regional_totals');

SELECT * FROM regional_totals ORDER BY region;
-- JP | 300 | 1
-- US | 160 | 2

-- Both checks must return zero rows, including when duplicates are present.
SELECT * FROM regional_totals
EXCEPT ALL
SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region;

SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region
EXCEPT ALL
SELECT * FROM regional_totals;
