#!/usr/bin/env python3
"""Measure pipeline refresh latency/storage and verify every result as a bag.

Run old and new binaries separately on an otherwise idle machine, using the
same arguments. Timings cover PRAGMA refresh_pipeline; setup, ingestion and
verification are outside the timer. A correctness failure aborts the run.
"""
import argparse
import csv
import io
import json
from pathlib import Path
import re
import statistics
import subprocess
import tempfile


def bag_check(view, query):
    expected = f"SELECT * FROM ({query}) expected"
    return (
        f"SELECT 'CHECK', count(*) FROM ((SELECT * FROM {view} EXCEPT ALL {expected}) "
        f"UNION ALL ({expected} EXCEPT ALL SELECT * FROM {view}));\n"
    )


def workload(rows, changes, shape, batches, threads):
    sql = (
        f"SET threads={threads}; SET openivm_disable_daemon=true; "
        "SET openivm_cascade_refresh='both';\n"
        f"CREATE TABLE t AS SELECT i::INTEGER k, (i%101)::BIGINT v FROM range({rows}) x(i);\n"
    )
    parent = "SELECT k,SUM(v) v FROM t GROUP BY k"
    if shape == "having":
        parent += " HAVING SUM(v)>50"
    elif shape == "topk":
        parent += " ORDER BY v DESC,k LIMIT 1000"
    sql += f"CREATE MATERIALIZED VIEW p AS {parent};\n"
    sql += "CREATE MATERIALIZED VIEW c AS SELECT k,v*2 v FROM p;\n"
    targets = ["c"]
    checks = {"p": parent, "c": f"SELECT k,v*2 v FROM ({parent}) q"}
    if shape == "fanout":
        for index in (2, 3):
            name = f"c{index}"
            sql += f"CREATE MATERIALIZED VIEW {name} AS SELECT k,v*{index+1} v FROM p;\n"
            targets.append(name)
            checks[name] = f"SELECT k,v*{index+1} v FROM ({parent}) q"
    else:
        sql += "CREATE MATERIALIZED VIEW d AS SELECT k,v+1 v FROM c;\n"
        targets = ["d"]
        checks["d"] = f"SELECT k,v*2+1 v FROM ({parent}) q"
    for _ in range(batches):
        sql += (
            f"INSERT INTO t SELECT i::INTEGER,7 FROM range({changes}) x(i);\n"
            f"UPDATE t SET v=v+2 WHERE k<{changes} AND k%2=0;\n"
            f"DELETE FROM t WHERE k<{changes} AND v=7 AND k%3=0;\n"
        )
        quoted_targets = ",".join(f"'{target}'" for target in targets)
        sql += f".timer on\nPRAGMA refresh_pipeline({quoted_targets});\n.timer off\n"
        sql += "".join(bag_check(view, query) for view, query in checks.items())
    return sql + "CHECKPOINT;\n", len(checks) * batches


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cli", type=Path)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--changes", type=int, nargs="+", default=[20, 10_000])
    parser.add_argument("--shapes", nargs="+", choices=["chain", "fanout", "having", "topk"],
                        default=["chain", "fanout", "having", "topk"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--batches", type=int, default=4)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    cli = args.cli.resolve()
    root = args.output or Path(tempfile.mkdtemp(prefix="openivm-publication-"))
    root.mkdir(parents=True, exist_ok=True)
    root = root.resolve()
    results = []
    for shape in args.shapes:
        for changes in args.changes:
            for repeat in range(args.repeats):
                database = root / f"{shape}-{changes}-{repeat}.db"
                if database.exists():
                    raise FileExistsError(f"Use a fresh output directory: {database}")
                sql, expected_checks = workload(args.rows, changes, shape, args.batches, args.threads)
                database.with_suffix(".sql").write_text(sql)
                process = subprocess.run(
                    [str(cli), str(database), "-csv", "-batch", "-bail"], input=sql,
                    text=True, capture_output=True, cwd=root, timeout=300,
                )
                output = process.stdout + "\nSTDERR\n" + process.stderr
                database.with_suffix(".log").write_text(output)
                if process.returncode:
                    raise RuntimeError(f"SQL failed; see {database.with_suffix('.log')}")
                checks = [row for row in csv.reader(io.StringIO(process.stdout)) if row and row[0] == "CHECK"]
                if len(checks) != expected_checks or any(row[1] != "0" for row in checks):
                    raise AssertionError(f"Bag equality failed: {database.with_suffix('.log')}")
                times = [float(value) for value in re.findall(r"Run Time \(s\): real ([0-9.]+)", output)]
                if len(times) != args.batches:
                    raise AssertionError(f"Missing timings: {database.with_suffix('.log')}")
                result = dict(shape=shape, changes=changes, repeat=repeat, seconds=times,
                              rows=args.rows, threads=args.threads, bytes=database.stat().st_size)
                results.append(result)
                (root / "results.json").write_text(json.dumps(results, indent=2))
                print(f"{shape} changes={changes} repeat={repeat}: "
                      f"median={statistics.median(times):.3f}s bytes={result['bytes']}", flush=True)
    print(f"Artifacts: {root}")


if __name__ == "__main__":
    main()
