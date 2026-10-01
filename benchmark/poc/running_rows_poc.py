"""Compare refresh binaries using ROWS windows; verify every batch."""

import argparse
import json
import pathlib
import re
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before", help="Baseline binary; affected-partition maintenance unless --before-suffix is set")
    parser.add_argument("after", help="Binary with ROWS suffix maintenance")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--before-suffix", action="store_true", help="Enable suffix maintenance in the baseline")
    parser.add_argument("--partitions", type=int, default=1)
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("--partitions must be positive")
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    sql_path = str(root).replace("'", "''")
    query = (
        "SELECT k,d,x,"
        "SUM(x) OVER (PARTITION BY k ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s,"
        "COUNT(x) OVER (PARTITION BY k ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n FROM t"
    )
    results = []
    binaries = [
        ("before_suffix" if args.before_suffix else "before_partition", args.before, str(args.before_suffix).lower()),
        ("after_suffix", args.after, "true"),
    ]
    for size, delta in [(100000, 20), (100000, 10000), (1000000, 20)]:
        for label, binary, enabled in binaries:
            for repeat in range(3):
                sql = (
                    ".bail on\nSET threads=4; SET openivm_disable_daemon=true; "
                    f"SET openivm_files_path='{sql_path}'; "
                    f"SET openivm_running_window_incremental={enabled}; "
                    f"CREATE TABLE t AS SELECT 1+i%{args.partitions} k, i//2 d,(i//2)%17-8 x FROM range({size}) r(i); "
                    f"CREATE MATERIALIZED VIEW m AS {query};\n"
                )
                for batch in range(3):
                    start = size + batch * delta
                    sql += (
                        f"INSERT INTO t SELECT 1+i%{args.partitions},i//2,(i//2)%17-8 FROM range({start},{start+delta}) r(i);\n"
                        ".timer on\nPRAGMA refresh('m');\n.timer off\n"
                        f"SELECT CASE WHEN EXISTS ((SELECT * FROM m EXCEPT ALL {query}) "
                        f"UNION ALL ({query} EXCEPT ALL SELECT * FROM m)) "
                        "THEN error('bag mismatch') ELSE 'bag equal' END;\n"
                    )
                stem = f"{size}-{delta}-{label}-{repeat}"
                (root / f"{stem}.sql").write_text(sql)
                process = subprocess.run(
                    [binary, "-batch", "-init", "/dev/null"], input=sql, capture_output=True, text=True
                )
                output = process.stdout + process.stderr
                (root / f"{stem}.log").write_text(output)
                if process.returncode:
                    raise RuntimeError(f"{stem}: {process.stderr}")
                times = [float(x) for x in re.findall(r"Run Time \(s\): real ([0-9.]+)", output)]
                if len(times) != 3:
                    raise RuntimeError(f"{stem}: expected three refresh timings, got {times}")
                results.append(
                    dict(size=size, delta=delta, partitions=args.partitions, label=label, repeat=repeat, seconds=times)
                )
                print(size, delta, label, repeat, statistics.median(times), flush=True)
                (root / "results.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
