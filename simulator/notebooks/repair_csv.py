#!/usr/bin/env python3
"""Fix sim_res/benchmark_results.csv where Hungarian-locale JVM wrote
decimals with commas, splitting 3 float columns into 6 fields.

Each row has 12 fields instead of 9. Layout:
   wf, sched, seed, ms_int, ms_frac, e_int, e_frac, total, nd, fd, inf_int, inf_frac
"""
from pathlib import Path

CSV = Path(__file__).resolve().parent.parent / "sim_res" / "benchmark_results.csv"

lines = CSV.read_text().splitlines()
header = lines[0]
out = [header]
fixed = 0
for line in lines[1:]:
    parts = line.split(",")
    if len(parts) == 9:
        out.append(line)                            # already fine
        continue
    if len(parts) != 12:
        print(f"⚠ skipping malformed row ({len(parts)} fields): {line[:80]}")
        continue
    wf, sched, seed = parts[0], parts[1], parts[2]
    ms = f"{parts[3]}.{parts[4]}"
    e  = f"{parts[5]}.{parts[6]}"
    total, nd, fd = parts[7], parts[8], parts[9]
    inf = f"{parts[10]}.{parts[11]}"
    out.append(f"{wf},{sched},{seed},{ms},{e},{total},{nd},{fd},{inf}")
    fixed += 1

CSV.write_text("\n".join(out) + "\n")
print(f"repaired {fixed} rows; wrote {CSV}")
