#!/usr/bin/env python3
"""
measure_total_runtime.py

Runs full MPI-based portfolio optimization pipeline and records runtimes:
  1. parallel_models_mpi.py  -> model training with MPI
  2. markowitz_mpi_optimize.py -> Markowitz portfolio construction
  3. visualize_portfolio_results.py -> plotting and reporting

Outputs:
  - mpi_outputs/runtime_report.csv
  - runtime_summary.txt (human-readable)
"""

import subprocess, time, os, csv, platform, psutil

# ---------- CONFIG ----------
N_CORES = 4
SCRIPTS = [
    ("MPI Model Training", f"mpiexec -n 4 --map-by ppr:4:node --bind-to core python3 parallel_models_mpi.py"),
    ("Markowitz Optimization", "python3 -u markowitz_new.py"),
    ("Visualization & Plots", "python3 -u plot.py")
]
OUT_DIR = "mpi_outputs"
os.makedirs(OUT_DIR, exist_ok=True)
RUNTIME_CSV = os.path.join(OUT_DIR, "runtime_report.csv")
SUMMARY_TXT = os.path.join(OUT_DIR, "runtime_summary.txt")

# ---------- HELPER ----------
def run_stage(name, cmd):
    print(f"\n🚀 Starting stage: {name}")
    print(f"   ↳ Command: {cmd}")
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    t1 = time.perf_counter()
    elapsed = t1 - t0
    if proc.returncode != 0:
        print(f"❌ Stage '{name}' failed!\n{proc.stderr[:500]}")
    else:
        print(f"✅ Completed {name} in {elapsed/60:.2f} minutes ({elapsed:.2f} sec)")
    return elapsed, proc.stdout, proc.stderr, proc.returncode

# ---------- RUN ALL STAGES ----------
results = []
total_start = time.perf_counter()

for stage_name, command in SCRIPTS:
    elapsed, out, err, rc = run_stage(stage_name, command)
    results.append({
        "Stage": stage_name,
        "Command": command,
        "Runtime_sec": round(elapsed, 2),
        "Status": "OK" if rc == 0 else "FAILED"
    })

total_end = time.perf_counter()
total_time = total_end - total_start

# ---------- SYSTEM INFO ----------
cpu_info = platform.processor() or "Unknown CPU"
cores = psutil.cpu_count(logical=True)
mem_gb = psutil.virtual_memory().total / (1024**3)

sys_info = {
    "OS": platform.system(),
    "Release": platform.release(),
    "CPU": cpu_info,
    "Cores": cores,
    "RAM_GB": round(mem_gb, 2),
    "Python_Version": platform.python_version()
}

# ---------- SAVE RESULTS ----------
with open(RUNTIME_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["Stage", "Command", "Runtime_sec", "Status"])
    writer.writeheader()
    for row in results:
        writer.writerow(row)
    writer.writerow({"Stage": "TOTAL", "Command": "-", "Runtime_sec": round(total_time, 2), "Status": "OK"})

# Save readable summary
with open(SUMMARY_TXT, "w") as f:
    f.write("===== HPC PIPELINE RUNTIME SUMMARY =====\n")
    for r in results:
        f.write(f"{r['Stage']}: {r['Runtime_sec']:.2f}s ({r['Status']})\n")
    f.write(f"----------------------------------------\nTotal Runtime: {total_time:.2f}s\n")
    f.write(f"\nSystem Info:\n")
    for k,v in sys_info.items():
        f.write(f"  {k}: {v}\n")

print("\n🕒 === FINAL RUNTIME REPORT ===")
for r in results:
    print(f"{r['Stage']:<30} : {r['Runtime_sec']:>8.2f} sec  [{r['Status']}]")
print(f"--------------------------------------------")
print(f"TOTAL RUNTIME: {total_time:.2f} sec  ({total_time/60:.2f} min)")
print(f"System Info: CPU={cpu_info}, Cores={cores}, RAM={mem_gb:.1f}GB, Python={platform.python_version()}")

print(f"\n💾 Saved runtime CSV -> {RUNTIME_CSV}")
print(f"💾 Saved summary -> {SUMMARY_TXT}")
