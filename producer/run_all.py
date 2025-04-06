import subprocess
import glob
import os
import sys

producer_files = glob.glob("producer/*.producer.py")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

print(f"[RUNNER] Found {len(producer_files)} producer(s): {producer_files}")

procs = []
for file in producer_files:
    print(f"[RUNNER] Starting: {file}")
    proc = subprocess.Popen(["python3", file])
    procs.append(proc)

# Optional: Wait for all to finish
for proc in procs:
    proc.wait()
