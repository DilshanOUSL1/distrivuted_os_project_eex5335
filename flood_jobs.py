#!/usr/bin/env python3
# flood_jobs.py - POST many jobs to web_bridge to create overload
# Usage: python3 flood_jobs.py <count> <interval>
# Example: python3 flood_jobs.py 50 0.01

import sys
import requests
import time

if len(sys.argv) < 2:
    print("Usage: python3 flood_jobs.py <count> <interval_seconds (optional)>")
    sys.exit(1)

count = int(sys.argv[1])
interval = float(sys.argv[2]) if len(sys.argv) >= 3 else 0.01

url = "http://localhost:5000/submit_job"

for i in range(1, count+1):
    jobid = f"job{i}"
    payload = f"task {i}"
    try:
        r = requests.post(url, json={"jobid": jobid, "task": payload}, timeout=2)
        print(i, r.json())
    except Exception as e:
        print("err", i, e)
    time.sleep(interval)
