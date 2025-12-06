#!/usr/bin/env python3
# dashboard.py - simple real-time CLI monitor for your demo
# Run: python3 dashboard.py

import zmq
import time
import threading
from collections import deque, defaultdict

context = zmq.Context()
sub = context.socket(zmq.SUB)
sub.setsockopt_string(zmq.SUBSCRIBE, "")
# connect to all node PUB ports and controller PUB
sub.connect("tcp://127.0.0.1:5001")
sub.connect("tcp://127.0.0.1:5002")
sub.connect("tcp://127.0.0.1:5003")
sub.connect("tcp://127.0.0.1:6000")

# state
leader = None
workers = {}          # worker_id -> "idle" / "busy"
job_queue = deque()   # list of job ids in arrival order (inferred from ENQUEUED)
job_assignments = {}  # jobid -> worker
recent = deque(maxlen=20)

poller = zmq.Poller()
poller.register(sub, zmq.POLLIN)

def human_now():
    return time.strftime("%H:%M:%S")

def process_msg(msg):
    global leader
    parts = msg.strip().split()
    if not parts:
        return
    t = parts[0]

    if t == "HEARTBEAT":
        # HEARTBEAT <id>
        nid = parts[1]
        # nothing special to show for heartbeat

    elif t == "COORDINATOR":
        leader = parts[1]
        recent.appendleft((human_now(), f"COORDINATOR -> {leader}"))

    elif t == "ENQUEUED":
        # ENQUEUED <leader> <jobid>
        # Note: send("ENQUEUED", jobid) created message like "ENQUEUED 3 job1"
        if len(parts) >= 3:
            jobid = parts[2]
            job_queue.append(jobid)
            recent.appendleft((human_now(), f"ENQUEUED {jobid}"))

    elif t == "ASSIGN":
        # ASSIGN <leader> <worker> <jobid> <payload...>
        if len(parts) >= 4:
            worker = parts[2]
            jobid = parts[3]
            job_assignments[jobid] = worker
            # mark worker busy and remove from queue if present
            workers[worker] = "busy"
            try:
                job_queue.remove(jobid)
            except ValueError:
                pass
            recent.appendleft((human_now(), f"ASSIGN {jobid} -> {worker}"))

    elif t == "RESULT":
        # RESULT <sender> <jobid> <worker>
        if len(parts) >= 4:
            jobid = parts[2]
            worker = parts[3]
            workers[worker] = "idle"
            job_assignments.pop(jobid, None)
            recent.appendleft((human_now(), f"RESULT {jobid} from {worker}"))

    elif t == "JOB":
        # controller-job (not enqueued by leader automatically)
        if len(parts) >= 2:
            jobid = parts[1]
            recent.appendleft((human_now(), f"JOB submitted {jobid}"))

    else:
        # other messages (ELECTION, OK, etc.)
        recent.appendleft((human_now(), msg.strip()))

def render():
    # clear screen (simple)
    print("\033[H\033[J", end="")

    print("=== Distributed OS Dashboard ===")
    print(f"Time: {human_now()}")
    print(f"Leader: {leader}")
    print("")
    print("Workers status:")
    if workers:
        for w in sorted(workers.keys()):
            print(f"  {w}: {workers[w]}")
    else:
        print("  (no worker status yet)")

    print("")
    print(f"Queued jobs (inferred): {len(job_queue)}")
    if job_queue:
        for j in list(job_queue)[:10]:
            print(f"  - {j}")
    print("")
    print("Recent events:")
    for ts, ev in list(recent)[:10]:
        print(f" {ts}  {ev}")

def listener_loop():
    while True:
        socks = dict(poller.poll(200))
        if sub in socks and socks[sub] == zmq.POLLIN:
            msg = sub.recv_string()
            process_msg(msg)

# populate initial known workers (1..3)
for i in ["1","2","3"]:
    workers[i] = "idle"

# run listener in background thread and render periodically
t = threading.Thread(target=listener_loop, daemon=True)
t.start()

try:
    while True:
        render()
        time.sleep(1.0)
except KeyboardInterrupt:
    print("\nDashboard exiting...")
finally:
    sub.close()
    context.term()
