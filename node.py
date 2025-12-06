#!/usr/bin/env python3
# node.py - single-threaded cluster node with heartbeats, Bully election, and leader-driven job queue
# Usage: python3 node.py <node_id>
# Example: python3 node.py 1

import sys
import time
import zmq
import random

# --- CONFIG ---
ALL_NODE_IDS = ["1", "2", "3"]   # nodes in the cluster (keep small for demo)
HEARTBEAT_INTERVAL = 1.0         # seconds between outgoing heartbeats
FAIL_TIMEOUT = 3.0               # if no heartbeat within this many seconds => failed
ELECTION_TIMEOUT = 2.0           # wait after sending ELECTION for OKs
COORDINATOR_BROADCAST_INTERVAL = 2.0
CONTROLLER_PUBADDR = "tcp://127.0.0.1:6000"  # controller publishes JOB messages here

# --- parse node id ---
if len(sys.argv) != 2:
    print("Usage: python3 node.py <node_id>")
    print("Example: python3 node.py 1")
    sys.exit(1)

node_id = sys.argv[1]
if node_id not in ALL_NODE_IDS:
    print(f"Invalid node id '{node_id}'. Choose from {ALL_NODE_IDS}")
    sys.exit(1)

# Convert IDs to integers for comparisons
ALL_NODE_IDS_INT = [int(x) for x in ALL_NODE_IDS]
NODE_INT = int(node_id)

# --- zmq setup ---
context = zmq.Context()

# PUB socket: bind to a unique port per node
pub = context.socket(zmq.PUB)
pub_address = f"tcp://127.0.0.1:500{node_id}"
pub.bind(pub_address)

# SUB socket: subscribe to all messages and connect to other nodes' PUBs
sub = context.socket(zmq.SUB)
sub.setsockopt_string(zmq.SUBSCRIBE, "")
for nid in ALL_NODE_IDS:
    if nid == node_id:
        continue
    sub.connect(f"tcp://127.0.0.1:500{nid}")

# Also connect to controller's PUB so controller can broadcast JOBs
sub.connect(CONTROLLER_PUBADDR)

# Use a poller for single-threaded non-blocking message receive
poller = zmq.Poller()
poller.register(sub, zmq.POLLIN)

# --- state for failure detection and election ---
last_seen = {nid: time.time() for nid in ALL_NODE_IDS}
failed_nodes = set()           # to avoid repeated failure alerts
leader_id = None               # current known coordinator (string) or None
election_in_progress = False
election_start_time = 0.0
received_ok = False
last_coordinator_announce = 0.0

# --- job-related state (leader + workers) ---
job_queue = []              # only used/managed by leader: list of (jobid, payload)
seen_jobs = set()           # leader's seen job IDs (mark only when leader enqueues)
MAX_QUEUE = 5               # capacity limit for leader queue (tune for tests)
worker_status = {nid: "idle" for nid in ALL_NODE_IDS if nid != node_id}  # leader's view
# For workers:
current_job = None       # tuple (jobid, payload)
job_end_time = 0.0

print(f"Node {node_id} starting. PUB={pub_address}. Peers: {[n for n in ALL_NODE_IDS if n!=node_id]}")
print(f"Controller PUB expected at {CONTROLLER_PUBADDR}")

# --- helper functions ---
def send(msg_type, payload=""):
    """Send a message of format: '<TYPE> <sender_id> [payload]'"""
    msg = f"{msg_type} {node_id}"
    if payload:
        msg += f" {payload}"
    try:
        pub.send_string(msg)
    except Exception as e:
        # avoid crashing on transient ZMQ errors
        print(f"[Node {node_id}] ERROR sending message: {e}")
    print(f"[Node {node_id}] Sent: {msg}")

def assign_jobs():
    """Leader: assign queued jobs to idle workers (FIFO)."""
    global job_queue, worker_status
    if node_id != leader_id:
        return
    if not job_queue:
        return
    # find idle workers
    for worker, status in list(worker_status.items()):
        if status == "idle" and job_queue:
            jobid, payload = job_queue.pop(0)
            # send ASSIGN <leader> <worker> <jobid> <payload>
            msg_payload = f"{worker} {jobid} {payload}"
            send("ASSIGN", msg_payload)
            worker_status[worker] = "busy"
            print(f"[Node {node_id}] (leader) Assigned job {jobid} to {worker}")

def start_election():
    """Start bully election: send ELECTION to all higher-id nodes."""
    global election_in_progress, election_start_time, received_ok, leader_id
    election_in_progress = True
    received_ok = False
    election_start_time = time.time()
    leader_id = None
    higher_exists = False
    for nid_int in ALL_NODE_IDS_INT:
        if nid_int > NODE_INT:
            higher_exists = True
            send("ELECTION")
    if not higher_exists:
        # I'm the highest ID -> declare coordinator
        declare_victory()

def declare_victory():
    """Announce self as coordinator."""
    global leader_id, election_in_progress, last_coordinator_announce, worker_status
    leader_id = node_id
    election_in_progress = False
    last_coordinator_announce = time.time()
    # initialize leader's view of workers
    worker_status = {nid: "idle" for nid in ALL_NODE_IDS if nid != node_id}
    send("COORDINATOR")
    print(f"[Node {node_id}] ACTION: Declared self as COORDINATOR")

def handle_incoming(msg):
    """
    Parse message and update last_seen, handle election messages and job messages.
    Known formats (cluster messages):
      HEARTBEAT <sender>
      ELECTION <sender>
      OK <sender>
      COORDINATOR <sender>
      ASSIGN <sender> <worker_id> <jobid> <task...>
      RESULT <sender> <jobid> <worker_id> [optional result]
    Controller format:
      JOB <jobid> <task...>
    """
    global last_seen, leader_id, election_in_progress, received_ok, job_queue, current_job, job_end_time, worker_status

    parts = msg.strip().split()
    if not parts:
        return
    mtype = parts[0]
    # Controller control format: "CTRL <CMD> <target> <value>"
    if mtype == "CTRL":
        # parts: ['CTRL', 'COMMAND', 'TARGET', 'VALUE...'] OR ['CTRL','SHUTDOWN','2']
        cmd = parts[1].upper() if len(parts) >= 2 else ''
        target = parts[2] if len(parts) >= 3 else ''
        val = " ".join(parts[3:]) if len(parts) > 3 else ''
        # If target specified and not this node, ignore (controller supports broadcasting when target empty)
        if target and target != node_id and target != 'ALL':
            return
        print(f"[Node {node_id}] Received CTRL {cmd} target={target} val={val}")
        if cmd == "SHUTDOWN":
            # graceful exit (for demo)
            print(f"[Node {node_id}] CTRL: Shutting down (demo)")
            sys.exit(0)
        elif cmd == "RESTART":
            # for demo: print and exit (supervisor can restart)
            print(f"[Node {node_id}] CTRL: Restarting (demo)")
            sys.exit(0)
        elif cmd == "CLEAR_QUEUE":
            if node_id == leader_id:
                job_queue.clear()
                seen_jobs.clear()
                print(f"[Node {node_id}] CTRL: Cleared leader queue")
        elif cmd == "SET_MAX_QUEUE":
            # val contains new size
            try:
                newv = int(val)
                if node_id == leader_id:
                    global MAX_QUEUE
                    MAX_QUEUE = newv
                    print(f"[Node {node_id}] CTRL: Set MAX_QUEUE = {MAX_QUEUE}")
            except:
                pass
        return


    # --- Handle controller JOB messages (no sender id) ---
    if mtype == "JOB":
        # JOB <jobid> <task...>
        if len(parts) >= 2:
            jobid = parts[1]
            payload = " ".join(parts[2:]) if len(parts) > 2 else ""
            print(f"[Node {node_id}] Received JOB from controller: id={jobid} payload='{payload}'")

            # Only the leader should decide to enqueue / mark seen.
            if leader_id == node_id:
                # leader enqueues and tries to assign, avoiding duplicates in leader's own seen_jobs
                if jobid in seen_jobs:
                    print(f"[Node {node_id}] INFO: Ignoring duplicate JOB {jobid} (leader seen)")
                else:
                    # leader-only: check queue capacity then enqueue
                    if len(job_queue) >= MAX_QUEUE:
                        print(f"[Node {node_id}] (leader) REJECTED job {jobid} — queue full")
                        send("REJECTED", jobid)
                    else:
                        seen_jobs.add(jobid)          # mark only when leader enqueues
                        job_queue.append((jobid, payload))
                        print(f"[Node {node_id}] (leader) Enqueued job {jobid} (queue_len={len(job_queue)})")
                        send("ENQUEUED", jobid)
                        assign_jobs()
            else:
                # Not leader: do not mark seen. Ignore — leader will handle enqueue.
                pass
        return    # JOB is not a peer-sourced message, done here

    # --- For cluster messages that include sender id as second token: ---
    sender = None
    if len(parts) >= 2:
        sender = parts[1]
        # update last_seen for known peers
        if sender in last_seen:
            last_seen[sender] = time.time()
            # If a node previously marked failed has come back, forget failure state
            if sender in failed_nodes:
                failed_nodes.discard(sender)
                print(f"[Node {node_id}] INFO: Node {sender} recovered")

    # Debug print for cluster messages
    print(f"[Node {node_id}] Received: {msg.strip()}")

    # Cluster message handling
    if mtype == "HEARTBEAT":
        # heartbeat only updates last_seen (done above)
        pass

    elif mtype == "ELECTION":
        # sender must be set for ELECTION messages
        if sender and int(sender) < NODE_INT:
            send("OK")
            if not election_in_progress:
                start_election()

    elif mtype == "OK":
        received_ok = True

    elif mtype == "COORDINATOR":
        leader_id = sender
        election_in_progress = False
        global last_coordinator_announce
        last_coordinator_announce = time.time()
        print(f"[Node {node_id}] INFO: New coordinator is Node {leader_id}")

    elif mtype == "ASSIGN":
        # ASSIGN <leader> <worker_id> <jobid> <task...>
        if len(parts) >= 4:
            worker = parts[2]
            jobid = parts[3]
            task = " ".join(parts[4:]) if len(parts) > 4 else ""
            # If this node is the worker target, accept job
            if worker == node_id:
                duration = random.uniform(6.0, 10.0)  # simulated work duration
                # assign worker-local state
                current_job = (jobid, task)
                job_end_time = time.time() + duration
                print(f"[Node {node_id}] ACCEPTED job {jobid} (will finish in {duration:.1f}s)")
            else:
                # leader can use this to update view if desired
                pass

    elif mtype == "RESULT":
        # RESULT <sender> <jobid> <worker>
        if len(parts) >= 4:
            jobid = parts[2]
            worker = parts[3]
            # Only leader cares to mark worker idle and record result
            if node_id == leader_id:
                worker_status[worker] = "idle"
                print(f"[Node {node_id}] (leader) Received RESULT for job {jobid} from worker {worker}")
                # try assign next job
                assign_jobs()

    elif mtype == "REJECTED":
        # informational: someone (leader) rejected a job; dashboard will display
        # nothing specific to do here on workers
        pass

def check_failures():
    """Detect failed nodes, trigger election if leader died, and avoid repeated alerts."""
    global leader_id
    now = time.time()
    for nid, t in list(last_seen.items()):
        if nid == node_id:
            continue
        # Node considered failed if not seen within FAIL_TIMEOUT
        if now - t > FAIL_TIMEOUT:
            if nid not in failed_nodes:
                # first time we detect failure -> announce and record it
                failed_nodes.add(nid)
                print(f"[Node {node_id}] ALERT: Node {nid} FAILED (last seen {now - t:.1f}s ago)")
                if leader_id == nid:
                    print(f"[Node {node_id}] INFO: Leader {leader_id} failed, initiating election.")
                    start_election()
                    break
            # if already in failed_nodes, do nothing (prevents repeated prints)
        else:
            # node is alive; if it was previously failed, remove from failed_nodes
            if nid in failed_nodes:
                failed_nodes.discard(nid)
                print(f"[Node {node_id}] INFO: Node {nid} recovered")

# --- main single-threaded scheduler loop ---
last_heartbeat_sent = 0.0
startup_time = time.time()
STARTUP_GRACE = 1.0

try:
    while True:
        now = time.time()

        # startup grace
        if now - startup_time < STARTUP_GRACE:
            time.sleep(0.05)
            continue

        # If we have no known leader and no election in progress, start one
        if leader_id is None and not election_in_progress:
            if now - startup_time >= STARTUP_GRACE:
                print(f"[Node {node_id}] INFO: No leader known after startup, starting election.")
                start_election()

        # 1) Send heartbeat periodically
        if now - last_heartbeat_sent >= HEARTBEAT_INTERVAL:
            send("HEARTBEAT")
            last_heartbeat_sent = now

        # 2) Non-blocking poll for incoming messages
        socks = dict(poller.poll(100))
        if sub in socks and socks[sub] == zmq.POLLIN:
            try:
                incoming = sub.recv_string(flags=zmq.NOBLOCK)
            except Exception:
                incoming = None
            if incoming:
                handle_incoming(incoming)

        # 3) Election timeout handling
        if election_in_progress:
            if received_ok:
                if now - election_start_time > ELECTION_TIMEOUT:
                    print(f"[Node {node_id}] WARNING: No COORDINATOR after OK, restarting election.")
                    start_election()
            else:
                if now - election_start_time > ELECTION_TIMEOUT:
                    declare_victory()

        # 4) Worker job completion handling (non-blocking)
        if current_job is not None and time.time() >= job_end_time:
            jobid, task = current_job
            # send RESULT <sender> <jobid> <worker_id>
            send("RESULT", f"{jobid} {node_id}")
            print(f"[Node {node_id}] Completed job {jobid} and reported result")
            current_job = None

        # 5) Periodic failure detection (includes leader check)
        check_failures()

        # 6) Leader re-announces coordinator periodically
        if leader_id == node_id and now - last_coordinator_announce >= COORDINATOR_BROADCAST_INTERVAL:
            send("COORDINATOR")
            last_coordinator_announce = now

        # 7) for visibility: print current leader and queue length occasionally
        # Print roughly once per second (use integer seconds to reduce spam)
        if int(now) % 1 == 0:
            if node_id == leader_id:
                print(f"[Node {node_id}] Current Leader: {leader_id} | queue_len={len(job_queue)}")
            else:
                print(f"[Node {node_id}] Current Leader: {leader_id}")

        # small sleep to avoid busy loop
        time.sleep(0.5)

except KeyboardInterrupt:
    print(f"\nNode {node_id} exiting...")
finally:
    pub.close()
    sub.close()
    context.term()
