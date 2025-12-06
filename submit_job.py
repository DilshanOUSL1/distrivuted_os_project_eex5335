#!/usr/bin/env python3
# robust submit_job.py - improved for ZMQ pub/sub startup timing
# Usage: python3 submit_job.py <jobid> "<task description>"

import sys
import time
import zmq

if len(sys.argv) < 3:
    print("Usage: python3 submit_job.py <jobid> \"<task description>\"")
    sys.exit(1)

jobid = sys.argv[1]
task = " ".join(sys.argv[2:])

context = zmq.Context()
pub = context.socket(zmq.PUB)
pub.bind("tcp://127.0.0.1:6000")

# WAIT longer so nodes have time to connect (ZMQ SUB->PUB setup needs time)
time.sleep(1.0)

msg = f"JOB {jobid} {task}"

# Send the message multiple times (helps ensure at least one arrives)
for i in range(3):
    pub.send_string(msg)
    print(f"[Controller] Sent ({i+1}/3): {msg}")
    time.sleep(0.2)

time.sleep(0.1)
pub.close()
context.term()
