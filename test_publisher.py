#!/usr/bin/env python3
# test_publisher.py - reliable publisher: waits for wire-up and sends a few times
import zmq, time, sys

ctx = zmq.Context()
pub = ctx.socket(zmq.PUB)
try:
    pub.bind("tcp://127.0.0.1:6000")
    print("Publisher bound to tcp://127.0.0.1:6000")
except zmq.ZMQError as e:
    print(f"Error binding to port 6000: {e}")
    print("Hint: Is web_bridge.py running? Stop it to run this test manually.")
    sys.exit(1)

# Allow time for SUB sockets to connect (important for PUB/SUB)
print("Waiting 1.0s for SUB sockets to connect...")
time.sleep(1.0)

jobid = sys.argv[1] if len(sys.argv) > 1 else "job-test"
task = sys.argv[2] if len(sys.argv) > 2 else "manual-test"
msg = f"JOB {jobid} {task}"
print("Sending:", msg)

# Send the message multiple times quickly to increase chance of delivery
for i in range(3):
    pub.send_string(msg)
    time.sleep(0.05)

print("Done sending. Sleeping briefly then exiting.")
time.sleep(0.2)
pub.close()
ctx.term()
