#!/usr/bin/env python3
# web_bridge.py - ZMQ -> Socket.IO bridge + REST + control commands
import time
import uuid
import zmq
from threading import Thread, Event

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'demo-secret'
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins="*")

# ZMQ setup
context = zmq.Context()
controller_pub = context.socket(zmq.PUB)
controller_pub.bind("tcp://127.0.0.1:6000")   # nodes connect here for JOBs & CTRL

sub = context.socket(zmq.SUB)
sub.setsockopt_string(zmq.SUBSCRIBE, "")
sub.connect("tcp://127.0.0.1:5001")
sub.connect("tcp://127.0.0.1:5002")
sub.connect("tcp://127.0.0.1:5003")

poller = zmq.Poller()
poller.register(sub, zmq.POLLIN)

# In-memory state for frontend
leader = None
workers = {}
job_queue = []
job_assignments = {}
recent = []
seen_jobs = set()

def push_recent(s):
    recent.insert(0, f"{time.strftime('%H:%M:%S')} {s}")
    if len(recent) > 100:
        recent.pop()

def process_zmq_message(msg):
    global leader
    parts = msg.strip().split()
    if not parts:
        return
    t = parts[0]
    if t == "HEARTBEAT":
        nid = parts[1]
        workers.setdefault(nid, "idle")
    elif t == "COORDINATOR":
        leader = parts[1]
        push_recent(f"COORDINATOR -> {leader}")
    elif t == "ENQUEUED":
        if len(parts) >= 3:
            jobid = parts[2]
            if jobid not in seen_jobs:
                seen_jobs.add(jobid)
                job_queue.append(jobid)
                push_recent(f"ENQUEUED {jobid}")
    elif t == "ASSIGN":
        if len(parts) >= 4:
            worker = parts[2]; jobid = parts[3]
            job_assignments[jobid] = worker
            workers[worker] = "busy"
            try:
                job_queue.remove(jobid)
            except ValueError:
                pass
            push_recent(f"ASSIGN {jobid} -> {worker}")
    elif t == "RESULT":
        if len(parts) >= 4:
            jobid = parts[2]; worker = parts[3]
            workers[worker] = "idle"
            job_assignments.pop(jobid, None)
            push_recent(f"RESULT {jobid} from {worker}")
    elif t == "REJECTED":
        if len(parts) >= 3:
            jobid = parts[2]
            push_recent(f"REJECTED {jobid}")
    elif t == "JOB":
        if len(parts) >= 2:
            jobid = parts[1]
            push_recent(f"JOB submitted {jobid}")
    else:
        push_recent(msg.strip())

    # push state to browsers
    socketio.emit('state_update', {
        'leader': leader,
        'workers': workers,
        'job_queue': job_queue,
        'recent': recent[:50]
    })

stop_event = Event()
def zmq_listener():
    while not stop_event.is_set():
        socks = dict(poller.poll(200))
        if sub in socks and socks[sub] == zmq.POLLIN:
            try:
                msg = sub.recv_string(flags=zmq.NOBLOCK)
            except Exception:
                continue
            process_zmq_message(msg)

zmq_thread = Thread(target=zmq_listener, daemon=True)
zmq_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit_job', methods=['POST'])
def submit_job():
    data = request.get_json() or request.form
    jobid = data.get('jobid') or f"job-{uuid.uuid4().hex[:6]}"
    task = data.get('task') or ""
    msg = f"JOB {jobid} {task}"
    time.sleep(0.05)
    controller_pub.send_string(msg)
    push_recent(f"JOB submitted {jobid} ({task})")
    socketio.emit('job_submitted', {'jobid': jobid, 'task': task})
    return jsonify({'status': 'ok', 'jobid': jobid})

# Control endpoint (HTTP)
@app.route('/control', methods=['POST'])
def control():
    """
    JSON: {"cmd":"shutdown"|"restart"|"clear_queue"|"set_max_queue", "target":"2", "value":5}
    """
    data = request.get_json() or request.form
    cmd = data.get('cmd')
    target = data.get('target', '')
    value = data.get('value', '')
    if not cmd:
        return jsonify({'status':'error','reason':'no cmd'}), 400
    ctrl_msg = f"CTRL {cmd.upper()} {target} {value}".strip()
    time.sleep(0.05)
    controller_pub.send_string(ctrl_msg)
    push_recent(f"SENT CTRL: {ctrl_msg}")
    return jsonify({'status':'ok','sent':ctrl_msg})

# SocketIO control (optional)
@socketio.on('control')
def socket_control(data):
    """
    Expects {cmd:'shutdown', target:'2', value:''}
    """
    cmd = data.get('cmd'); target = data.get('target',''); value = data.get('value','')
    ctrl_msg = f"CTRL {cmd.upper()} {target} {value}".strip()
    controller_pub.send_string(ctrl_msg)
    push_recent(f"SENT CTRL: {ctrl_msg}")
    socketio.emit('ctrl_ack', {'sent':ctrl_msg})

@socketio.on('connect')
def on_connect():
    print("Browser connected:", request.sid, "addr=", request.remote_addr)
    socketio.emit('state_update', {
        'leader': leader,
        'workers': workers,
        'job_queue': job_queue,
        'recent': recent[:50]
    })

def shutdown():
    stop_event.set()
    zmq_thread.join(timeout=1)
    controller_pub.close()
    sub.close()
    context.term()

if __name__ == '__main__':
    try:
        socketio.run(app, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        shutdown()
