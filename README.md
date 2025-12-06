# 🎛️ Distributed Operating System for Industrial IoT

### **A Lightweight Distributed OS Prototype with Leader Election, Failure Detection, Job Scheduling & Web Dashboard**

This project implements a **Distributed Operating System** designed for **Industrial IoT (IIoT)** environments.
The system consists of multiple cooperating nodes that perform:

* Distributed job execution
* Bully algorithm leader election
* Heartbeat-based failure detection
* FIFO job queue with overflow handling
* Real-time monitoring dashboard (Socket.IO + Flask)

Each node runs independently but communicates via **ZeroMQ** message passing.

---

# 📌 **Features**

### ✔ Distributed coordination using the **Bully Algorithm**

### ✔ Heartbeat-based node liveness detection

### ✔ FIFO job scheduling with **MAX_QUEUE** limit

### ✔ Job rejection when queue is full

### ✔ Worker job execution simulation

### ✔ Web dashboard for real-time monitoring

### ✔ Flood test tool for stress testing

---

# 🛠️ **Project Structure**

```
/distributed_os_project
│
├── node.py               # Main distributed node implementation
├── controller.py         # Web bridge controller (PUB for JOB messages + dashboard updates)
├── test_publisher.py     # Minimal job sender for testing
├── flood_jobs.py         # Stress test script (send 50/100/etc jobs)
│
├── web/
│   ├── dashboard.html    # Real-time dashboard UI
│   ├── static/
│       ├── style.css     # Dashboard styles
│       ├── client.js     # Socket.IO client logic
│
├── README.md             # This file
```

---

# 🚀 **1. Environment Setup**

You can run this project using:

### **✔ WSL Ubuntu (recommended)**

### ✔ Linux

### ✔ macOS

### ✔ Windows PowerShell (less stable for ZeroMQ)**

---

# 📦 **2. Install Required Libraries**

Create and activate a Python virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

Upgrade pip:

```bash
pip install --upgrade pip
```

Install dependencies:

```bash
pip install pyzmq flask flask-socketio eventlet
```

---

# 🔌 **3. Running the Distributed Nodes**

Each distributed node runs as a separate terminal window.

### **Start Node 1**

```bash
python3 node.py 1
```

### **Start Node 2**

```bash
python3 node.py 2
```

### **Start Node 3**

```bash
python3 node.py 3
```

> The system automatically elects a leader using the **Bully Algorithm**.
> Highest active node ID becomes leader.

---

# 🌐 **4. Start the Web Dashboard Controller**

This script binds port **6000** for JOB broadcasting and hosts the dashboard.

```bash
python3 controller.py
```

Once running:

Open your browser →
👉 **[http://localhost:5000](http://localhost:5000)**

Dashboard shows:

* Current leader
* Node status
* Job queue length
* Worker progress
* Job events (ENQUEUED, ASSIGNED, REJECTED, RESULT)

---

# 📝 **5. Send a Manual Job for Testing**

You can manually test job submission with:

```bash
python3 test_publisher.py job_manual1 "manual test job"
```

Expected behavior:

* Leader prints `Received JOB`
* Job is queued or rejected
* If queue is free → worker is assigned
* Worker prints job completion

---

# 🚨 **6. Flood Test (Stress Testing)**

Send 50 jobs at 0.01 second intervals:

```bash
python3 flood_jobs.py 50 0.01
```

Send 100 jobs at 0.005 second intervals:

```bash
python3 flood_jobs.py 100 0.005
```

Expected outcomes:

* Queue fills up to `MAX_QUEUE = 5`
* Additional jobs get `REJECTED`
* Worker assignment begins immediately
* Dashboard updates in real time

---

# 🧠 **7. Key Internal Components**

### **Leader Election — Bully Algorithm**

When a node detects leader failure:

* Sends **ELECTION** to higher IDs
* If no “OK” received → declares itself **COORDINATOR**
* Broadcasts **COORDINATOR** to all nodes

### **Heartbeat IPC**

Nodes send:

```
HEARTBEAT <node_id>
```

Leader failure detected after **FAIL_TIMEOUT = 3 seconds**.

### **Job Queue (Leader Only)**

```python
MAX_QUEUE = 5
job_queue.append((jobid, payload))
```

When queue full:

```python
if len(job_queue) >= MAX_QUEUE:
    send("REJECTED", jobid)
```

### **Worker Execution Simulation**

```python
duration = random.uniform(6.0, 10.0)
job_end_time = time.time() + duration
```

---

# 🧪 **8. Testing Workflow Checklist**

Open three terminals:

Terminal 1:

```
python3 node.py 1
```

Terminal 2:

```
python3 node.py 2
```

Terminal 3:

```
python3 node.py 3
```

Terminal 4:

```
python3 controller.py
```

Browser:

```
http://localhost:5000
```

Send a test job:

```
python3 test_publisher.py job1 "task A"
```

Stress-test:

```
python3 flood_jobs.py 50 0.01
```

Observe:

* Leader election prints
* Queue grows
* Overflow triggers REJECTED
* Workers execute jobs
* Dashboard updates live

---

# 📘 **9. Requirements**

* Python 3.8+
* ZeroMQ
* Flask + Socket.IO
* Eventlet (for async support)

---

# 📄 **11. License**

MIT License — free to use and modify.

---
