#!/usr/bin/env python3
"""Reproduce client-disconnect-mid-goal crash against a live rws instance.

Timeline:
  t=0    start rws_server (stdout echoed with [rws] prefix) + /sleep action server
  t=5    websocket client connects and sends a /sleep goal (takes 5 s server-side)
  t=7.5  client connection is killed hard (TCP RST) while the goal is in flight
  t=10   /sleep finishes and rws tries to deliver result/feedback to the dead client
  exit   report SURVIVED or CRASHED (rws exit code / signal)

Run from a shell with ROS + this workspace sourced:
  python3 test/sleep_disconnect_repro.py [--port 9095] [--rws-binary path]
"""

import argparse
import json
import os

os.environ.setdefault("RMW_IMPLEMENTATION", "rmw_zenoh_cpp")
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

import rclpy
from rclpy.action import ActionServer
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from test_msgs.action import Fibonacci

import websocket

GOAL_DURATION_SEC = 5.0
KILL_AFTER_SEC = 2.5
CONNECT_DELAY_SEC = 5.0


def ensure_zenoh_router():
    """Start a zenoh router if none is listening; returns the Popen or None."""
    if os.environ.get("RMW_IMPLEMENTATION") != "rmw_zenoh_cpp":
        return None
    with socket.socket() as probe:
        probe.settimeout(0.5)
        if probe.connect_ex(("127.0.0.1", 7447)) == 0:
            print("[repro] zenoh router already running", flush=True)
            return None
    print("[repro] starting zenoh router", flush=True)
    proc = subprocess.Popen(
        ["ros2", "run", "rmw_zenoh_cpp", "rmw_zenohd"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        with socket.socket() as probe:
            probe.settimeout(0.5)
            if probe.connect_ex(("127.0.0.1", 7447)) == 0:
                return proc
        time.sleep(0.3)
    proc.terminate()
    sys.exit("[repro] zenoh router failed to start")


def start_rws(binary, port):
    env = dict(os.environ)
    env["RCUTILS_LOGGING_BUFFERED_STREAM"] = "0"
    env["RCUTILS_COLORIZED_OUTPUT"] = "0"
    proc = subprocess.Popen(
        ["stdbuf", "-oL", "-eL", binary, "--ros-args", "--log-level", "debug",
         "-p", f"port:={port}", "-p", "rosbridge_compatible:=true", "-p", "watchdog:=true"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True,
    )

    def pump():
        for line in proc.stdout:
            print(f"[rws] {line}", end="", flush=True)

    threading.Thread(target=pump, daemon=True).start()
    return proc


class SleepActionServer(Node):
    def __init__(self):
        super().__init__("sleep_action_server")
        self._server = ActionServer(
            self, Fibonacci, "sleep", self.execute,
            callback_group=ReentrantCallbackGroup(),
        )

    def execute(self, goal_handle):
        print(f"[sleep] goal accepted, sleeping {GOAL_DURATION_SEC}s", flush=True)
        steps = int(GOAL_DURATION_SEC / 0.5)
        fb = Fibonacci.Feedback()
        for i in range(steps):
            time.sleep(0.5)
            fb.sequence = list(range(i + 1))
            goal_handle.publish_feedback(fb)
            print(f"[sleep] feedback {i + 1}/{steps}", flush=True)
        goal_handle.succeed()
        print("[sleep] goal succeeded", flush=True)
        result = Fibonacci.Result()
        result.sequence = fb.sequence
        return result


def run_client_and_kill(port):
    """Connect, send the /sleep goal, then RST the connection mid-goal."""
    done = threading.Event()

    def on_message(ws, message):
        data = json.loads(message)
        print(f"[client] <- {data.get('op', '?')}: {message[:200]}", flush=True)

    def on_open(ws):
        goal = {
            "op": "send_action_goal",
            "id": "sleep-repro-goal",
            "action": "/sleep",
            "action_type": "test_msgs/action/Fibonacci",
            "feedback": True,
            "args": {"order": 5},
        }
        print("[client] connected, sending /sleep goal", flush=True)
        ws.send(json.dumps(goal))

        def kill():
            time.sleep(KILL_AFTER_SEC)
            print(f"[client] t+{KILL_AFTER_SEC}s: killing connection (TCP RST)", flush=True)
            try:
                raw = ws.sock.sock
                raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                raw.close()
            except Exception as e:  # noqa: BLE001
                print(f"[client] kill error: {e}", flush=True)
            done.set()

        threading.Thread(target=kill, daemon=True).start()

    def on_close(ws, code, msg):
        print(f"[client] closed (code={code})", flush=True)
        done.set()

    def on_error(ws, err):
        print(f"[client] error: {err}", flush=True)

    ws = websocket.WebSocketApp(
        f"ws://127.0.0.1:{port}",
        on_open=on_open, on_message=on_message,
        on_close=on_close, on_error=on_error,
    )
    threading.Thread(target=ws.run_forever, daemon=True).start()
    done.wait(timeout=CONNECT_DELAY_SEC + KILL_AFTER_SEC + 10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    default_bin = os.path.join(os.path.dirname(__file__), "..", "build", "rws", "rws_server")
    parser.add_argument("--rws-binary", default=os.path.normpath(default_bin))
    parser.add_argument("--port", type=int, default=9095)
    args = parser.parse_args()

    if not os.path.exists(args.rws_binary):
        sys.exit(f"rws_server binary not found at {args.rws_binary} (build first?)")

    zenohd = ensure_zenoh_router()
    rws = start_rws(args.rws_binary, args.port)

    rclpy.init()
    node = SleepActionServer()
    executor = MultiThreadedExecutor(num_threads=4)
    executor.add_node(node)
    spin_thread = threading.Thread(target=executor.spin, daemon=True)
    spin_thread.start()

    try:
        print(f"[repro] waiting {CONNECT_DELAY_SEC}s before calling /sleep", flush=True)
        time.sleep(CONNECT_DELAY_SEC)

        if rws.poll() is not None:
            sys.exit(f"[repro] rws exited during startup (code {rws.returncode})")

        run_client_and_kill(args.port)

        # Goal keeps running server-side; give rws time to hit the dead client.
        grace = GOAL_DURATION_SEC - KILL_AFTER_SEC + 3.0
        print(f"[repro] connection killed; watching rws for {grace}s", flush=True)
        deadline = time.time() + grace
        while time.time() < deadline and rws.poll() is None:
            time.sleep(0.2)

        code = rws.poll()
        if code is None:
            print("\n[repro] RESULT: rws SURVIVED the mid-goal disconnect", flush=True)
            exit_code = 0
        else:
            if code < 0:
                sig = signal.Signals(-code).name
                print(f"\n[repro] RESULT: rws CRASHED with signal {sig}", flush=True)
            else:
                print(f"\n[repro] RESULT: rws exited with code {code}", flush=True)
            exit_code = 1
    finally:
        if rws.poll() is None:
            rws.terminate()
            try:
                rws.wait(timeout=5)
            except subprocess.TimeoutExpired:
                rws.kill()
        executor.shutdown(timeout_sec=2)
        node.destroy_node()
        rclpy.shutdown()
        if zenohd is not None:
            zenohd.terminate()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
