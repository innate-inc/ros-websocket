#!/usr/bin/env python3
"""Black-box test for the subscriber ``durability`` option.

This test talks to a real, running ``rws_server`` over a WebSocket exactly like a
browser client would. It does NOT link against any rws C++ code -- it only uses
the public WebSocket protocol plus a normal ROS 2 (rclpy) publisher.

What it checks (the latched-topic behavior):

  1. transient_local: a subscriber that asks for ``"durability": "transient_local"``
     receives a message that a latched publisher published BEFORE the subscriber
     connected. This is the fix.

  2. volatile: a subscriber that asks for ``"durability": "volatile"`` matches the
     same publisher but does NOT receive that already-published sample. This is
     why the option matters.

Usage (build + source the workspace first):

    colcon build --packages-select rws
    source install/setup.bash
    python3 test/test_durability.py

Note on RMW: rws_server's generic pub/sub needs runtime type introspection, so
the server refuses to run under plain ``rmw_fastrtps_cpp``. Use any other RMW --
e.g. Zenoh (start its router first) or ``rmw_fastrtps_dynamic_cpp``:

    ros2 run rmw_zenoh_cpp rmw_zenohd &        # only for Zenoh
    export RMW_IMPLEMENTATION=rmw_zenoh_cpp
"""

import json
import subprocess
import threading
import time

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, QoSDurabilityPolicy
from std_msgs.msg import String
from websocket import create_connection

PORT = 9098
TOPIC = "/py_latched_topic"
TYPE = "std_msgs/msg/String"
LATCHED_TEXT = "hello from a latched publisher"
WS_URL = f"ws://localhost:{PORT}"


def start_server():
    """Launch rws_server on PORT and wait until it accepts WebSocket connections."""
    proc = subprocess.Popen(
        ["ros2", "run", "rws", "rws_server", "--ros-args", "-p", f"port:={PORT}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(100):  # wait up to ~10s
        try:
            create_connection(WS_URL, timeout=1).close()
            return proc
        except Exception:
            time.sleep(0.1)
    proc.terminate()
    raise RuntimeError("rws_server did not start")


def make_latched_publisher():
    """Create a ROS 2 node that latches one retained message on TOPIC."""
    node = Node("py_latched_publisher")
    qos = QoSProfile(depth=1)
    qos.durability = QoSDurabilityPolicy.TRANSIENT_LOCAL
    publisher = node.create_publisher(String, TOPIC, qos)
    msg = String()
    msg.data = LATCHED_TEXT
    publisher.publish(msg)
    return node


def receive_with_durability(durability):
    """Subscribe over the WebSocket with the given durability and return the first
    publish message's text, or None if nothing arrives within 3 seconds."""
    ws = create_connection(WS_URL, timeout=5)
    ws.send(
        json.dumps(
            {"op": "subscribe", "topic": TOPIC, "type": TYPE, "durability": durability}
        )
    )
    deadline = time.time() + 3
    try:
        while time.time() < deadline:
            ws.settimeout(deadline - time.time())
            try:
                message = json.loads(ws.recv())
            except Exception:
                break
            if message.get("op") == "publish" and message.get("topic") == TOPIC:
                return message["msg"]["data"]
        return None
    finally:
        ws.close()


def main():
    rclpy.init()
    publisher_node = make_latched_publisher()

    # Keep the publisher alive in the background so it serves its latched sample
    # to late-joining subscribers.
    spin_thread = threading.Thread(
        target=rclpy.spin, args=(publisher_node,), daemon=True
    )
    spin_thread.start()

    server = start_server()
    time.sleep(1.0)  # let discovery settle

    try:
        # The fix: a transient_local subscriber receives the message that was
        # latched before it ever connected.
        got = receive_with_durability("transient_local")
        assert got == LATCHED_TEXT, f"transient_local should get the latched message, got {got!r}"
        print("PASS: transient_local subscriber received the latched message")

        # The contrast: a volatile subscriber matches the publisher but does not
        # receive a sample published before it joined.
        got = receive_with_durability("volatile")
        assert got is None, f"volatile should NOT get the latched message, got {got!r}"
        print("PASS: volatile subscriber did not receive the latched message")
    finally:
        server.terminate()
        server.wait(timeout=5)
        publisher_node.destroy_node()
        rclpy.shutdown()

    print("All checks passed.")


if __name__ == "__main__":
    main()
