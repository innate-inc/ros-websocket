#!/usr/bin/env python3
"""Black-box test for the subscriber durability option, over a real WebSocket."""

# This test talks to a real, running rws_server over a WebSocket exactly like a
# browser client would. It does NOT link against any rws C++ code -- it only uses
# the public WebSocket protocol plus a normal ROS 2 (rclpy) publisher.
#
# What it checks (the latched-topic behavior):
#   1. transient_local: a subscriber that asks for {"qos": {"durability":
#      "transient_local"}} receives a message that a latched publisher published
#      BEFORE the subscriber connected. This is the fix.
#   2. volatile: a subscriber that asks for {"qos": {"durability": "volatile"}}
#      matches the same publisher but does NOT receive that already-published
#      sample. This is why the option matters.
#
# Dependencies:
#   * rclpy + std_msgs  -- from a sourced ROS 2 install (ros-humble-std-msgs)
#   * websocket-client  -- pip install websocket-client  (provides `websocket`)
#   * rws_server on the PATH -- colcon build + source install/setup.bash
#
# Usage (build + source the workspace first):
#   colcon build --packages-select rws
#   source install/setup.bash
#   python3 test/test_durability.py
#
# Note on RMW: rws_server's generic pub/sub needs runtime type introspection, so
# the server refuses to run under plain rmw_fastrtps_cpp. Use any other RMW --
# e.g. Zenoh (start its router first) or rmw_fastrtps_dynamic_cpp:
#   ros2 run rmw_zenoh_cpp rmw_zenohd &        # only for Zenoh
#   export RMW_IMPLEMENTATION=rmw_zenoh_cpp

import json
import subprocess
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
    print(f"[publisher] latched {LATCHED_TEXT!r} on {TOPIC} (transient_local)")
    return node


def receive_with_durability(durability, wait_seconds):
    """Subscribe with the given durability and return the received text or None."""
    print(f"[subscriber durability={durability!r}] subscribing, waiting {wait_seconds}s...")
    ws = create_connection(WS_URL, timeout=5)
    ws.send(
        json.dumps(
            {
                "op": "subscribe",
                "topic": TOPIC,
                "type": TYPE,
                "qos": {"durability": durability},
            }
        )
    )
    deadline = time.time() + wait_seconds
    try:
        while time.time() < deadline:
            ws.settimeout(deadline - time.time())
            try:
                message = json.loads(ws.recv())
            except Exception:
                break
            if message.get("op") == "publish" and message.get("topic") == TOPIC:
                text = message["msg"]["data"]
                print(f"[subscriber durability={durability!r}] received {text!r}")
                return text
        print(f"[subscriber durability={durability!r}] received nothing")
        return None
    finally:
        ws.close()


def main():
    rclpy.init()
    publisher_node = make_latched_publisher()

    # The publisher_node stays alive for the duration of main(); the middleware
    # retains the latched sample and serves it to late-joining subscribers
    # without needing to spin the node.
    server = start_server()
    time.sleep(2.0)  # let discovery settle

    try:
        # The fix: a transient_local subscriber receives the message that was
        # latched before it ever connected. Allow a generous window because the
        # retained sample is only delivered once the subscription matches the
        # publisher, which depends on discovery latency. Running this case first
        # also warms up discovery for the volatile case below.
        got = receive_with_durability("transient_local", wait_seconds=10)
        assert got == LATCHED_TEXT, f"transient_local should get the latched message, got {got!r}"
        print("PASS: transient_local subscriber received the latched message")

        # The contrast: a volatile subscriber matches the publisher but does not
        # receive a sample published before it joined. Discovery is already warm
        # from the case above, so a short window is enough to show it gets nothing.
        got = receive_with_durability("volatile", wait_seconds=4)
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
