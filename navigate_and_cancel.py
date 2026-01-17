#!/usr/bin/env python3
"""
Script to send a NavigateToPose action goal via rosbridge and cancel it after 2 seconds.
"""

import json
import time
import threading
import websocket
import uuid

# Configuration
ROSBRIDGE_HOST = "mars-26.local"
ROSBRIDGE_PORT = 9090
ACTION_NAME = "/navigate_to_pose"
ACTION_TYPE = "nav2_msgs/action/NavigateToPose"

# Generate a unique ID for this goal (client-side)
CLIENT_ID = str(uuid.uuid4())

# Will be set when we receive the goal acceptance from the server
server_goal_id = None
ws_instance = None


def on_message(ws, message):
    """Handle incoming messages from rosbridge."""
    global server_goal_id
    
    data = json.loads(message)
    op = data.get("op", "")
    
    if op == "action_feedback":
        print(f"[Feedback] {json.dumps(data.get('values', {}), indent=2)}")
    elif op == "action_result":
        status = data.get("status", -1)
        result = data.get("result", False)
        values = data.get("values", {})
        status_names = {
            0: "STATUS_UNKNOWN",
            1: "STATUS_ACCEPTED",
            2: "STATUS_EXECUTING",
            3: "STATUS_CANCELING",
            4: "STATUS_SUCCEEDED",
            5: "STATUS_CANCELED",
            6: "STATUS_ABORTED",
        }
        # Handle string status from rosbridge
        if isinstance(status, str):
            status_name = status.upper()
        else:
            status_name = status_names.get(status, f"UNKNOWN({status})")
        
        print(f"[Result] status={status_name}, result={result}")
        print(f"         values={json.dumps(values, indent=2)}")
        
        # Only close on terminal statuses (not "accepted" or "executing")
        if isinstance(status, str):
            if status.lower() in {"succeeded", "canceled", "aborted"}:
                ws.close()
        elif status in {4, 5, 6}:
            ws.close()
    elif op == "send_action_goal":
        # This is the acknowledgment that the goal was accepted
        server_goal_id = data.get("goal_id", "")
        accepted = data.get("result", False)
        print(f"[Goal Accepted] goal_id={server_goal_id}, accepted={accepted}")
    elif op == "status":
        level = data.get("level", "")
        msg = data.get("msg", "")
        print(f"[Status:{level}] {msg}")
    else:
        print(f"[{op}] {json.dumps(data, indent=2)}")


def on_error(ws, error):
    """Handle websocket errors."""
    print(f"[Error] {error}")


def on_close(ws, close_status_code, close_msg):
    """Handle websocket close."""
    print(f"[Closed] Connection closed (code={close_status_code}, msg={close_msg})")


def on_open(ws):
    """Handle websocket connection open."""
    global ws_instance
    ws_instance = ws
    
    print(f"[Connected] Connected to rosbridge at {ROSBRIDGE_HOST}:{ROSBRIDGE_PORT}")
    
    # Send the action goal
    goal_msg = {
        "op": "send_action_goal",
        "id": CLIENT_ID,
        "action": ACTION_NAME,
        "action_type": ACTION_TYPE,
        "feedback": True,
        "args": {
            "pose": {
                "header": {
                    "frame_id": "map"
                },
                "pose": {
                    "position": {
                        "x": 1.0,
                        "y": 0.49,
                        "z": 0.0
                    },
                    "orientation": {
                        "x": 0.0,
                        "y": 0.0,
                        "z": -1.0,
                        "w": 0.06
                    }
                }
            },
            "behavior_tree": "mapfree"
        }
    }
    
    print(f"[Sending Goal] id={CLIENT_ID}")
    print(f"               pose: x=1.0, y=0.49, z=0.0")
    print(f"               orientation: x=0.0, y=0.0, z=-1.0, w=0.06")
    print(f"               behavior_tree: mapfree")
    ws.send(json.dumps(goal_msg))
    
    # Schedule cancellation after 2 seconds
    def cancel_goal():
        global server_goal_id
        time.sleep(5)
        
        # Wait a bit more if we haven't received the goal_id yet
        retries = 10
        while server_goal_id is None and retries > 0:
            time.sleep(0.1)
            retries -= 1
        
        if server_goal_id is None:
            print("[Error] Never received goal_id from server, cannot cancel")
            return
            
        cancel_msg = {
            "op": "cancel_action_goal",
            "id": CLIENT_ID,
            "action": ACTION_NAME,
            "goal_id": server_goal_id
        }
        print(f"\n[Canceling] Canceling goal after 2 seconds (goal_id={server_goal_id})")
        ws.send(json.dumps(cancel_msg))
    
    cancel_thread = threading.Thread(target=cancel_goal, daemon=True)
    cancel_thread.start()


def main():
    """Main entry point."""
    url = f"ws://{ROSBRIDGE_HOST}:{ROSBRIDGE_PORT}"
    print(f"[Connecting] {url}")
    
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Run forever (until action result received or error)
    ws.run_forever()


if __name__ == "__main__":
    main()
