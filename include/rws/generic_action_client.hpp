// Copyright 2024 Karmanyaah Malhotra
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RWS__GENERIC_ACTION_CLIENT_HPP_
#define RWS__GENERIC_ACTION_CLIENT_HPP_

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <rapidjson/document.h>

#include "action_msgs/msg/goal_status.hpp"
#include "action_msgs/msg/goal_status_array.hpp"
#include "action_msgs/srv/cancel_goal.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/serialized_message.hpp"
#include "rclcpp_action/client.hpp"
#include "rclcpp_action/types.hpp"
#include "rcpputils/shared_library.hpp"
#include "rosidl_runtime_c/action_type_support_struct.h"
#include "rosidl_typesupport_introspection_cpp/message_introspection.hpp"
#include "rosidl_typesupport_introspection_cpp/service_introspection.hpp"
#include "rws/typesupport_helpers.hpp"

namespace rws
{

using rosidl_typesupport_introspection_cpp::MessageMembers;
using rosidl_typesupport_introspection_cpp::ServiceMembers;

/// A generic action client that works with runtime-determined action types.
class GenericActionClient : public rclcpp_action::ClientBase
{
public:
  using SharedResponse = std::shared_ptr<rclcpp::SerializedMessage>;
  using GoalUUID = rclcpp_action::GoalUUID;

  using GoalResponseCallback = std::function<void(bool accepted, const GoalUUID & goal_id)>;
  using FeedbackCallback = std::function<void(const GoalUUID & goal_id, SharedResponse feedback)>;
  using ResultCallback = std::function<void(const GoalUUID & goal_id, int8_t status, SharedResponse result)>;
  using CancelCallback = std::function<void(bool success)>;

  RCLCPP_SMART_PTR_DEFINITIONS(GenericActionClient)

  GenericActionClient(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_base,
    rclcpp::node_interfaces::NodeGraphInterface::SharedPtr node_graph,
    rclcpp::node_interfaces::NodeLoggingInterface::SharedPtr node_logging,
    const std::string & action_name,
    const std::string & action_type,
    const rcl_action_client_options_t & options = rcl_action_client_get_default_options());

  virtual ~GenericActionClient();

  /// Send an action goal asynchronously.
  /**
   * Callbacks are stored in an owner-tagged registry and looked up at fire
   * time (never captured by the async continuations), so detach_owner() can
   * sever a disconnected websocket client from its in-flight goals without
   * destroying this (long-lived, node-owned) action client.
   *
   * \param[in] owner_id Tag identifying the websocket client that owns the
   *   goal (see detach_owner)
   * \param[in] goal_json RapidJSON Value containing the goal fields
   * \param[in] goal_response_callback Called when goal is accepted/rejected
   * \param[in] feedback_callback Called when feedback is received (optional)
   * \param[in] result_callback Called when result is received
   * \return The goal UUID
   */
  GoalUUID async_send_goal(
    int owner_id,
    const rapidjson::Value & goal_json,
    GoalResponseCallback goal_response_callback,
    FeedbackCallback feedback_callback,
    ResultCallback result_callback);

  /// Cancel a goal by its UUID.
  /**
   * \param[in] owner_id Tag identifying the websocket client requesting the cancel
   * \param[in] goal_id The goal UUID to cancel
   * \param[in] cancel_callback Called when cancel response is received
   */
  void async_cancel_goal(int owner_id, const GoalUUID & goal_id, CancelCallback cancel_callback);

  /// Sever a websocket client from this action client.
  /**
   * Erases every callback registered by \p owner_id — under the same mutex the
   * dispatch paths invoke callbacks with, so after this returns no callback of
   * that owner is running or can ever run — and requests cancellation of the
   * owner's in-flight goals so a vanished client doesn't leave the robot
   * executing headless work. Called from ~ClientHandler; the action client
   * itself stays alive (destroying ROS entities under a spinning executor is
   * what used to crash the bridge).
   */
  void detach_owner(int owner_id);

  /// Get the action type string.
  std::string get_action_type() const { return action_type_; }

  /// Get goal members for serialization/deserialization
  const MessageMembers * get_goal_members() const { return goal_members_; }

  /// Get feedback members for serialization/deserialization
  const MessageMembers * get_feedback_members() const { return feedback_members_; }

  /// Get result members for serialization/deserialization
  const MessageMembers * get_result_members() const { return result_members_; }

protected:
  // ClientBase pure virtual overrides
  std::shared_ptr<void> create_goal_response() const override;
  std::shared_ptr<void> create_result_response() const override;
  std::shared_ptr<void> create_cancel_response() const override;
  std::shared_ptr<void> create_feedback_message() const override;
  std::shared_ptr<void> create_status_message() const override;

  void handle_feedback_message(std::shared_ptr<void> message) override;
  void handle_status_message(std::shared_ptr<void> message) override;

private:
  std::string action_type_;

  // Type support libraries and handles
  std::shared_ptr<rcpputils::SharedLibrary> action_ts_lib_;
  const rosidl_action_type_support_t * action_ts_hdl_;

  // Introspection typesupport for serialization
  std::shared_ptr<rcpputils::SharedLibrary> goal_srv_intro_lib_;
  const rosidl_service_type_support_t * goal_srv_intro_hdl_;
  std::shared_ptr<rcpputils::SharedLibrary> result_srv_intro_lib_;
  const rosidl_service_type_support_t * result_srv_intro_hdl_;
  std::shared_ptr<rcpputils::SharedLibrary> feedback_msg_intro_lib_;
  const rosidl_message_type_support_t * feedback_msg_intro_hdl_;

  // Message members for goal, feedback, result (from introspection)
  const MessageMembers * goal_members_;
  const MessageMembers * feedback_members_;  // The inner feedback, not the full FeedbackMessage
  const MessageMembers * result_members_;

  // Full message members (including wrapper fields like goal_id)
  const MessageMembers * feedback_message_members_;  // Full FeedbackMessage (goal_id + feedback)

  // Goal service request/response members
  const MessageMembers * goal_request_members_;
  const MessageMembers * goal_response_members_;
  const MessageMembers * result_request_members_;
  const MessageMembers * result_response_members_;

  // Pending callbacks, tagged with the websocket client that registered them.
  // Dispatch paths look these up at fire time and invoke them WHILE HOLDING
  // goal_callbacks_mutex_; detach_owner erases under the same mutex, which is
  // the use-after-free guard for destroyed ClientHandlers.
  struct GoalCallbacks {
    int owner_id;
    GoalResponseCallback goal_response_callback;
    FeedbackCallback feedback_callback;
    ResultCallback result_callback;
  };
  std::unordered_map<GoalUUID, GoalCallbacks> goal_callbacks_;
  std::mutex goal_callbacks_mutex_;

  struct CancelEntry {
    int owner_id;
    CancelCallback callback;
  };
  std::unordered_map<int64_t, CancelEntry> pending_cancel_callbacks_;
  int64_t next_cancel_token_ = 0;
  std::mutex cancel_callbacks_mutex_;

  /// Send a CancelGoal request with no reply callback (used by detach_owner).
  void send_detached_cancel(const GoalUUID & goal_id);
};

}  // namespace rws

#endif  // RWS__GENERIC_ACTION_CLIENT_HPP_
