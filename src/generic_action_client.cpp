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

#include "rws/generic_action_client.hpp"

#include <cstring>

#include "action_msgs/msg/goal_info.hpp"
#include "rclcpp/typesupport_helpers.hpp"
#include "rmw/rmw.h"
#include "unique_identifier_msgs/msg/uuid.hpp"
#include "rws/translate.hpp"

namespace rws
{

namespace {
// Thread-local storage for passing typesupport between helper function and constructor
thread_local std::shared_ptr<rcpputils::SharedLibrary> g_action_ts_lib;
thread_local const rosidl_action_type_support_t * g_action_ts_hdl = nullptr;

const rosidl_action_type_support_t * load_and_cache_action_typesupport(
  const std::string & action_type)
{
  const char * action_ts_id = "rosidl_typesupport_cpp";
  g_action_ts_lib = rws::get_typesupport_library(action_type, action_ts_id);
  g_action_ts_hdl = rws::get_action_typesupport_handle(action_type, action_ts_id, *g_action_ts_lib);
  return g_action_ts_hdl;
}
}  // anonymous namespace

GenericActionClient::GenericActionClient(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_base,
  rclcpp::node_interfaces::NodeGraphInterface::SharedPtr node_graph,
  rclcpp::node_interfaces::NodeLoggingInterface::SharedPtr node_logging,
  const std::string & action_name,
  const std::string & action_type,
  const rcl_action_client_options_t & options)
: rclcpp_action::ClientBase(
    node_base, node_graph, node_logging, action_name,
    load_and_cache_action_typesupport(action_type),
    options),
  action_type_(action_type),
  action_ts_lib_(g_action_ts_lib),
  action_ts_hdl_(g_action_ts_hdl)
{
  // For introspection (serialization/deserialization), we need the introspection typesupport
  // Get the service type names for goal and result
  // Action types are structured as: pkg/action/Name
  // Goal service: pkg/action/Name_SendGoal
  // Result service: pkg/action/Name_GetResult
  // Feedback message: pkg/action/Name_FeedbackMessage
  
  // Extract package and action name
  auto sep_pos = action_type.find_last_of('/');
  auto pkg_sep = action_type.find_first_of('/');
  std::string package_name = action_type.substr(0, pkg_sep);
  std::string action_name_only = action_type.substr(sep_pos + 1);
  
  std::string goal_srv_type = package_name + "/action/" + action_name_only + "_SendGoal";
  std::string result_srv_type = package_name + "/action/" + action_name_only + "_GetResult";
  std::string feedback_msg_type = package_name + "/action/" + action_name_only + "_FeedbackMessage";

  // Load introspection typesupport for goal service
  goal_srv_intro_lib_ = rclcpp::get_typesupport_library(goal_srv_type, rws::ts_identifier);
  goal_srv_intro_hdl_ = rws::get_service_typesupport_handle(
    goal_srv_type, rws::ts_identifier, *goal_srv_intro_lib_);
  auto goal_srv_members = static_cast<const ServiceMembers *>(goal_srv_intro_hdl_->data);
  goal_request_members_ = goal_srv_members->request_members_;
  goal_response_members_ = goal_srv_members->response_members_;

  // Load introspection typesupport for result service
  result_srv_intro_lib_ = rclcpp::get_typesupport_library(result_srv_type, rws::ts_identifier);
  result_srv_intro_hdl_ = rws::get_service_typesupport_handle(
    result_srv_type, rws::ts_identifier, *result_srv_intro_lib_);
  auto result_srv_members = static_cast<const ServiceMembers *>(result_srv_intro_hdl_->data);
  result_request_members_ = result_srv_members->request_members_;
  result_response_members_ = result_srv_members->response_members_;

  // Load introspection typesupport for feedback message
  feedback_msg_intro_lib_ = rclcpp::get_typesupport_library(feedback_msg_type, rws::ts_identifier);
  feedback_msg_intro_hdl_ = rclcpp::get_typesupport_handle(
    feedback_msg_type, rws::ts_identifier, *feedback_msg_intro_lib_);
  feedback_message_members_ = static_cast<const MessageMembers *>(feedback_msg_intro_hdl_->data);

  // Extract goal and result members from the request/response members
  // Goal request has: goal_id (UUID) + goal (the actual goal message)
  // Result response has: status (int8) + result (the actual result message)
  // The goal member is the second field in goal request
  // The result member is the second field in result response
  
  // For goal members, we look at the nested message type in goal request
  // MessageMember.members_ is a rosidl_message_type_support_t*, we need to cast its data field
  if (goal_request_members_->member_count_ >= 2) {
    const auto * goal_member = &goal_request_members_->members_[1];
    if (goal_member->members_) {
      goal_members_ = static_cast<const MessageMembers *>(goal_member->members_->data);
    } else {
      goal_members_ = goal_request_members_;  // Fallback
    }
  } else {
    goal_members_ = goal_request_members_;
  }

  // For result members, we look at the nested message type in result response
  if (result_response_members_->member_count_ >= 2) {
    const auto * result_member = &result_response_members_->members_[1];
    if (result_member->members_) {
      result_members_ = static_cast<const MessageMembers *>(result_member->members_->data);
    } else {
      result_members_ = result_response_members_;  // Fallback
    }
  } else {
    result_members_ = result_response_members_;
  }

  // For feedback, the FeedbackMessage contains goal_id + feedback
  // Extract the inner feedback type from the second field
  feedback_members_ = feedback_message_members_;  // Default to full message
  if (feedback_message_members_->member_count_ >= 2) {
    const auto * feedback_member = &feedback_message_members_->members_[1];
    if (feedback_member->members_) {
      feedback_members_ = static_cast<const MessageMembers *>(feedback_member->members_->data);
    }
  }
}

GenericActionClient::~GenericActionClient() {}

GenericActionClient::GoalUUID GenericActionClient::async_send_goal(
  const rapidjson::Value & goal_json,
  GoalResponseCallback goal_response_callback,
  FeedbackCallback feedback_callback,
  ResultCallback result_callback)
{
  // Generate a new goal UUID
  GoalUUID goal_id = this->generate_goal_id();

  // Allocate and populate the goal request message
  auto request = allocate_message(goal_request_members_);

  // Set the goal_id field (first field of SendGoal_Request)
  // The goal_id is a unique_identifier_msgs/UUID which contains a uint8[16] array
  auto * goal_id_field = reinterpret_cast<unique_identifier_msgs::msg::UUID *>(
    reinterpret_cast<uint8_t *>(request.get()) + goal_request_members_->members_[0].offset_);
  std::copy(goal_id.begin(), goal_id.end(), goal_id_field->uuid.begin());

  // Populate the goal field from JSON directly
  if (goal_request_members_->member_count_ >= 2) {
    const auto * goal_member = &goal_request_members_->members_[1];
    void * goal_field = reinterpret_cast<uint8_t *>(request.get()) + goal_member->offset_;
    
    // Use json_to_ros_message to populate the goal field directly
    json_to_ros_message(goal_json, goal_members_, goal_field);
  }

  // Store callbacks for this goal
  {
    std::lock_guard<std::mutex> lock(goal_callbacks_mutex_);
    goal_callbacks_[goal_id] = {feedback_callback, result_callback};
  }

  // Send the goal request
  this->send_goal_request(
    request,
    [this, goal_id, goal_response_callback, result_callback](std::shared_ptr<void> response) {
      // Check if goal was accepted
      // GoalResponse has: accepted (bool) + stamp (builtin_interfaces/Time)
      auto * accepted_field = reinterpret_cast<bool *>(
        reinterpret_cast<uint8_t *>(response.get()) + goal_response_members_->members_[0].offset_);
      bool accepted = *accepted_field;

      if (goal_response_callback) {
        goal_response_callback(accepted, goal_id);
      }

      if (accepted && result_callback) {
        // Request the result
        auto result_request = allocate_message(result_request_members_);
        
        // Set goal_id in result request
        auto * result_goal_id = reinterpret_cast<unique_identifier_msgs::msg::UUID *>(
          reinterpret_cast<uint8_t *>(result_request.get()) + 
          result_request_members_->members_[0].offset_);
        std::copy(goal_id.begin(), goal_id.end(), result_goal_id->uuid.begin());

        this->send_result_request(
          result_request,
          [this, goal_id, result_callback](std::shared_ptr<void> result_response) {
            // Extract status and result
            int8_t status = *reinterpret_cast<int8_t *>(
              reinterpret_cast<uint8_t *>(result_response.get()) + 
              result_response_members_->members_[0].offset_);

            // Serialize the result field for the callback
            if (result_response_members_->member_count_ >= 2) {
              const auto * result_member = &result_response_members_->members_[1];
              void * result_field = reinterpret_cast<uint8_t *>(result_response.get()) + 
                result_member->offset_;

              // result_member->members_ is a rosidl_message_type_support_t*, cast its data
              const auto * result_field_members = static_cast<const MessageMembers *>(result_member->members_->data);
              std::string result_type = rws::get_type_from_message_members(result_field_members);
              auto result_ts_lib = rclcpp::get_typesupport_library(result_type, rws::ts_identifier);
              auto result_ts_hdl = rclcpp::get_typesupport_handle(
                result_type, rws::ts_identifier, *result_ts_lib);

              auto ser_result = std::make_shared<rclcpp::SerializedMessage>();
              rmw_ret_t r = rmw_serialize(
                result_field, result_ts_hdl, &ser_result->get_rcl_serialized_message());
              if (r == RMW_RET_OK && result_callback) {
                result_callback(goal_id, status, ser_result);
              }
            }

            // Remove goal from tracking
            std::lock_guard<std::mutex> lock(goal_callbacks_mutex_);
            goal_callbacks_.erase(goal_id);
          });
      } else if (!accepted) {
        // Goal was rejected, remove from tracking
        std::lock_guard<std::mutex> lock(goal_callbacks_mutex_);
        goal_callbacks_.erase(goal_id);
      }
    });

  return goal_id;
}

void GenericActionClient::async_cancel_goal(
  const GoalUUID & goal_id,
  CancelCallback cancel_callback)
{
  auto cancel_request = std::make_shared<action_msgs::srv::CancelGoal::Request>();
  std::copy(goal_id.begin(), goal_id.end(), cancel_request->goal_info.goal_id.uuid.begin());

  this->send_cancel_request(
    std::static_pointer_cast<void>(cancel_request),
    [this, goal_id, cancel_callback](std::shared_ptr<void> response) {
      auto cancel_response = std::static_pointer_cast<action_msgs::srv::CancelGoal::Response>(response);
      bool success = cancel_response->return_code == action_msgs::srv::CancelGoal::Response::ERROR_NONE;
      
      if (cancel_callback) {
        cancel_callback(success);
      }

      if (success) {
        std::lock_guard<std::mutex> lock(goal_callbacks_mutex_);
        goal_callbacks_.erase(goal_id);
      }
    });
}

std::shared_ptr<void> GenericActionClient::create_goal_response() const
{
  return allocate_message(goal_response_members_);
}

std::shared_ptr<void> GenericActionClient::create_result_response() const
{
  return allocate_message(result_response_members_);
}

std::shared_ptr<void> GenericActionClient::create_cancel_response() const
{
  return std::make_shared<action_msgs::srv::CancelGoal::Response>();
}

std::shared_ptr<void> GenericActionClient::create_feedback_message() const
{
  // Must allocate the full FeedbackMessage (goal_id + feedback), not just the inner feedback
  return allocate_message(feedback_message_members_);
}

std::shared_ptr<void> GenericActionClient::create_status_message() const
{
  return std::make_shared<action_msgs::msg::GoalStatusArray>();
}

void GenericActionClient::handle_feedback_message(std::shared_ptr<void> message)
{
  // Extract goal_id from feedback message (first field of FeedbackMessage)
  auto * goal_id_field = reinterpret_cast<unique_identifier_msgs::msg::UUID *>(
    reinterpret_cast<uint8_t *>(message.get()) + feedback_message_members_->members_[0].offset_);
  
  GoalUUID goal_id;
  std::copy(goal_id_field->uuid.begin(), goal_id_field->uuid.end(), goal_id.begin());

  // Find the callback for this goal
  FeedbackCallback callback;
  {
    std::lock_guard<std::mutex> lock(goal_callbacks_mutex_);
    auto it = goal_callbacks_.find(goal_id);
    if (it != goal_callbacks_.end() && it->second.feedback_callback) {
      callback = it->second.feedback_callback;
    }
  }

  if (callback) {
    // Serialize the feedback field for the callback
    if (feedback_message_members_->member_count_ >= 2) {
      const auto * feedback_member = &feedback_message_members_->members_[1];
      void * feedback_field = reinterpret_cast<uint8_t *>(message.get()) + feedback_member->offset_;

      // feedback_member->members_ is a rosidl_message_type_support_t*, cast its data
      const auto * feedback_field_members = static_cast<const MessageMembers *>(feedback_member->members_->data);
      std::string feedback_type = rws::get_type_from_message_members(feedback_field_members);
      auto feedback_ts_lib = rclcpp::get_typesupport_library(feedback_type, rws::ts_identifier);
      auto feedback_ts_hdl = rclcpp::get_typesupport_handle(
        feedback_type, rws::ts_identifier, *feedback_ts_lib);

      auto ser_feedback = std::make_shared<rclcpp::SerializedMessage>();
      rmw_ret_t r = rmw_serialize(
        feedback_field, feedback_ts_hdl, &ser_feedback->get_rcl_serialized_message());
      if (r == RMW_RET_OK) {
        callback(goal_id, ser_feedback);
      }
    }
  }
}

void GenericActionClient::handle_status_message(std::shared_ptr<void> /* message */)
{
  // Status messages are not directly exposed through the rosbridge protocol
  // They are used internally to track goal status
}

}  // namespace rws
