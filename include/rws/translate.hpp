// Copyright 2022 Vasily Kiniv
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

#ifndef RWS__TRANSLATE_HPP_
#define RWS__TRANSLATE_HPP_

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <memory>
#include <string>

#include "rclcpp/serialized_message.hpp"
#include "rosidl_typesupport_introspection_cpp/message_introspection.hpp"

namespace rws
{

using SharedMessage = std::shared_ptr<rclcpp::SerializedMessage>;
using ConstSharedMessage = std::shared_ptr<const rclcpp::SerializedMessage>;
using rosidl_typesupport_introspection_cpp::MessageMembers;

// RapidJSON type aliases
using RapidWriter = rapidjson::Writer<rapidjson::StringBuffer>;

/// Translate serialized message to RapidJSON Value
/**
 * \param msg_type Message type, e.g. "std_msgs/String"
 * \param[in] msg Serialized message
 * \param[out] doc RapidJSON document to populate
 */
void serialized_message_to_json(const std::string & msg_type, ConstSharedMessage msg, rapidjson::Document & doc);

/// Translate RapidJSON Value to serialized message
/**
 * \param[in] msg_type Message type, e.g. "std_msgs/msg/String"
 * \param[in] value RapidJSON Value representation of the message
 * \return Serialized message
 */
SharedMessage json_to_serialized_message(const std::string & msg_type, const rapidjson::Value & value);

/// Translate RapidJSON Value to serialized service request
/**
 * \param[in] srv_type Service type, e.g. "std_srvs/srv/SetBool"
 * \param[in] value RapidJSON Value representation of service request
 * \return Serialized service request
 */
SharedMessage json_to_serialized_service_request(const std::string & srv_type, const rapidjson::Value & value);

/// Translate serialized service response to RapidJSON and write directly
/**
 * \param[in] srv_type Service type, e.g. "std_srvs/srv/SetBool"
 * \param[in] msg Serialized service response
 * \param[out] writer RapidJSON writer to output to
 */
void serialized_service_response_to_json(const std::string & srv_type, ConstSharedMessage msg, RapidWriter & writer);

/// Populate a ROS message from RapidJSON using introspection
/**
 * \param[in] value RapidJSON Value representation of the message fields
 * \param[in] members MessageMembers introspection data
 * \param[out] message Pointer to the pre-allocated ROS message to populate
 */
void json_to_ros_message(const rapidjson::Value & value, const MessageMembers * members, void * message);

/// Fast serialize ROS message to JSON string for publish op using RapidJSON
/**
 * \param[in] topic Topic name
 * \param[in] msg_type Message type, e.g. "std_msgs/msg/String"
 * \param[in] msg Serialized message
 * \return Complete JSON string: {"op":"publish","topic":"...","msg":{...}}
 */
std::string build_publish_message(const std::string & topic, const std::string & msg_type, ConstSharedMessage msg);

/// Generate textual representation of the message structure
/**
 * \param[in] msg_type Message type, e.g. "std_msgs/msg/String"
 * \param[in] rosbridge_compatible Is Rosbidge compatible, e.g. replace nanosec with nsec
 * \return String representation of the message structure
 */
std::string generate_message_meta(const std::string & msg_type, bool rosbridge_compatible = false);

/// Convert ROS1-style message type to ROS2 style
/**
 * \param[in] type Message type in ROS1 style (e.g. "std_msgs/String")
 * \return Message type in ROS2 style (e.g. "std_msgs/msg/String")
 */
std::string message_type_to_ros2_style(const std::string & type);

}  // namespace rws

#endif  // RWS__TRANSLATE_HPP_