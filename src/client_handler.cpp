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

#include "rws/client_handler.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include "rclcpp/logger.hpp"
#include "rclcpp/qos.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rws/translate.hpp"

namespace rws
{

using namespace std::chrono_literals;
using std::placeholders::_1;

std::string string_thread_id()
{
  auto hashed = std::hash<std::thread::id>()(std::this_thread::get_id());
  return std::to_string(hashed);
}

ClientHandler::ClientHandler(
  int client_id, std::shared_ptr<rws::NodeInterface<>> node, std::shared_ptr<Connector<>> connector,
  bool rosbridge_compatible, std::function<void(std::string & msg)> callback,
  std::function<void(std::vector<std::uint8_t> & msg)> binary_callback)
: client_id_(client_id),
  node_(node),
  connector_(connector),
  rosbridge_compatible_(rosbridge_compatible),
  callback_(callback),
  binary_callback_(binary_callback)
{
  RCLCPP_INFO(
    get_logger(), "Constructing client %s(%s)", std::to_string(client_id_).c_str(),
    string_thread_id().c_str());
}

ClientHandler::~ClientHandler()
{
  RCLCPP_INFO(
    get_logger(), "Destroying client %s(%s)", std::to_string(client_id_).c_str(),
    string_thread_id().c_str());
  for (auto it = subscriptions_.begin(); it != subscriptions_.end(); ++it) {
    it->second();
  }
  for (auto it = publishers_.begin(); it != publishers_.end(); ++it) {
    it->second();
  }
   RCLCPP_INFO(
    get_logger(), "Done Destroying client %s(%s)", std::to_string(client_id_).c_str(),
    string_thread_id().c_str()); 
}

void ClientHandler::send_message(std::string & msg)
{
  if (this->callback_) {
    this->callback_(msg);
  }
}

void ClientHandler::send_message(std::vector<std::uint8_t> & msg)
{
  if (this->binary_callback_) {
    this->binary_callback_(msg);
  }
}

void ClientHandler::subscription_callback(topic_params & params, std::shared_ptr<const rclcpp::SerializedMessage> message)
{
  auto compression = params.compression;
  auto sub_type = params.type;

  // Binary formats not supported
  if (compression == "cbor-raw" || compression == "cbor" || compression == "bson" ||
      compression == "msgpack" || compression == "ubjson" || compression == "bjdata") {
    std::string error_msg = "Unsupported protocol: " + compression;
    RCLCPP_WARN(get_logger(), "%s", error_msg.c_str());
    this->send_message(error_msg);
    return;
  }

  // Use RapidJSON to build complete publish message
  std::string json_str = rws::build_publish_message(params.topic, sub_type, message);
  this->send_message(json_str);
}

// ============================================================================
// Helper functions for RapidJSON
// ============================================================================

static inline bool has_string(const rapidjson::Value& v, const char* key) {
  return v.HasMember(key) && v[key].IsString();
}

static inline bool get_bool(const rapidjson::Value& v, const char* key, bool def = false) {
  if (v.HasMember(key) && v[key].IsBool()) return v[key].GetBool();
  return def;
}

static inline double get_double(const rapidjson::Value& v, const char* key, double def = 0.0) {
  if (v.HasMember(key)) {
    if (v[key].IsDouble()) return v[key].GetDouble();
    if (v[key].IsInt()) return static_cast<double>(v[key].GetInt());
    if (v[key].IsFloat()) return static_cast<double>(v[key].GetFloat());
  }
  return def;
}

// Helper to write the id field from document to writer
static void write_id(const rapidjson::Document& msg, RapidWriter& w) {
  if (msg.HasMember("id")) {
    w.Key("id");
    if (msg["id"].IsString()) w.String(msg["id"].GetString());
    else if (msg["id"].IsInt()) w.Int(msg["id"].GetInt());
    else if (msg["id"].IsUint()) w.Uint(msg["id"].GetUint());
  }
}

// ============================================================================
// RapidJSON message processing
// ============================================================================

std::string ClientHandler::process_message_rapid(const char* data, size_t length)
{
  rapidjson::Document doc;
  doc.Parse(data, length);
  
  rapidjson::StringBuffer buffer;
  buffer.Reserve(512);
  RapidWriter writer(buffer);
  
  if (doc.HasParseError()) {
    writer.StartObject();
    writer.Key("error"); writer.String("JSON parse error");
    writer.Key("result"); writer.Bool(false);
    writer.EndObject();
    return std::string(buffer.GetString(), buffer.GetSize());
  }
  
  if (!doc.HasMember("op") || !doc["op"].IsString()) {
    writer.StartObject();
    write_id(doc, writer);
    writer.Key("error"); writer.String("No op specified");
    writer.Key("result"); writer.Bool(false);
    writer.EndObject();
    return std::string(buffer.GetString(), buffer.GetSize());
  }
  
  const char* op = doc["op"].GetString();
  bool handled = false;
  
  if (strcmp(op, "subscribe") == 0) {
    handled = subscribe_to_topic_rapid(doc, buffer, writer);
  } else if (strcmp(op, "unsubscribe") == 0) {
    handled = unsubscribe_from_topic_rapid(doc, buffer, writer);
  } else if (strcmp(op, "advertise") == 0) {
    handled = advertise_topic_rapid(doc, buffer, writer);
  } else if (strcmp(op, "unadvertise") == 0) {
    handled = unadvertise_topic_rapid(doc, buffer, writer);
  } else if (strcmp(op, "publish") == 0) {
    handled = publish_to_topic_rapid(doc, buffer, writer);
  } else if (strcmp(op, "call_service") == 0) {
    handled = call_service_rapid(doc, buffer, writer);
  } else if (strcmp(op, "send_action_goal") == 0) {
    handled = send_action_goal_rapid(doc, buffer, writer);
  } else if (strcmp(op, "cancel_action_goal") == 0) {
    handled = cancel_action_goal_rapid(doc, buffer, writer);
  }
  
  if (!handled) {
    RCLCPP_WARN(get_logger(), "Unhandled request op: %s", op);
    buffer.Clear();
    writer.Reset(buffer);
    writer.StartObject();
    write_id(doc, writer);
    writer.Key("result"); writer.Bool(false);
    writer.Key("error"); writer.String("Unknown operation");
    writer.EndObject();
  }
  
  return std::string(buffer.GetString(), buffer.GetSize());
}

bool ClientHandler::subscribe_to_topic_rapid(const rapidjson::Document & msg, rapidjson::StringBuffer & buf, RapidWriter & w)
{
  if (!has_string(msg, "topic")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No topic specified");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No topic specified");
    return true;
  }
  
  std::string topic = msg["topic"].GetString();
  
  // Determine the topic type - either from message or by discovery
  std::string sub_type;
  if (has_string(msg, "type") && strlen(msg["type"].GetString()) > 0) {
    // Type is provided, use it directly
    sub_type = msg["type"].GetString();
  } else {
    // No type provided, need to discover it
    std::map<std::string, std::vector<std::string>> topics = node_->get_topic_names_and_types();
    if (topics.find(topic) == topics.end()) {
      w.StartObject();
      write_id(msg, w);
      w.Key("op"); w.String("status");
      w.Key("level"); w.String("error");
      w.Key("msg"); w.String(("Topic " + topic + " not found and no type specified").c_str());
      w.EndObject();
      RCLCPP_ERROR(get_logger(), "Topic %s not found and no type specified", topic.c_str());
      return true;
    }
    sub_type = topics[topic][0];
  }
  
  size_t history_depth = 10;
  if (msg.HasMember("history_depth") && msg["history_depth"].IsNumber()) {
    history_depth = msg["history_depth"].GetInt();
  } else if (msg.HasMember("queue_size") && msg["queue_size"].IsNumber()) {
    history_depth = msg["queue_size"].GetInt();
  }
  
  rclcpp::Duration throttle_rate(0, 0);
  if (msg.HasMember("throttle_rate") && msg["throttle_rate"].IsNumber()) {
    size_t throttle_rate_ms = msg["throttle_rate"].GetInt();
    throttle_rate = rclcpp::Duration(0, throttle_rate_ms * 1000000);
  }
  
  std::string compression = has_string(msg, "compression") ? msg["compression"].GetString() : "none";
  
  if (subscriptions_.count(topic) == 0) {
    topic_params params(topic, sub_type, history_depth, compression, throttle_rate);
    subscriptions_[topic] = connector_->subscribe_to_topic(
      client_id_, params, std::bind(&ClientHandler::subscription_callback, this, std::placeholders::_1, std::placeholders::_2));
  }
  
  // Success - no response per rosbridge spec (messages will arrive via publish op)
  buf.Clear();
  return true;
}

bool ClientHandler::unsubscribe_from_topic_rapid(const rapidjson::Document & msg, rapidjson::StringBuffer & buf, RapidWriter & w)
{
  if (!has_string(msg, "topic")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No topic specified");
    w.EndObject();
    return true;
  }
  
  std::string topic = msg["topic"].GetString();
  if (subscriptions_.count(topic) > 0) {
    subscriptions_[topic]();
    subscriptions_.erase(topic);
  }
  
  // Success - no response per rosbridge spec
  buf.Clear();
  return true;
}

bool ClientHandler::advertise_topic_rapid(const rapidjson::Document & msg, rapidjson::StringBuffer & buf, RapidWriter & w)
{
  if (!has_string(msg, "topic")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No topic specified");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No topic specified");
    return true;
  }
  
  std::string topic = msg["topic"].GetString();
  std::string type;
  
  if (has_string(msg, "type") && strlen(msg["type"].GetString()) > 0) {
    type = rws::message_type_to_ros2_style(msg["type"].GetString());
  } else {
    std::map<std::string, std::vector<std::string>> topics = node_->get_topic_names_and_types();
    if (topics.find(topic) != topics.end() && !topics[topic].empty()) {
      type = topics[topic][0];
    } else {
      w.StartObject();
      write_id(msg, w);
      w.Key("op"); w.String("status");
      w.Key("level"); w.String("error");
      w.Key("msg"); w.String("No type specified and topic not found for type lookup");
      w.EndObject();
      RCLCPP_ERROR(get_logger(), "No type specified and topic not found for type lookup");
      return true;
    }
  }
  
  size_t history_depth = 10;
  if (msg.HasMember("history_depth") && msg["history_depth"].IsNumber()) {
    history_depth = msg["history_depth"].GetInt();
  } else if (msg.HasMember("queue_size") && msg["queue_size"].IsNumber()) {
    history_depth = msg["queue_size"].GetInt();
  }
  
  bool latch = get_bool(msg, "latch", false);
  topic_params params(topic, type, history_depth, latch);
  
  if (publishers_.count(topic) == 0) {
    publishers_[topic] = connector_->advertise_topic(client_id_, params, publisher_cb_[topic]);
    publisher_type_[topic] = type;
  }
  
  // Success - no response per rosbridge spec
  buf.Clear();
  return true;
}

bool ClientHandler::unadvertise_topic_rapid(const rapidjson::Document & msg, rapidjson::StringBuffer & buf, RapidWriter & w)
{
  if (!has_string(msg, "topic")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("warning");
    w.Key("msg"); w.String("No topic specified");
    w.EndObject();
    return true;
  }
  
  std::string topic = msg["topic"].GetString();
  if (publishers_.count(topic) > 0) {
    publishers_[topic]();
    publishers_.erase(topic);
    publisher_cb_.erase(topic);
    publisher_type_.erase(topic);
  } else {
    // Per spec: warning if topic doesn't exist or not advertising
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("warning");
    w.Key("msg"); w.String(("Not advertising topic: " + topic).c_str());
    w.EndObject();
    return true;
  }
  
  // Success - no response per rosbridge spec
  buf.Clear();
  return true;
}

bool ClientHandler::publish_to_topic_rapid(const rapidjson::Document & msg, rapidjson::StringBuffer & buf, RapidWriter & w)
{
  if (!has_string(msg, "topic")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No topic specified");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No topic specified");
    return true;
  }
  
  std::string topic = msg["topic"].GetString();
  
  // Auto-advertise if not already advertised
  if (publishers_.count(topic) == 0) {
    // Need to call advertise first - reuse the rapid version
    rapidjson::StringBuffer adv_buf;
    RapidWriter adv_w(adv_buf);
    if (!advertise_topic_rapid(msg, adv_buf, adv_w)) {
      w.StartObject();
      write_id(msg, w);
      w.Key("op"); w.String("status");
      w.Key("level"); w.String("error");
      w.Key("msg"); w.String("Failed to auto-advertise topic");
      w.EndObject();
      return true;
    }
    // Check if we actually advertised (publisher should exist now)
    if (publishers_.count(topic) == 0) {
      w.StartObject();
      write_id(msg, w);
      w.Key("op"); w.String("status");
      w.Key("level"); w.String("error");
      w.Key("msg"); w.String("Failed to auto-advertise topic");
      w.EndObject();
      return true;
    }
  }
  
  if (!msg.HasMember("msg") || !msg["msg"].IsObject()) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No msg specified");
    w.EndObject();
    return true;
  }
  
  std::string type = publisher_type_[topic];
  auto serialized_msg = rws::json_to_serialized_message(type, msg["msg"]);
  publisher_cb_[topic](serialized_msg);
  
  // Success - no response per rosbridge spec
  buf.Clear();
  return true;
}

bool ClientHandler::call_service_rapid(const rapidjson::Document & msg, rapidjson::StringBuffer & buf, RapidWriter & w)
{
  if (!has_string(msg, "service")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No service specified");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No service specified");
    return true;
  }
  
  std::string service = msg["service"].GetString();
  
  RCLCPP_DEBUG(get_logger(), "call_service_rapid: %s", service.c_str());
  
  // Handle rosapi services
  if (service == "/rosapi/topics_and_raw_types" || service == "/rosapi/topics") {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("service_response");
    w.Key("service"); w.String(service.c_str());
    w.Key("values");
    w.StartObject();
    
    std::map<std::string, std::vector<std::string>> topics = node_->get_topic_names_and_types();
    
    w.Key("topics");
    w.StartArray();
    for (auto& t : topics) w.String(t.first.c_str());
    w.EndArray();
    
    w.Key("types");
    w.StartArray();
    for (auto& t : topics) w.String(t.second[0].c_str());
    w.EndArray();
    
    if (service == "/rosapi/topics_and_raw_types") {
      w.Key("typedefs_full_text");
      w.StartArray();
      for (auto& t : topics) {
        w.String(rws::generate_message_meta(t.second[0], rosbridge_compatible_).c_str());
      }
      w.EndArray();
    }
    
    w.EndObject();  // values
    w.Key("result"); w.Bool(true);
    w.EndObject();
    return true;
  }
  
  if (service == "/rosapi/service_type") {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("service_response");
    w.Key("service"); w.String(service.c_str());
    
    if (!msg.HasMember("args") || !msg["args"].HasMember("service")) {
      w.Key("result"); w.Bool(false);
      w.EndObject();
      return true;
    }
    
    std::string service_name = msg["args"]["service"].GetString();
    std::map<std::string, std::vector<std::string>> services = node_->get_service_names_and_types();
    if (services.find(service_name) == services.end()) {
      RCLCPP_ERROR(get_logger(), "Service not found: %s", service_name.c_str());
      w.Key("result"); w.Bool(false);
      w.EndObject();
      return true;
    }

    std::string service_type = services[service_name][0];
    w.Key("values");
    w.StartObject();
    w.Key("type"); w.String(service_type.c_str());
    w.EndObject();
    w.Key("result"); w.Bool(true);
    w.EndObject();
    return true;
  }
  
  if (service == "/rosapi/nodes") {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("service_response");
    w.Key("service"); w.String(service.c_str());
    w.Key("values");
    w.StartObject();
    w.Key("nodes");
    w.StartArray();
    for (auto& n : node_->get_node_names()) w.String(n.c_str());
    w.EndArray();
    w.EndObject();
    w.Key("result"); w.Bool(true);
    w.EndObject();
    return true;
  }
  
  if (service == "/rosapi/publishers") {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("service_response");
    w.Key("service"); w.String(service.c_str());
    w.Key("values");
    w.StartObject();
    w.Key("publishers");
    w.StartArray();
    
    if (msg.HasMember("args") && msg["args"].HasMember("topic")) {
      std::vector<rclcpp::TopicEndpointInfo> publishers = 
        node_->get_publishers_info_by_topic(msg["args"]["topic"].GetString());
      for (const auto & pub_info : publishers) {
        std::string name = "/" + pub_info.node_name();
        w.String(name.c_str());
      }
    }
    
    w.EndArray();
    w.EndObject();
    w.Key("result"); w.Bool(true);
    w.EndObject();
    return true;
  }
  
  if (service == "/rosapi/subscribers") {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("service_response");
    w.Key("service"); w.String(service.c_str());
    w.Key("values");
    w.StartObject();
    w.Key("subscribers");
    w.StartArray();
    
    if (msg.HasMember("args") && msg["args"].HasMember("topic")) {
      std::vector<rclcpp::TopicEndpointInfo> subscribers = 
        node_->get_subscriptions_info_by_topic(msg["args"]["topic"].GetString());
      for (const auto & sub_info : subscribers) {
        std::string name = "/" + sub_info.node_name();
        w.String(name.c_str());
      }
    }
    
    w.EndArray();
    w.EndObject();
    w.Key("result"); w.Bool(true);
    w.EndObject();
    return true;
  }
  
  if (service == "/rosapi/node_details") {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("service_response");
    w.Key("service"); w.String(service.c_str());
    w.Key("values");
    w.StartObject();
    
    w.Key("subscribing"); w.StartArray();
    w.Key("publishing"); w.StartArray();
    w.Key("services"); w.StartArray();
    
    if (msg.HasMember("args") && msg["args"].HasMember("node")) {
      auto [ns, node_name] = split_ns_node_name(msg["args"]["node"].GetString());
      
      // Collect subscribing topics
      std::vector<std::string> subscribing;
      std::vector<std::string> publishing;
      
      std::map<std::string, std::vector<std::string>> topics = node_->get_topic_names_and_types();
      for (auto& topic : topics) {
        auto subscribers = node_->get_subscriptions_info_by_topic(topic.first);
        for (auto& sub : subscribers) {
          std::string sub_node = sub.node_name();
          std::string sub_ns = sub.node_namespace() == "/" ? "" : sub.node_namespace();
          if (sub_ns + sub_node == ns + node_name) {
            subscribing.push_back(topic.first);
          }
        }
        
        auto publishers = node_->get_publishers_info_by_topic(topic.first);
        for (auto& pub : publishers) {
          std::string pub_node = pub.node_name();
          std::string pub_ns = pub.node_namespace() == "/" ? "" : pub.node_namespace();
          if (pub_ns + pub_node == ns + node_name) {
            publishing.push_back(topic.first);
          }
        }
      }
      
      // Reset and write properly
      buf.Clear();
      w.Reset(buf);
      w.StartObject();
      write_id(msg, w);
      w.Key("op"); w.String("service_response");
      w.Key("service"); w.String(service.c_str());
      w.Key("values");
      w.StartObject();
      
      w.Key("subscribing"); w.StartArray();
      for (auto& s : subscribing) w.String(s.c_str());
      w.EndArray();
      
      w.Key("publishing"); w.StartArray();
      for (auto& p : publishing) w.String(p.c_str());
      w.EndArray();
      
      w.Key("services"); w.StartArray();
      try {
        auto services = node_->get_service_names_and_types_by_node(node_name, ns);
        for (auto& s : services) w.String(s.first.c_str());
      } catch (const std::exception & e) {
        RCLCPP_ERROR(get_logger(), "Exception while fetching services: %s", e.what());
      }
      w.EndArray();
      
      w.EndObject();  // values
      w.Key("result"); w.Bool(true);
      w.EndObject();
      return true;
    }
    
    w.EndArray();  // services
    w.EndArray();  // publishing  
    w.EndArray();  // subscribing
    w.EndObject();  // values
    w.Key("result"); w.Bool(true);
    w.EndObject();
    return true;
  }
  
  if (service == "/rosapi/topic_type") {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("service_response");
    w.Key("service"); w.String(service.c_str());
    
    if (!msg.HasMember("args") || !msg["args"].HasMember("topic")) {
      w.Key("result"); w.Bool(false);
      w.EndObject();
      return true;
    }
    
    std::string topic_name = msg["args"]["topic"].GetString();
    std::map<std::string, std::vector<std::string>> topics = node_->get_topic_names_and_types(); 
    if (topics.find(topic_name) == topics.end()) {
      RCLCPP_ERROR(get_logger(), "Topic not found: %s", topic_name.c_str());
      w.Key("result"); w.Bool(false);
      w.EndObject();
      return true;
    }

    w.Key("values");
    w.StartObject();
    w.Key("type"); w.String(topics[topic_name][0].c_str());
    w.EndObject();
    w.Key("result"); w.Bool(true);
    w.EndObject();
    return true;
  }
  
  if (service == "/rosapi/services_for_type") {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("service_response");
    w.Key("service"); w.String(service.c_str());
    
    w.Key("values");
    w.StartObject();
    w.Key("services");
    w.StartArray();
    
    if (msg.HasMember("args") && msg["args"].HasMember("type")) {
      std::string service_type = msg["args"]["type"].GetString();
      auto service_name_and_types = node_->get_service_names_and_types();

      for (const auto &pair : service_name_and_types) {
        for (const auto &type : pair.second) {
          if (type == service_type) {
            w.String(pair.first.c_str());
            break;
          }
        }
      }
    }
    
    w.EndArray();
    w.EndObject();
    w.Key("result"); w.Bool(true);
    w.EndObject();
    return true;
  }
  
  // External service call
  std::string service_type;
  if (has_string(msg, "type") && strlen(msg["type"].GetString()) > 0) {
    // Type is provided, use it directly
    service_type = msg["type"].GetString();
  } else {
    // No type provided, need to discover it
    std::map<std::string, std::vector<std::string>> services = node_->get_service_names_and_types();
    auto service_it = services.find(service);
    if (service_it == services.end()) {
      w.StartObject();
      write_id(msg, w);
      w.Key("op"); w.String("service_response");
      w.Key("service"); w.String(service.c_str());
      w.Key("result"); w.Bool(false);
      w.EndObject();
      RCLCPP_ERROR(get_logger(), "Service not found: %s", service.c_str());
      return true;
    }
    if (service_it->second.empty()) {
      w.StartObject();
      write_id(msg, w);
      w.Key("op"); w.String("service_response");
      w.Key("service"); w.String(service.c_str());
      w.Key("result"); w.Bool(false);
      w.EndObject();
      RCLCPP_ERROR(get_logger(), "Service has no advertised type: %s", service.c_str());
      return true;
    }
    service_type = service_it->second.front();
  }

  if (clients_.count(service) == 0) {
    clients_[service] = node_->create_generic_client(
      service, service_type, rmw_qos_profile_services_default, nullptr);
  }

  while (!clients_[service]->wait_for_service(1s)) {
    if (!rclcpp::ok()) {
      w.StartObject();
      write_id(msg, w);
      w.Key("op"); w.String("service_response");
      w.Key("service"); w.String(service.c_str());
      w.Key("result"); w.Bool(false);
      w.EndObject();
      RCLCPP_ERROR(get_logger(), "Interrupted while waiting for the service. Exiting.");
      return true;
    }
    RCLCPP_INFO(get_logger(), "service not available, waiting again...");
  }

  static const rapidjson::Document empty_doc;
  const rapidjson::Value& args = msg.HasMember("args") ? msg["args"] : empty_doc;
  auto serialized_req = json_to_serialized_service_request(service_type, args);
  
  // Get id value for callback
  std::string id_str;
  int id_int = 0;
  bool id_is_string = false;
  if (msg.HasMember("id")) {
    if (msg["id"].IsString()) {
      id_str = msg["id"].GetString();
      id_is_string = true;
    } else if (msg["id"].IsInt()) {
      id_int = msg["id"].GetInt();
    }
  }
  
  // Get timeout value (in seconds, 0 or negative means no timeout)
  double timeout_sec = get_double(msg, "timeout", 0.0);
  
  // Use shared atomic to track if response has been sent (for timeout handling)
  auto response_sent = std::make_shared<std::atomic<bool>>(false);
  
  using ServiceResponseFuture = rws::GenericClient::SharedFuture;
  auto response_received_callback = [this, id_str, id_int, id_is_string, service, service_type, response_sent](ServiceResponseFuture future) {
    // Check if timeout already sent a response
    bool expected = false;
    if (!response_sent->compare_exchange_strong(expected, true)) {
      // Timeout already sent a response, don't send another
      return;
    }
    
    rapidjson::StringBuffer resp_buf;
    RapidWriter resp_w(resp_buf);
    
    resp_w.StartObject();
    if (id_is_string) {
      resp_w.Key("id"); resp_w.String(id_str.c_str());
    } else if (id_int != 0) {
      resp_w.Key("id"); resp_w.Int(id_int);
    }
    resp_w.Key("op"); resp_w.String("service_response");
    resp_w.Key("service"); resp_w.String(service.c_str());
    resp_w.Key("values");
    serialized_service_response_to_json(service_type, future.get(), resp_w);
    resp_w.Key("result"); resp_w.Bool(true);
    resp_w.EndObject();

    std::string json_str(resp_buf.GetString(), resp_buf.GetSize());
    this->send_message(json_str);
  };
  clients_[service]->async_send_request(serialized_req, response_received_callback);

  // Set up timeout if requested
  if (timeout_sec > 0.0) {
    std::thread([this, id_str, id_int, id_is_string, service, response_sent, timeout_sec]() {
      std::this_thread::sleep_for(std::chrono::duration<double>(timeout_sec));
      
      // Check if real response already sent
      bool expected = false;
      if (!response_sent->compare_exchange_strong(expected, true)) {
        // Response already sent, don't send timeout
        return;
      }
      
      rapidjson::StringBuffer resp_buf;
      RapidWriter resp_w(resp_buf);
      
      resp_w.StartObject();
      if (id_is_string) {
        resp_w.Key("id"); resp_w.String(id_str.c_str());
      } else if (id_int != 0) {
        resp_w.Key("id"); resp_w.Int(id_int);
      }
      resp_w.Key("op"); resp_w.String("service_response");
      resp_w.Key("service"); resp_w.String(service.c_str());
      resp_w.Key("values"); resp_w.String("Timeout exceeded while waiting for service response");
      resp_w.Key("result"); resp_w.Bool(false);
      resp_w.EndObject();

      std::string json_str(resp_buf.GetString(), resp_buf.GetSize());
      this->send_message(json_str);
    }).detach();
  }

  // No immediate response per rosbridge spec - service_response comes asynchronously
  buf.Clear();
  return true;
}

GenericActionClient::SharedPtr ClientHandler::get_or_create_action_client(
  const std::string & action_name, const std::string & action_type)
{
  if (action_clients_.count(action_name) == 0) {
    action_clients_[action_name] = node_->create_generic_action_client(action_name, action_type);
  }
  return action_clients_[action_name];
}

bool ClientHandler::send_action_goal_rapid(const rapidjson::Document & msg, rapidjson::StringBuffer & buf, RapidWriter & w)
{
  if (!has_string(msg, "action")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No action name specified");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No action name specified");
    return true;
  }

  std::string action_name = msg["action"].GetString();

  if (!has_string(msg, "action_type")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No action_type specified");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No action_type specified");
    return true;
  }

  std::string action_type = msg["action_type"].GetString();
  
  // Get optional feedback flag
  bool feedback = get_bool(msg, "feedback", false);

  // Get goal arguments - use static empty doc to avoid copy constructor issues
  static const rapidjson::Document empty_action_doc;
  const rapidjson::Value* goal_args_ptr = &empty_action_doc;
  if (msg.HasMember("args") && msg["args"].IsObject()) {
    goal_args_ptr = &msg["args"];
  } else if (msg.HasMember("goal") && msg["goal"].IsObject()) {
    goal_args_ptr = &msg["goal"];
  }
  const rapidjson::Value& goal_args = *goal_args_ptr;

  try {
    auto action_client = get_or_create_action_client(action_name, action_type);

    // Get id for callbacks
    std::string id_str;
    int id_int = 0;
    bool id_is_string = false;
    if (msg.HasMember("id")) {
      if (msg["id"].IsString()) {
        id_str = msg["id"].GetString();
        id_is_string = true;
      } else if (msg["id"].IsInt()) {
        id_int = msg["id"].GetInt();
      }
    }

    // Callbacks for goal response, feedback, and result
    // Note: Per rosbridge protocol, we do NOT send an immediate action_result when the goal is accepted.
    // The action_result is only sent when the action completes (succeeds, fails, or is canceled).
    // We only log the acceptance status here for debugging purposes.
    auto goal_response_callback = [this, action_name](
      bool accepted, const GenericActionClient::GoalUUID & goal_id) {
      // Format goal_id as string for logging
      std::string goal_id_str;
      for (size_t i = 0; i < goal_id.size(); ++i) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02x", goal_id[i]);
        goal_id_str += buf;
      }
      RCLCPP_DEBUG(get_logger(), "Action goal %s %s for %s", 
        goal_id_str.c_str(), accepted ? "accepted" : "rejected", action_name.c_str());
    };

    GenericActionClient::FeedbackCallback feedback_callback = nullptr;
    if (feedback) {
      feedback_callback = [this, id_str, id_int, id_is_string, action_name, action_type](
        const GenericActionClient::GoalUUID & goal_id,
        GenericActionClient::SharedResponse feedback_msg) {
        // Format goal_id as string
        std::string goal_id_str;
        for (size_t i = 0; i < goal_id.size(); ++i) {
          char buf[3];
          snprintf(buf, sizeof(buf), "%02x", goal_id[i]);
          goal_id_str += buf;
        }

        rapidjson::StringBuffer resp_buf;
        RapidWriter resp_w(resp_buf);
        resp_w.StartObject();
        if (id_is_string) {
          resp_w.Key("id"); resp_w.String(id_str.c_str());
        } else if (id_int != 0) {
          resp_w.Key("id"); resp_w.Int(id_int);
        }
        resp_w.Key("op"); resp_w.String("action_feedback");
        resp_w.Key("action"); resp_w.String(action_name.c_str());
        resp_w.Key("goal_id"); resp_w.String(goal_id_str.c_str());
        resp_w.Key("values");
        
        // Deserialize feedback to JSON
        // action_type is like "example_interfaces/Fibonacci"
        // We need "example_interfaces/action/Fibonacci_Feedback"
        auto sep_pos = action_type.find_last_of('/');
        auto pkg_sep = action_type.find_first_of('/');
        std::string package_name = action_type.substr(0, pkg_sep);
        std::string action_name_only = action_type.substr(sep_pos + 1);
        std::string feedback_type = package_name + "/action/" + action_name_only + "_Feedback";
        
        rapidjson::Document feedback_doc;
        serialized_message_to_json(feedback_type, feedback_msg, feedback_doc);
        
        // Write the feedback document
        feedback_doc.Accept(resp_w);
        
        resp_w.EndObject();

        std::string json_str(resp_buf.GetString(), resp_buf.GetSize());
        this->send_message(json_str);
      };
    }

    auto result_callback = [this, id_str, id_int, id_is_string, action_name, action_type](
      const GenericActionClient::GoalUUID & goal_id,
      int8_t status,
      GenericActionClient::SharedResponse result_msg) {
      // Format goal_id as string
      std::string goal_id_str;
      for (size_t i = 0; i < goal_id.size(); ++i) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02x", goal_id[i]);
        goal_id_str += buf;
      }

      rapidjson::StringBuffer resp_buf;
      RapidWriter resp_w(resp_buf);
      resp_w.StartObject();
      if (id_is_string) {
        resp_w.Key("id"); resp_w.String(id_str.c_str());
      } else if (id_int != 0) {
        resp_w.Key("id"); resp_w.Int(id_int);
      }
      resp_w.Key("op"); resp_w.String("action_result");
      resp_w.Key("action"); resp_w.String(action_name.c_str());
      resp_w.Key("goal_id"); resp_w.String(goal_id_str.c_str());
      resp_w.Key("status"); resp_w.Int(status);  // Send as integer per rosbridge protocol
      resp_w.Key("values");
      
      // Deserialize result to JSON
      // action_type is like "example_interfaces/Fibonacci"
      // We need "example_interfaces/action/Fibonacci_Result"
      auto sep_pos = action_type.find_last_of('/');
      auto pkg_sep = action_type.find_first_of('/');
      std::string package_name = action_type.substr(0, pkg_sep);
      std::string action_name_only = action_type.substr(sep_pos + 1);
      std::string result_type = package_name + "/action/" + action_name_only + "_Result";
      
      rapidjson::Document result_doc;
      serialized_message_to_json(result_type, result_msg, result_doc);
      result_doc.Accept(resp_w);
      
      resp_w.Key("result"); resp_w.Bool(status == 4);  // true if succeeded
      resp_w.EndObject();

      std::string json_str(resp_buf.GetString(), resp_buf.GetSize());
      this->send_message(json_str);
    };

    auto goal_id = action_client->async_send_goal(
      goal_args, goal_response_callback, feedback_callback, result_callback);

    // No immediate response per rosbridge spec - action_result comes asynchronously
    (void)goal_id;  // Goal ID is returned via action_result callback
    buf.Clear();
    return true;

  } catch (const std::exception & e) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String((std::string("Failed to send action goal: ") + e.what()).c_str());
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "Failed to send action goal: %s", e.what());
    return true;
  }
}

bool ClientHandler::cancel_action_goal_rapid(const rapidjson::Document & msg, rapidjson::StringBuffer & buf, RapidWriter & w)
{
  if (!has_string(msg, "action")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No action name specified");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No action name specified");
    return true;
  }

  std::string action_name = msg["action"].GetString();

  if (action_clients_.count(action_name) == 0) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String(("No active action client for: " + action_name).c_str());
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No active action client for: %s", action_name.c_str());
    return true;
  }

  if (!has_string(msg, "goal_id")) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("No goal_id specified");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "No goal_id specified");
    return true;
  }

  std::string goal_id_str = msg["goal_id"].GetString();
  
  // Parse goal_id from hex string
  GenericActionClient::GoalUUID goal_id;
  if (goal_id_str.length() == 32) {  // 16 bytes * 2 hex chars
    for (size_t i = 0; i < 16; ++i) {
      goal_id[i] = static_cast<uint8_t>(
        std::stoul(goal_id_str.substr(i * 2, 2), nullptr, 16));
    }
  } else {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String("Invalid goal_id format");
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "Invalid goal_id format");
    return true;
  }

  try {
    // Get id for callback
    std::string id_str;
    int id_int = 0;
    bool id_is_string = false;
    if (msg.HasMember("id")) {
      if (msg["id"].IsString()) {
        id_str = msg["id"].GetString();
        id_is_string = true;
      } else if (msg["id"].IsInt()) {
        id_int = msg["id"].GetInt();
      }
    }
    
    action_clients_[action_name]->async_cancel_goal(
      goal_id,
      [this, id_str, id_int, id_is_string, action_name, goal_id_str](bool success) {
        // Per spec, cancellation confirmation comes via action_result with canceled status
        // But we send a status message if cancel request failed
        if (!success) {
          rapidjson::StringBuffer resp_buf;
          RapidWriter resp_w(resp_buf);
          resp_w.StartObject();
          if (id_is_string) {
            resp_w.Key("id"); resp_w.String(id_str.c_str());
          } else if (id_int != 0) {
            resp_w.Key("id"); resp_w.Int(id_int);
          }
          resp_w.Key("op"); resp_w.String("status");
          resp_w.Key("level"); resp_w.String("warning");
          resp_w.Key("msg"); resp_w.String("Failed to cancel action goal");
          resp_w.EndObject();

          std::string json_str(resp_buf.GetString(), resp_buf.GetSize());
          this->send_message(json_str);
        }
        // Success: action_result with "canceled" status will be sent by result_callback
      });

    // No immediate response per rosbridge spec
    buf.Clear();
    return true;

  } catch (const std::exception & e) {
    w.StartObject();
    write_id(msg, w);
    w.Key("op"); w.String("status");
    w.Key("level"); w.String("error");
    w.Key("msg"); w.String((std::string("Failed to cancel action goal: ") + e.what()).c_str());
    w.EndObject();
    RCLCPP_ERROR(get_logger(), "Failed to cancel action goal: %s", e.what());
    return true;
  }
}

}  // namespace rws
