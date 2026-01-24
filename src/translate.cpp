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

#include "rws/translate.hpp"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include "rclcpp/rclcpp.hpp"
#include "rosidl_typesupport_introspection_cpp/field_types.hpp"
#include "rosidl_typesupport_introspection_cpp/message_introspection.hpp"
#include "rosidl_typesupport_introspection_cpp/service_introspection.hpp"
#include "rws/typesupport_helpers.hpp"
#include "serdes.hpp"

namespace rws
{

static rclcpp::Logger get_logger() { return rclcpp::get_logger("rws::translate"); }

using rosidl_typesupport_introspection_cpp::MessageMember;
using rosidl_typesupport_introspection_cpp::MessageMembers;
using rosidl_typesupport_introspection_cpp::ServiceMembers;

// ============================================================================
// RapidJSON Deserialization (ROS message -> JSON)
// ============================================================================

template <typename T>
static void deserialize_field_rapid(cycdeser & deser, const MessageMember * member, RapidWriter & writer)
{
  T val;
  if (!member->is_array_) {
    deser >> val;
    if constexpr (std::is_same_v<T, bool>) {
      writer.Bool(val);
    } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t>) {
      writer.Int(val);
    } else if constexpr (std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t>) {
      writer.Uint(val);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      writer.Int64(val);
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      writer.Uint64(val);
    } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
      writer.Double(val);
    } else if constexpr (std::is_same_v<T, char>) {
      writer.Int(static_cast<int>(val));
    } else if constexpr (std::is_same_v<T, std::string>) {
      writer.String(val.c_str(), static_cast<rapidjson::SizeType>(val.size()));
    } else if constexpr (std::is_same_v<T, std::wstring>) {
      // Convert wstring to string (simplified)
      std::string narrow(val.begin(), val.end());
      writer.String(narrow.c_str(), static_cast<rapidjson::SizeType>(narrow.size()));
    }
  } else {
    writer.StartArray();
    size_t count;
    if (member->array_size_ && !member->is_upper_bound_) {
      count = member->array_size_;
    } else {
      uint32_t seq_size;
      deser >> seq_size;
      count = seq_size;
    }
    for (size_t i = 0; i < count; i++) {
      deser >> val;
      if constexpr (std::is_same_v<T, bool>) {
        writer.Bool(val);
      } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t>) {
        writer.Int(val);
      } else if constexpr (std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t>) {
        writer.Uint(val);
      } else if constexpr (std::is_same_v<T, int64_t>) {
        writer.Int64(val);
      } else if constexpr (std::is_same_v<T, uint64_t>) {
        writer.Uint64(val);
      } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
        writer.Double(val);
      } else if constexpr (std::is_same_v<T, char>) {
        writer.Int(static_cast<int>(val));
      } else if constexpr (std::is_same_v<T, std::string>) {
        writer.String(val.c_str(), static_cast<rapidjson::SizeType>(val.size()));
      } else if constexpr (std::is_same_v<T, std::wstring>) {
        std::string narrow(val.begin(), val.end());
        writer.String(narrow.c_str(), static_cast<rapidjson::SizeType>(narrow.size()));
      }
    }
    writer.EndArray();
  }
}

static void serialized_message_to_json_rapid(cycdeser & deser, const MessageMembers * members, RapidWriter & writer)
{
  writer.StartObject();
  for (uint32_t i = 0; i < members->member_count_; ++i) {
    const auto * member = members->members_ + i;
    writer.Key(member->name_);

    switch (member->type_id_) {
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_BOOL:
        deserialize_field_rapid<bool>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_BYTE:
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT8:
        deserialize_field_rapid<uint8_t>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_CHAR:
        deserialize_field_rapid<char>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT32:
        deserialize_field_rapid<float>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT64:
        deserialize_field_rapid<double>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT8:
        deserialize_field_rapid<int8_t>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT16:
        deserialize_field_rapid<int16_t>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT16:
        deserialize_field_rapid<uint16_t>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT32:
        deserialize_field_rapid<int32_t>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT32:
        deserialize_field_rapid<uint32_t>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT64:
        deserialize_field_rapid<int64_t>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT64:
        deserialize_field_rapid<uint64_t>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_STRING:
        deserialize_field_rapid<std::string>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_WSTRING:
        deserialize_field_rapid<std::wstring>(deser, member, writer);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE: {
        auto sub_members = (const MessageMembers *)member->members_->data;
        if (!member->is_array_) {
          serialized_message_to_json_rapid(deser, sub_members, writer);
        } else {
          writer.StartArray();
          size_t array_size = 0;
          if (member->array_size_ && !member->is_upper_bound_) {
            array_size = member->array_size_;
          } else {
            array_size = deser.deserialize_len(1);
          }
          for (size_t index = 0; index < array_size; ++index) {
            serialized_message_to_json_rapid(deser, sub_members, writer);
          }
          writer.EndArray();
        }
        break;
      }
      default:
        throw std::runtime_error("unknown type");
    }
  }
  writer.EndObject();
}

void serialized_message_to_json(const std::string & msg_type, ConstSharedMessage msg, rapidjson::Document & doc)
{
  auto library = rws::get_typesupport_library(msg_type, rws::ts_identifier);
  auto ts = rclcpp::get_typesupport_handle(msg_type, rws::ts_identifier, *library);
  auto members = static_cast<const MessageMembers *>(ts->data);
  auto rcl_msg = &msg->get_rcl_serialized_message();

  cycdeser deser(rcl_msg->buffer, rcl_msg->buffer_length);
  
  // Write to string buffer first, then parse into document
  rapidjson::StringBuffer buffer;
  RapidWriter writer(buffer);
  serialized_message_to_json_rapid(deser, members, writer);
  
  doc.Parse(buffer.GetString());
}

std::string build_publish_message(const std::string & topic, const std::string & msg_type, ConstSharedMessage msg)
{
  auto library = rws::get_typesupport_library(msg_type, rws::ts_identifier);
  auto ts = rclcpp::get_typesupport_handle(msg_type, rws::ts_identifier, *library);
  auto members = static_cast<const MessageMembers *>(ts->data);
  auto rcl_msg = &msg->get_rcl_serialized_message();

  cycdeser deser(rcl_msg->buffer, rcl_msg->buffer_length);
  
  rapidjson::StringBuffer buffer;
  // JSON is typically 2-4x larger than binary due to field names, quotes, etc.
  // Add fixed overhead for envelope {"op":"publish","topic":"...","msg":}
  buffer.Reserve(rcl_msg->buffer_length * 3 + 64);
  RapidWriter writer(buffer);
  
  writer.StartObject();
  writer.Key("op");
  writer.String("publish");
  writer.Key("topic");
  writer.String(topic.c_str(), static_cast<rapidjson::SizeType>(topic.size()));
  writer.Key("msg");
  serialized_message_to_json_rapid(deser, members, writer);
  writer.EndObject();
  
  return std::string(buffer.GetString(), buffer.GetSize());
}

// ============================================================================
// RapidJSON Serialization (JSON -> ROS message)
// ============================================================================

template <typename T>
static void serialize_field_rapid(
  const MessageMember * member, const rapidjson::Value & field, cycser & ser, T default_value)
{
  if (!member->is_array_) {
    if (field.IsNull()) {
      ser << default_value;
    } else {
      if constexpr (std::is_same_v<T, bool>) {
        ser << field.GetBool();
      } else if constexpr (std::is_same_v<T, int8_t>) {
        ser << static_cast<int8_t>(field.GetInt());
      } else if constexpr (std::is_same_v<T, int16_t>) {
        ser << static_cast<int16_t>(field.GetInt());
      } else if constexpr (std::is_same_v<T, int32_t>) {
        ser << field.GetInt();
      } else if constexpr (std::is_same_v<T, int64_t>) {
        ser << field.GetInt64();
      } else if constexpr (std::is_same_v<T, uint8_t>) {
        ser << static_cast<uint8_t>(field.GetUint());
      } else if constexpr (std::is_same_v<T, uint16_t>) {
        ser << static_cast<uint16_t>(field.GetUint());
      } else if constexpr (std::is_same_v<T, uint32_t>) {
        ser << field.GetUint();
      } else if constexpr (std::is_same_v<T, uint64_t>) {
        ser << field.GetUint64();
      } else if constexpr (std::is_same_v<T, float>) {
        ser << static_cast<float>(field.GetDouble());
      } else if constexpr (std::is_same_v<T, double>) {
        ser << field.GetDouble();
      } else if constexpr (std::is_same_v<T, char>) {
        ser << static_cast<char>(field.GetInt());
      } else if constexpr (std::is_same_v<T, std::string>) {
        ser << std::string(field.GetString(), field.GetStringLength());
      } else if constexpr (std::is_same_v<T, std::wstring>) {
        std::string narrow(field.GetString(), field.GetStringLength());
        ser << std::wstring(narrow.begin(), narrow.end());
      }
    }
  } else if (member->array_size_ && !member->is_upper_bound_) {
    // Fixed-size array
    for (size_t i = 0; i < member->array_size_; i++) {
      if (field.IsNull() || !field.IsArray() || i >= field.Size() || field[i].IsNull()) {
        ser << default_value;
      } else {
        const auto& elem = field[static_cast<rapidjson::SizeType>(i)];
        if constexpr (std::is_same_v<T, bool>) {
          ser << elem.GetBool();
        } else if constexpr (std::is_same_v<T, int8_t>) {
          ser << static_cast<int8_t>(elem.GetInt());
        } else if constexpr (std::is_same_v<T, int16_t>) {
          ser << static_cast<int16_t>(elem.GetInt());
        } else if constexpr (std::is_same_v<T, int32_t>) {
          ser << elem.GetInt();
        } else if constexpr (std::is_same_v<T, int64_t>) {
          ser << elem.GetInt64();
        } else if constexpr (std::is_same_v<T, uint8_t>) {
          ser << static_cast<uint8_t>(elem.GetUint());
        } else if constexpr (std::is_same_v<T, uint16_t>) {
          ser << static_cast<uint16_t>(elem.GetUint());
        } else if constexpr (std::is_same_v<T, uint32_t>) {
          ser << elem.GetUint();
        } else if constexpr (std::is_same_v<T, uint64_t>) {
          ser << elem.GetUint64();
        } else if constexpr (std::is_same_v<T, float>) {
          ser << static_cast<float>(elem.GetDouble());
        } else if constexpr (std::is_same_v<T, double>) {
          ser << elem.GetDouble();
        } else if constexpr (std::is_same_v<T, char>) {
          ser << static_cast<char>(elem.GetInt());
        } else if constexpr (std::is_same_v<T, std::string>) {
          ser << std::string(elem.GetString(), elem.GetStringLength());
        } else if constexpr (std::is_same_v<T, std::wstring>) {
          std::string narrow(elem.GetString(), elem.GetStringLength());
          ser << std::wstring(narrow.begin(), narrow.end());
        }
      }
    }
  } else {
    // Dynamic array
    uint32_t seq_size = field.IsArray() ? field.Size() : 0;
    ser << seq_size;

    for (uint32_t i = 0; i < seq_size; i++) {
      const auto& elem = field[i];
      if (elem.IsNull()) {
        ser << default_value;
      } else {
        if constexpr (std::is_same_v<T, bool>) {
          ser << elem.GetBool();
        } else if constexpr (std::is_same_v<T, int8_t>) {
          ser << static_cast<int8_t>(elem.GetInt());
        } else if constexpr (std::is_same_v<T, int16_t>) {
          ser << static_cast<int16_t>(elem.GetInt());
        } else if constexpr (std::is_same_v<T, int32_t>) {
          ser << elem.GetInt();
        } else if constexpr (std::is_same_v<T, int64_t>) {
          ser << elem.GetInt64();
        } else if constexpr (std::is_same_v<T, uint8_t>) {
          ser << static_cast<uint8_t>(elem.GetUint());
        } else if constexpr (std::is_same_v<T, uint16_t>) {
          ser << static_cast<uint16_t>(elem.GetUint());
        } else if constexpr (std::is_same_v<T, uint32_t>) {
          ser << elem.GetUint();
        } else if constexpr (std::is_same_v<T, uint64_t>) {
          ser << elem.GetUint64();
        } else if constexpr (std::is_same_v<T, float>) {
          ser << static_cast<float>(elem.GetDouble());
        } else if constexpr (std::is_same_v<T, double>) {
          ser << elem.GetDouble();
        } else if constexpr (std::is_same_v<T, char>) {
          ser << static_cast<char>(elem.GetInt());
        } else if constexpr (std::is_same_v<T, std::string>) {
          ser << std::string(elem.GetString(), elem.GetStringLength());
        } else if constexpr (std::is_same_v<T, std::wstring>) {
          std::string narrow(elem.GetString(), elem.GetStringLength());
          ser << std::wstring(narrow.begin(), narrow.end());
        }
      }
    }
  }
}

// Null value for cases where field is not found
static rapidjson::Value g_null_value;

static const rapidjson::Value& get_member_value(const rapidjson::Value& j, const char* name)
{
  if (j.IsObject() && j.HasMember(name)) {
    return j[name];
  }
  return g_null_value;
}

static void json_to_serialized_message_rapid(cycser & ser, const MessageMembers * members, const rapidjson::Value & j)
{
  for (uint32_t i = 0; i < members->member_count_; ++i) {
    const auto member = members->members_ + i;

    if (strcmp(member->name_, "structure_needs_at_least_one_member") == 0) {
      continue;
    }

    const rapidjson::Value& field = get_member_value(j, member->name_);
    
    if (field.IsNull() && !j.IsNull()) {
      RCLCPP_INFO(
        get_logger(), "Field '%s' is not in json, default: %p", member->name_,
        member->default_value_);
    }

    switch (member->type_id_) {
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_BOOL:
        serialize_field_rapid<bool>(member, field, ser, false);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_BYTE:
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT8:
        serialize_field_rapid<uint8_t>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_CHAR:
        serialize_field_rapid<char>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT32:
        serialize_field_rapid<float>(member, field, ser, 0.0f);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT64:
        serialize_field_rapid<double>(member, field, ser, 0.0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT8:
        serialize_field_rapid<int8_t>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT16:
        serialize_field_rapid<int16_t>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT16:
        serialize_field_rapid<uint16_t>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT32:
        serialize_field_rapid<int32_t>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT32:
        serialize_field_rapid<uint32_t>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT64:
        serialize_field_rapid<int64_t>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT64:
        serialize_field_rapid<uint64_t>(member, field, ser, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_STRING:
        serialize_field_rapid<std::string>(member, field, ser, std::string(""));
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_WSTRING:
        serialize_field_rapid<std::wstring>(member, field, ser, std::wstring(L""));
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE: {
        auto sub_members = static_cast<const MessageMembers *>(member->members_->data);
        if (!member->is_array_) {
          json_to_serialized_message_rapid(ser, sub_members, field);
        } else {
          size_t array_size = 0;

          if (member->array_size_ && !member->is_upper_bound_) {
            array_size = member->array_size_;
          } else {
            if (field.IsArray()) {
              array_size = field.Size();
            }
            // Serialize length
            ser << (uint32_t)array_size;
          }

          for (size_t index = 0; index < array_size; ++index) {
            const rapidjson::Value& elem = field.IsArray() && index < field.Size() 
              ? field[static_cast<rapidjson::SizeType>(index)] 
              : g_null_value;
            json_to_serialized_message_rapid(ser, sub_members, elem);
          }
        }
        break;
      }
      default:
        throw std::runtime_error("unknown type");
    }
  }
}

SharedMessage json_to_serialized_message(const std::string & msg_type, const rapidjson::Value & value)
{
  auto library = rws::get_typesupport_library(msg_type, rws::ts_identifier);
  auto ts = rclcpp::get_typesupport_handle(msg_type, rws::ts_identifier, *library);
  auto members = static_cast<const MessageMembers *>(ts->data);

  auto msg = std::make_shared<rclcpp::SerializedMessage>(0);
  auto rcl_msg = &msg->get_rcl_serialized_message();
  std::vector<unsigned char> buffer;
  cycser ser(buffer);

  json_to_serialized_message_rapid(ser, members, value);

  msg->reserve(buffer.size());
  memcpy(rcl_msg->buffer, buffer.data(), buffer.size());
  rcl_msg->buffer_length = buffer.size();

  return msg;
}

SharedMessage json_to_serialized_service_request(const std::string & srv_type, const rapidjson::Value & value)
{
  auto library = rws::get_typesupport_library(srv_type, rws::ts_identifier);
  auto ts = rws::get_service_typesupport_handle(srv_type, rws::ts_identifier, *library);
  auto srv_members = static_cast<const ServiceMembers *>(ts->data);
  auto request_members = srv_members->request_members_;

  auto msg = std::make_shared<rclcpp::SerializedMessage>(0);
  auto rcl_msg = &msg->get_rcl_serialized_message();
  std::vector<unsigned char> buffer;
  cycser ser(buffer);

  json_to_serialized_message_rapid(ser, request_members, value);

  msg->reserve(buffer.size());
  memcpy(rcl_msg->buffer, buffer.data(), buffer.size());
  rcl_msg->buffer_length = buffer.size();

  return msg;
}

void serialized_service_response_to_json(const std::string & srv_type, ConstSharedMessage msg, RapidWriter & writer)
{
  auto library = rws::get_typesupport_library(srv_type, rws::ts_identifier);
  auto ts = rws::get_service_typesupport_handle(srv_type, rws::ts_identifier, *library);
  auto srv_members = static_cast<const ServiceMembers *>(ts->data);
  auto response_members = srv_members->response_members_;

  auto rcl_msg = &msg->get_rcl_serialized_message();

  cycdeser deser(rcl_msg->buffer, rcl_msg->buffer_length);
  serialized_message_to_json_rapid(deser, response_members, writer);
}

// ============================================================================
// Direct ROS message population from JSON (for actions)
// ============================================================================

// Helper function to set a primitive field value
template <typename T>
static void set_field_value_rapid(void * field_ptr, const rapidjson::Value & value, T default_val)
{
  if (value.IsNull()) {
    *static_cast<T *>(field_ptr) = default_val;
  } else {
    if constexpr (std::is_same_v<T, bool>) {
      *static_cast<T *>(field_ptr) = value.GetBool();
    } else if constexpr (std::is_same_v<T, int8_t>) {
      *static_cast<T *>(field_ptr) = static_cast<int8_t>(value.GetInt());
    } else if constexpr (std::is_same_v<T, int16_t>) {
      *static_cast<T *>(field_ptr) = static_cast<int16_t>(value.GetInt());
    } else if constexpr (std::is_same_v<T, int32_t>) {
      *static_cast<T *>(field_ptr) = value.GetInt();
    } else if constexpr (std::is_same_v<T, int64_t>) {
      *static_cast<T *>(field_ptr) = value.GetInt64();
    } else if constexpr (std::is_same_v<T, uint8_t>) {
      *static_cast<T *>(field_ptr) = static_cast<uint8_t>(value.GetUint());
    } else if constexpr (std::is_same_v<T, uint16_t>) {
      *static_cast<T *>(field_ptr) = static_cast<uint16_t>(value.GetUint());
    } else if constexpr (std::is_same_v<T, uint32_t>) {
      *static_cast<T *>(field_ptr) = value.GetUint();
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      *static_cast<T *>(field_ptr) = value.GetUint64();
    } else if constexpr (std::is_same_v<T, float>) {
      *static_cast<T *>(field_ptr) = static_cast<float>(value.GetDouble());
    } else if constexpr (std::is_same_v<T, double>) {
      *static_cast<T *>(field_ptr) = value.GetDouble();
    } else if constexpr (std::is_same_v<T, char>) {
      *static_cast<T *>(field_ptr) = static_cast<char>(value.GetInt());
    }
  }
}

// Helper function to set an array field
template <typename T>
static void set_array_field_rapid(
  const MessageMember * member, void * field_ptr, const rapidjson::Value & value, T default_val)
{
  if (!member->is_array_) {
    set_field_value_rapid<T>(field_ptr, value, default_val);
  } else if (member->array_size_ && !member->is_upper_bound_) {
    // Fixed-size array
    T * data = static_cast<T *>(field_ptr);
    for (size_t i = 0; i < member->array_size_; i++) {
      if (value.IsArray() && i < value.Size() && !value[static_cast<rapidjson::SizeType>(i)].IsNull()) {
        const auto& elem = value[static_cast<rapidjson::SizeType>(i)];
        if constexpr (std::is_same_v<T, bool>) {
          data[i] = elem.GetBool();
        } else if constexpr (std::is_same_v<T, int8_t>) {
          data[i] = static_cast<int8_t>(elem.GetInt());
        } else if constexpr (std::is_same_v<T, int16_t>) {
          data[i] = static_cast<int16_t>(elem.GetInt());
        } else if constexpr (std::is_same_v<T, int32_t>) {
          data[i] = elem.GetInt();
        } else if constexpr (std::is_same_v<T, int64_t>) {
          data[i] = elem.GetInt64();
        } else if constexpr (std::is_same_v<T, uint8_t>) {
          data[i] = static_cast<uint8_t>(elem.GetUint());
        } else if constexpr (std::is_same_v<T, uint16_t>) {
          data[i] = static_cast<uint16_t>(elem.GetUint());
        } else if constexpr (std::is_same_v<T, uint32_t>) {
          data[i] = elem.GetUint();
        } else if constexpr (std::is_same_v<T, uint64_t>) {
          data[i] = elem.GetUint64();
        } else if constexpr (std::is_same_v<T, float>) {
          data[i] = static_cast<float>(elem.GetDouble());
        } else if constexpr (std::is_same_v<T, double>) {
          data[i] = elem.GetDouble();
        } else if constexpr (std::is_same_v<T, char>) {
          data[i] = static_cast<char>(elem.GetInt());
        }
      } else {
        data[i] = default_val;
      }
    }
  } else {
    // Dynamic array (vector)
    auto * vec = static_cast<std::vector<T> *>(field_ptr);
    vec->clear();
    if (value.IsArray()) {
      vec->reserve(value.Size());
      for (rapidjson::SizeType i = 0; i < value.Size(); i++) {
        const auto& elem = value[i];
        if (elem.IsNull()) {
          vec->push_back(default_val);
        } else {
          if constexpr (std::is_same_v<T, bool>) {
            vec->push_back(elem.GetBool());
          } else if constexpr (std::is_same_v<T, int8_t>) {
            vec->push_back(static_cast<int8_t>(elem.GetInt()));
          } else if constexpr (std::is_same_v<T, int16_t>) {
            vec->push_back(static_cast<int16_t>(elem.GetInt()));
          } else if constexpr (std::is_same_v<T, int32_t>) {
            vec->push_back(elem.GetInt());
          } else if constexpr (std::is_same_v<T, int64_t>) {
            vec->push_back(elem.GetInt64());
          } else if constexpr (std::is_same_v<T, uint8_t>) {
            vec->push_back(static_cast<uint8_t>(elem.GetUint()));
          } else if constexpr (std::is_same_v<T, uint16_t>) {
            vec->push_back(static_cast<uint16_t>(elem.GetUint()));
          } else if constexpr (std::is_same_v<T, uint32_t>) {
            vec->push_back(elem.GetUint());
          } else if constexpr (std::is_same_v<T, uint64_t>) {
            vec->push_back(elem.GetUint64());
          } else if constexpr (std::is_same_v<T, float>) {
            vec->push_back(static_cast<float>(elem.GetDouble()));
          } else if constexpr (std::is_same_v<T, double>) {
            vec->push_back(elem.GetDouble());
          } else if constexpr (std::is_same_v<T, char>) {
            vec->push_back(static_cast<char>(elem.GetInt()));
          }
        }
      }
    }
  }
}

// Forward declaration
static void populate_message_from_json_rapid(
  const rapidjson::Value & j, const MessageMembers * members, void * message);

// Helper for string fields
static void set_string_field_rapid(const MessageMember * member, void * field_ptr, const rapidjson::Value & value)
{
  if (!member->is_array_) {
    auto * str = static_cast<std::string *>(field_ptr);
    *str = value.IsNull() ? "" : std::string(value.GetString(), value.GetStringLength());
  } else if (member->array_size_ && !member->is_upper_bound_) {
    // Fixed-size array of strings
    auto * data = static_cast<std::string *>(field_ptr);
    for (size_t i = 0; i < member->array_size_; i++) {
      if (value.IsArray() && i < value.Size() && !value[static_cast<rapidjson::SizeType>(i)].IsNull()) {
        const auto& elem = value[static_cast<rapidjson::SizeType>(i)];
        data[i] = std::string(elem.GetString(), elem.GetStringLength());
      } else {
        data[i] = "";
      }
    }
  } else {
    // Dynamic array of strings
    auto * vec = static_cast<std::vector<std::string> *>(field_ptr);
    vec->clear();
    if (value.IsArray()) {
      vec->reserve(value.Size());
      for (rapidjson::SizeType i = 0; i < value.Size(); i++) {
        const auto& elem = value[i];
        vec->push_back(elem.IsNull() ? "" : std::string(elem.GetString(), elem.GetStringLength()));
      }
    }
  }
}

static void populate_message_from_json_rapid(
  const rapidjson::Value & j, const MessageMembers * members, void * message)
{
  for (uint32_t i = 0; i < members->member_count_; ++i) {
    const auto * member = members->members_ + i;

    if (strcmp(member->name_, "structure_needs_at_least_one_member") == 0) {
      continue;
    }

    void * field_ptr = static_cast<uint8_t *>(message) + member->offset_;

    const rapidjson::Value& field = get_member_value(j, member->name_);
    
    if (field.IsNull()) {
      RCLCPP_INFO(
        get_logger(), "Field '%s' is not in json, using default", member->name_);
      // Field not in JSON, leave as default (already initialized)
      // For nested messages, we still need to recurse with empty JSON
      if (member->type_id_ == rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE) {
        auto sub_members = static_cast<const MessageMembers *>(member->members_->data);
        if (!member->is_array_) {
          populate_message_from_json_rapid(g_null_value, sub_members, field_ptr);
        }
      }
      continue;
    }

    switch (member->type_id_) {
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_BOOL:
        set_array_field_rapid<bool>(member, field_ptr, field, false);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_BYTE:
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT8:
        set_array_field_rapid<uint8_t>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_CHAR:
        set_array_field_rapid<char>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT32:
        set_array_field_rapid<float>(member, field_ptr, field, 0.0f);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT64:
        set_array_field_rapid<double>(member, field_ptr, field, 0.0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT8:
        set_array_field_rapid<int8_t>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT16:
        set_array_field_rapid<int16_t>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT16:
        set_array_field_rapid<uint16_t>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT32:
        set_array_field_rapid<int32_t>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT32:
        set_array_field_rapid<uint32_t>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT64:
        set_array_field_rapid<int64_t>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT64:
        set_array_field_rapid<uint64_t>(member, field_ptr, field, 0);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_STRING:
        set_string_field_rapid(member, field_ptr, field);
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE: {
        auto sub_members = static_cast<const MessageMembers *>(member->members_->data);
        if (!member->is_array_) {
          populate_message_from_json_rapid(field.IsNull() ? g_null_value : field, sub_members, field_ptr);
        } else if (member->array_size_ && !member->is_upper_bound_) {
          // Fixed-size array of messages
          for (size_t idx = 0; idx < member->array_size_; idx++) {
            void * elem_ptr = member->get_function(field_ptr, idx);
            const rapidjson::Value& elem_json = (field.IsArray() && idx < field.Size()) 
              ? field[static_cast<rapidjson::SizeType>(idx)] 
              : g_null_value;
            populate_message_from_json_rapid(elem_json, sub_members, elem_ptr);
          }
        } else {
          // Dynamic array of messages - use resize_function
          size_t arr_size = field.IsArray() ? field.Size() : 0;
          if (member->resize_function) {
            member->resize_function(field_ptr, arr_size);
          }
          for (size_t idx = 0; idx < arr_size; idx++) {
            void * elem_ptr = member->get_function(field_ptr, idx);
            populate_message_from_json_rapid(field[static_cast<rapidjson::SizeType>(idx)], sub_members, elem_ptr);
          }
        }
        break;
      }
      default:
        RCLCPP_WARN(get_logger(), "Unknown field type: %d", member->type_id_);
        break;
    }
  }
}

void json_to_ros_message(const rapidjson::Value & value, const MessageMembers * members, void * message)
{
  populate_message_from_json_rapid(value, members, message);
}

// ============================================================================
// Message metadata generation
// ============================================================================

static std::string members_to_meta(
  const MessageMembers * members, std::map<std::string, std::string> & deps,
  bool rosbridge_compatible = false, std::string parent_name = "")
{
  std::stringstream s;

  for (uint32_t i = 0; i < members->member_count_; ++i) {
    const auto member = members->members_ + i;
    std::string name = member->name_;
    if (name == "structure_needs_at_least_one_member") {
      continue;
    }
    if (rosbridge_compatible && name == "nanosec" && parent_name == "stamp") {
      name = "nsec";
    }

    std::string b =
      member->is_array_
        ? !member->array_size_ ? "[]" : "[" + std::to_string(member->array_size_) + "]"
        : "";

    switch (member->type_id_) {
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_BOOL:
        s << "bool" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_BYTE:
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_CHAR:
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT8:
        s << "uint8" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT8:
        s << "int8" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT32:
        s << "float32" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_FLOAT64:
        s << "float64" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT16:
        s << "int16" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT16:
        s << "uint16" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT32:
        s << "int32" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT32:
        s << "uint32" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_INT64:
        s << "int64" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_UINT64:
        s << "uint64" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_STRING:
        s << "string" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_WSTRING:
        s << "wstring" << b << " " << name << "\n";
        break;
      case rosidl_typesupport_introspection_cpp::ROS_TYPE_MESSAGE: {
        auto sub_members = static_cast<const MessageMembers *>(member->members_->data);
        auto msg_path = get_type_from_message_members(sub_members);

        s << msg_path << b << " " << name << "\n";

        if (deps.count(msg_path) == 0) {
          deps[msg_path] = members_to_meta(sub_members, deps, rosbridge_compatible, name);
        }
      } break;
      default:
        throw std::runtime_error("unknown type");
    }
  }

  return s.str();
}

std::string generate_message_meta(const std::string & msg_type, bool rosbridge_compatible)
{
  auto library = rws::get_typesupport_library(msg_type, rws::ts_identifier);
  auto ts = rclcpp::get_typesupport_handle(msg_type, rws::ts_identifier, *library);
  auto members = static_cast<const MessageMembers *>(ts->data);

  std::stringstream s;
  std::map<std::string, std::string> deps;
  s << members_to_meta(members, deps, rosbridge_compatible);

  for (auto & dep : deps) {
    s << "============\n";
    s << "MSG: " << dep.first << "\n";
    s << dep.second;
  }

  return s.str();
}

}  // namespace rws
