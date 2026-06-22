#ifndef RWS__TRANSLATE_TEST_HPP_
#define RWS__TRANSLATE_TEST_HPP_

#include "rws/translate.hpp"

#include <gtest/gtest.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <rclcpp/rclcpp.hpp>

#include <cstring>
#include <string>
#include <vector>

#include "rcl_interfaces/msg/log.hpp"
#include "rclcpp/serialization.hpp"
#include "rws/base64.hpp"

namespace rws
{

class TranslateFixture : public testing::Test
{
public:
  TranslateFixture() {}

  void SetUp() override {}

  void TearDown() override {}

protected:
};

TEST_F(TranslateFixture, DescribeStdMsgsString)
{
  std::string msg_desc = generate_message_meta("std_msgs/msg/String");
  EXPECT_EQ(msg_desc, "string data\n");
}

TEST_F(TranslateFixture, DescribeStdMsgsBuiltins)
{
  auto msg_desc = generate_message_meta("test_msgs/msg/Builtins");

  std::string expected =
    "builtin_interfaces/msg/Duration duration_value\n"
    "builtin_interfaces/msg/Time time_value\n"
    "============\n"
    "MSG: builtin_interfaces/msg/Duration\n"
    "int32 sec\n"
    "uint32 nanosec\n"
    "============\n"
    "MSG: builtin_interfaces/msg/Time\n"
    "int32 sec\n"
    "uint32 nanosec\n";

  EXPECT_EQ(msg_desc, expected);
}

TEST_F(TranslateFixture, RosbridgeCompatibleDescriptionHasRenamedNanosecondsField)
{
  auto msg_desc = generate_message_meta("rcl_interfaces/msg/Log", true);

  std::string expected =
    "builtin_interfaces/msg/Time stamp\n"
    "uint8 level\n"
    "string name\n"
    "string msg\n"
    "string file\n"
    "string function\n"
    "uint32 line\n"
    "============\n"
    "MSG: builtin_interfaces/msg/Time\n"
    "int32 sec\n"
    "uint32 nsec\n";  // <--- renamed from nanosec to nsec

  EXPECT_EQ(msg_desc, expected);
}

TEST_F(TranslateFixture, DescribeTestMsgsUnboundedSequence)
{
  auto msg_desc = generate_message_meta("test_msgs/msg/UnboundedSequences");

  std::string expected =
    "bool[] bool_values\n"
    "uint8[] byte_values\n"
    "uint8[] char_values\n"
    "float32[] float32_values\n"
    "float64[] float64_values\n"
    "int8[] int8_values\n"
    "uint8[] uint8_values\n"
    "int16[] int16_values\n"
    "uint16[] uint16_values\n"
    "int32[] int32_values\n"
    "uint32[] uint32_values\n"
    "int64[] int64_values\n"
    "uint64[] uint64_values\n"
    "string[] string_values\n"
    "test_msgs/msg/BasicTypes[] basic_types_values\n"
    "test_msgs/msg/Constants[] constants_values\n"
    "test_msgs/msg/Defaults[] defaults_values\n"
    "bool[] bool_values_default\n"
    "uint8[] byte_values_default\n"
    "uint8[] char_values_default\n"
    "float32[] float32_values_default\n"
    "float64[] float64_values_default\n"
    "int8[] int8_values_default\n"
    "uint8[] uint8_values_default\n"
    "int16[] int16_values_default\n"
    "uint16[] uint16_values_default\n"
    "int32[] int32_values_default\n"
    "uint32[] uint32_values_default\n"
    "int64[] int64_values_default\n"
    "uint64[] uint64_values_default\n"
    "string[] string_values_default\n"
    "int32 alignment_check\n"
    "============\n"
    "MSG: test_msgs/msg/BasicTypes\n"
    "bool bool_value\n"
    "uint8 byte_value\n"
    "uint8 char_value\n"
    "float32 float32_value\n"
    "float64 float64_value\n"
    "int8 int8_value\n"
    "uint8 uint8_value\n"
    "int16 int16_value\n"
    "uint16 uint16_value\n"
    "int32 int32_value\n"
    "uint32 uint32_value\n"
    "int64 int64_value\n"
    "uint64 uint64_value\n"
    "============\n"
    "MSG: test_msgs/msg/Constants\n"
    "============\n"
    "MSG: test_msgs/msg/Defaults\n"
    "bool bool_value\n"
    "uint8 byte_value\n"
    "uint8 char_value\n"
    "float32 float32_value\n"
    "float64 float64_value\n"
    "int8 int8_value\n"
    "uint8 uint8_value\n"
    "int16 int16_value\n"
    "uint16 uint16_value\n"
    "int32 int32_value\n"
    "uint32 uint32_value\n"
    "int64 int64_value\n"
    "uint64 uint64_value\n";

  EXPECT_EQ(msg_desc, expected);
}

TEST_F(TranslateFixture, DescribeTestMsgsArrays)
{
  auto msg_desc = generate_message_meta("test_msgs/msg/Arrays");

  std::string expected =
    "bool[3] bool_values\n"
    "uint8[3] byte_values\n"
    "uint8[3] char_values\n"
    "float32[3] float32_values\n"
    "float64[3] float64_values\n"
    "int8[3] int8_values\n"
    "uint8[3] uint8_values\n"
    "int16[3] int16_values\n"
    "uint16[3] uint16_values\n"
    "int32[3] int32_values\n"
    "uint32[3] uint32_values\n"
    "int64[3] int64_values\n"
    "uint64[3] uint64_values\n"
    "string[3] string_values\n"
    "test_msgs/msg/BasicTypes[3] basic_types_values\n"
    "test_msgs/msg/Constants[3] constants_values\n"
    "test_msgs/msg/Defaults[3] defaults_values\n"
    "bool[3] bool_values_default\n"
    "uint8[3] byte_values_default\n"
    "uint8[3] char_values_default\n"
    "float32[3] float32_values_default\n"
    "float64[3] float64_values_default\n"
    "int8[3] int8_values_default\n"
    "uint8[3] uint8_values_default\n"
    "int16[3] int16_values_default\n"
    "uint16[3] uint16_values_default\n"
    "int32[3] int32_values_default\n"
    "uint32[3] uint32_values_default\n"
    "int64[3] int64_values_default\n"
    "uint64[3] uint64_values_default\n"
    "string[3] string_values_default\n"
    "int32 alignment_check\n"
    "============\n"
    "MSG: test_msgs/msg/BasicTypes\n"
    "bool bool_value\n"
    "uint8 byte_value\n"
    "uint8 char_value\n"
    "float32 float32_value\n"
    "float64 float64_value\n"
    "int8 int8_value\n"
    "uint8 uint8_value\n"
    "int16 int16_value\n"
    "uint16 uint16_value\n"
    "int32 int32_value\n"
    "uint32 uint32_value\n"
    "int64 int64_value\n"
    "uint64 uint64_value\n"
    "============\n"
    "MSG: test_msgs/msg/Constants\n"
    "============\n"
    "MSG: test_msgs/msg/Defaults\n"
    "bool bool_value\n"
    "uint8 byte_value\n"
    "uint8 char_value\n"
    "float32 float32_value\n"
    "float64 float64_value\n"
    "int8 int8_value\n"
    "uint8 uint8_value\n"
    "int16 int16_value\n"
    "uint16 uint16_value\n"
    "int32 int32_value\n"
    "uint32 uint32_value\n"
    "int64 int64_value\n"
    "uint64 uint64_value\n";

  EXPECT_EQ(msg_desc, expected);
}

TEST_F(TranslateFixture, DeserializeRclInterfacesLogMessage)
{
  auto log_msg = std::make_shared<rcl_interfaces::msg::Log>();
  log_msg->level = 1;
  log_msg->name = "client_handler";
  log_msg->msg =
    "process_message: "
    "{\"args\":{},\"id\":\"call_service:/rosapi/"
    "topics_and_raw_types:1\",\"op\":\"call_service\",\"service\":\"/rosapi/"
    "topics_and_raw_types\",\"type\":\"rosapi/TopicsAndRawTypes\"}";
  log_msg->file = "/src/client_handler.cpp";
  log_msg->function = "process_message";
  log_msg->line = 1;

  auto serialized_msg = std::make_shared<rclcpp::SerializedMessage>();

  static rclcpp::Serialization<rcl_interfaces::msg::Log> serializer;
  serializer.serialize_message(log_msg.get(), &*serialized_msg);

  rapidjson::Document log_msg_doc;
  rws::serialized_message_to_json("rcl_interfaces/msg/Log", serialized_msg, log_msg_doc);
  
  // Compare individual fields rather than full JSON string (key order may differ)
  EXPECT_TRUE(log_msg_doc.HasMember("stamp"));
  EXPECT_TRUE(log_msg_doc["stamp"].HasMember("sec"));
  EXPECT_EQ(log_msg_doc["stamp"]["sec"].GetInt(), 0);
  EXPECT_TRUE(log_msg_doc["stamp"].HasMember("nanosec"));
  EXPECT_EQ(log_msg_doc["stamp"]["nanosec"].GetInt(), 0);
  EXPECT_EQ(log_msg_doc["level"].GetInt(), 1);
  EXPECT_STREQ(log_msg_doc["name"].GetString(), "client_handler");
  EXPECT_STREQ(log_msg_doc["file"].GetString(), "/src/client_handler.cpp");
  EXPECT_STREQ(log_msg_doc["function"].GetString(), "process_message");
  EXPECT_EQ(log_msg_doc["line"].GetInt(), 1);
  // Verify msg field contains the expected content
  std::string msg_str = log_msg_doc["msg"].GetString();
  EXPECT_TRUE(msg_str.find("call_service") != std::string::npos);
  EXPECT_TRUE(msg_str.find("/rosapi/topics_and_raw_types") != std::string::npos);
}

TEST_F(TranslateFixture, DescribeMsgsNestedType)
{
  auto msg_desc = generate_message_meta("sensor_msgs/Image");

  std::string expected =
    "std_msgs/msg/Header header\n"
    "uint32 height\n"
    "uint32 width\n"
    "string encoding\n"
    "uint8 is_bigendian\n"
    "uint32 step\n"
    "uint8[] data\n"
    "============\n"
    "MSG: builtin_interfaces/msg/Time\n"
    "int32 sec\n"
    "uint32 nanosec\n"
    "============\n"
    "MSG: std_msgs/msg/Header\n"
    "builtin_interfaces/msg/Time stamp\n"
    "string frame_id\n";

  EXPECT_EQ(msg_desc, expected);
}

// ============================================================================
// Base64 byte-array encoding (rosbridge protocol §3.1)
// ============================================================================

namespace
{

// Build a JSON document with a single member set to a number array of bytes.
rapidjson::Document make_byte_array_doc(const char * field, const std::vector<int> & bytes)
{
  rapidjson::Document doc;
  doc.SetObject();
  auto & alloc = doc.GetAllocator();
  rapidjson::Value arr(rapidjson::kArrayType);
  for (int b : bytes) {
    arr.PushBack(b, alloc);
  }
  doc.AddMember(rapidjson::StringRef(field), arr, alloc);
  return doc;
}

// Build a JSON document with a single member set to a (base64) string.
rapidjson::Document make_string_doc(const char * field, const char * value)
{
  rapidjson::Document doc;
  doc.SetObject();
  auto & alloc = doc.GetAllocator();
  doc.AddMember(rapidjson::StringRef(field), rapidjson::StringRef(value), alloc);
  return doc;
}

bool serialized_equal(const rws::SharedMessage & a, const rws::SharedMessage & b)
{
  const auto & ra = a->get_rcl_serialized_message();
  const auto & rb = b->get_rcl_serialized_message();
  if (ra.buffer_length != rb.buffer_length) {
    return false;
  }
  return std::memcmp(ra.buffer, rb.buffer, ra.buffer_length) == 0;
}

}  // namespace

// --- Step 2: self-contained base64 codec unit tests ---

TEST_F(TranslateFixture, Base64EncodeSpecExamples)
{
  const unsigned char zeros[4] = {0, 0, 0, 0};
  EXPECT_EQ(rws::base64_encode(zeros, 4), "AAAAAA==");
  const unsigned char ffs[4] = {255, 255, 255, 255};
  EXPECT_EQ(rws::base64_encode(ffs, 4), "/////w==");
}

TEST_F(TranslateFixture, Base64EncodeKnownVectors)
{
  EXPECT_EQ(rws::base64_encode(reinterpret_cast<const unsigned char *>("Man"), 3), "TWFu");
  EXPECT_EQ(rws::base64_encode(reinterpret_cast<const unsigned char *>("Ma"), 2), "TWE=");
  EXPECT_EQ(rws::base64_encode(reinterpret_cast<const unsigned char *>("M"), 1), "TQ==");
  EXPECT_EQ(rws::base64_encode(reinterpret_cast<const unsigned char *>(""), 0), "");
}

TEST_F(TranslateFixture, Base64DecodeSpecExamples)
{
  EXPECT_EQ(rws::base64_decode("AAAAAA=="), std::string(4, '\0'));
  EXPECT_EQ(rws::base64_decode("/////w=="), std::string(4, static_cast<char>(0xFF)));
}

TEST_F(TranslateFixture, Base64DecodeSkipsWhitespaceAndPadding)
{
  EXPECT_EQ(rws::base64_decode("TW Fu\n"), "Man");
  EXPECT_EQ(rws::base64_decode(""), "");
}

TEST_F(TranslateFixture, Base64RoundTripAllByteValues)
{
  std::string data;
  for (int i = 0; i < 256; ++i) {
    data.push_back(static_cast<char>(i));
  }
  auto enc = rws::base64_encode(reinterpret_cast<const unsigned char *>(data.data()), data.size());
  EXPECT_EQ(rws::base64_decode(enc), data);
}

// --- Step 1: integration tests (ROS <-> JSON wire protocol) ---

// Encode: outgoing uint8[] / byte[] dynamic arrays become base64 strings.
TEST_F(TranslateFixture, EncodeDynamicByteArraysAsBase64)
{
  const char * type = "test_msgs/msg/UnboundedSequences";
  auto doc = make_byte_array_doc("uint8_values", {0, 0, 0, 0});
  auto & alloc = doc.GetAllocator();
  rapidjson::Value bv(rapidjson::kArrayType);
  for (int b : {255, 255, 255, 255}) {
    bv.PushBack(b, alloc);
  }
  doc.AddMember("byte_values", bv, alloc);

  auto msg = rws::json_to_serialized_message(type, doc);
  rapidjson::Document out;
  rws::serialized_message_to_json(type, msg, out);

  ASSERT_TRUE(out["uint8_values"].IsString());
  EXPECT_STREQ(out["uint8_values"].GetString(), "AAAAAA==");
  ASSERT_TRUE(out["byte_values"].IsString());
  EXPECT_STREQ(out["byte_values"].GetString(), "/////w==");
}

// Encode: outgoing char[] arrays become base64 strings.
TEST_F(TranslateFixture, EncodeCharArrayAsBase64)
{
  const char * type = "test_msgs/msg/UnboundedSequences";
  auto doc = make_byte_array_doc("char_values", {65, 66, 67});  // "ABC"

  auto msg = rws::json_to_serialized_message(type, doc);
  rapidjson::Document out;
  rws::serialized_message_to_json(type, msg, out);

  ASSERT_TRUE(out["char_values"].IsString());
  EXPECT_STREQ(out["char_values"].GetString(), "QUJD");
}

// Encode: an empty byte array becomes an empty base64 string.
TEST_F(TranslateFixture, EncodeEmptyByteArrayAsEmptyString)
{
  const char * type = "test_msgs/msg/UnboundedSequences";
  auto doc = make_byte_array_doc("uint8_values", {});

  auto msg = rws::json_to_serialized_message(type, doc);
  rapidjson::Document out;
  rws::serialized_message_to_json(type, msg, out);

  ASSERT_TRUE(out["uint8_values"].IsString());
  EXPECT_STREQ(out["uint8_values"].GetString(), "");
}

// Encode: fixed-size uint8[N] arrays become base64 strings.
TEST_F(TranslateFixture, EncodeFixedSizeByteArrayAsBase64)
{
  const char * type = "test_msgs/msg/Arrays";
  auto doc = make_byte_array_doc("uint8_values", {1, 2, 3});

  auto msg = rws::json_to_serialized_message(type, doc);
  rapidjson::Document out;
  rws::serialized_message_to_json(type, msg, out);

  ASSERT_TRUE(out["uint8_values"].IsString());
  EXPECT_STREQ(out["uint8_values"].GetString(), "AQID");
}

// Non-byte numeric arrays must remain JSON number arrays, never base64.
TEST_F(TranslateFixture, NonByteArraysStayNumeric)
{
  const char * type = "test_msgs/msg/UnboundedSequences";
  auto doc = make_byte_array_doc("uint16_values", {1, 2, 3});

  auto msg = rws::json_to_serialized_message(type, doc);
  rapidjson::Document out;
  rws::serialized_message_to_json(type, msg, out);

  ASSERT_TRUE(out["uint16_values"].IsArray());
  ASSERT_EQ(out["uint16_values"].Size(), 3u);
  EXPECT_EQ(out["uint16_values"][0].GetUint(), 1u);
  EXPECT_EQ(out["uint16_values"][2].GetUint(), 3u);
}

// Decode: an incoming base64 string yields the same message as the equivalent
// number array (dynamic array).
TEST_F(TranslateFixture, DecodeBase64DynamicMatchesNumberArray)
{
  const char * type = "test_msgs/msg/UnboundedSequences";
  auto from_b64 = rws::json_to_serialized_message(type, make_string_doc("uint8_values", "/////w=="));
  auto from_num = rws::json_to_serialized_message(type, make_byte_array_doc("uint8_values", {255, 255, 255, 255}));
  EXPECT_TRUE(serialized_equal(from_b64, from_num));
}

// Decode: an incoming base64 string yields the same message as the equivalent
// number array (fixed-size array).
TEST_F(TranslateFixture, DecodeBase64FixedMatchesNumberArray)
{
  const char * type = "test_msgs/msg/Arrays";
  auto from_b64 = rws::json_to_serialized_message(type, make_string_doc("uint8_values", "AQID"));
  auto from_num = rws::json_to_serialized_message(type, make_byte_array_doc("uint8_values", {1, 2, 3}));
  EXPECT_TRUE(serialized_equal(from_b64, from_num));
}

}  // namespace rws

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();
  rclcpp::shutdown();

  return result;
}

#endif  // RWS__TRANSLATE_TEST_HPP_
