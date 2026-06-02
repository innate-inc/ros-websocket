
#include "rws/client_handler.hpp"

#include <chrono>
#include <condition_variable>
#include <gtest/gtest.h>
#include <mutex>

#include "generic_publisher_mock.hpp"
#include "rapidjson/document.h"
#include "rws/node_interface_impl.hpp"
#include "std_srvs/srv/set_bool.hpp"

namespace rws
{

using namespace std::chrono_literals;

class ClientHandlerFixture : public testing::Test
{
public:
  ClientHandlerFixture() {}

  void SetUp() override {}

  void TearDown() override {}

protected:
};

struct ExecutorSpinGuard
{
  explicit ExecutorSpinGuard(rclcpp::Executor & executor)
  : executor_(executor), thread_([this]() { executor_.spin(); })
  {
  }

  ~ExecutorSpinGuard()
  {
    executor_.cancel();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  rclcpp::Executor & executor_;
  std::thread thread_;
};

TEST_F(ClientHandlerFixture, advertised_service_round_trips_ros_service_call)
{
  auto server_node = std::make_shared<rclcpp::Node>("rws_advertised_service_server_test");
  auto client_node = std::make_shared<rclcpp::Node>("rws_advertised_service_client_test");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(server_node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);

  std::mutex messages_mutex;
  std::condition_variable messages_cv;
  std::vector<std::string> outbound_messages;
  auto websocket_callback =
    [&](std::string & msg)
    {
      {
        std::lock_guard<std::mutex> lock(messages_mutex);
        outbound_messages.push_back(msg);
      }
      messages_cv.notify_all();
    };

  auto handler = std::make_shared<rws::ClientHandler>(
    1, node_interface, connector, true, websocket_callback,
    [](std::vector<std::uint8_t> &) {});

  const char * advertise_json = R"({
    "op": "advertise_service",
    "service": "/test_advertised_set_bool",
    "type": "std_srvs/srv/SetBool"
  })";
  auto advertise_response =
    handler->process_message_rapid(advertise_json, strlen(advertise_json));
  EXPECT_TRUE(advertise_response.empty());

  rclcpp::executors::MultiThreadedExecutor executor;
  executor.add_node(server_node);
  executor.add_node(client_node);
  ExecutorSpinGuard spin_guard(executor);

  auto ros_client =
    client_node->create_client<std_srvs::srv::SetBool>("/test_advertised_set_bool");
  ASSERT_TRUE(ros_client->wait_for_service(2s));

  auto request = std::make_shared<std_srvs::srv::SetBool::Request>();
  request->data = true;
  auto future = ros_client->async_send_request(request);

  std::string forwarded_request;
  {
    std::unique_lock<std::mutex> lock(messages_mutex);
    ASSERT_TRUE(messages_cv.wait_for(
      lock, 2s,
      [&outbound_messages]()
      {
        return !outbound_messages.empty();
      }));
    forwarded_request = outbound_messages.back();
  }

  rapidjson::Document forwarded_doc;
  forwarded_doc.Parse(forwarded_request.c_str());
  ASSERT_FALSE(forwarded_doc.HasParseError());
  ASSERT_TRUE(forwarded_doc.HasMember("op"));
  EXPECT_STREQ(forwarded_doc["op"].GetString(), "call_service");
  ASSERT_TRUE(forwarded_doc.HasMember("service"));
  EXPECT_STREQ(forwarded_doc["service"].GetString(), "/test_advertised_set_bool");
  ASSERT_TRUE(forwarded_doc.HasMember("id"));
  ASSERT_TRUE(forwarded_doc["id"].IsString());
  ASSERT_TRUE(forwarded_doc.HasMember("args"));
  ASSERT_TRUE(forwarded_doc["args"].HasMember("data"));
  EXPECT_TRUE(forwarded_doc["args"]["data"].GetBool());

  const std::string request_id = forwarded_doc["id"].GetString();
  const std::string response_json =
    R"({"op":"service_response","id":")" + request_id +
    R"(","service":"/test_advertised_set_bool","values":{"success":true,"message":"ok"}})";
  auto service_response =
    handler->process_message_rapid(response_json.c_str(), response_json.size());
  EXPECT_TRUE(service_response.empty());

  ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
  auto response = future.get();
  EXPECT_TRUE(response->success);
  EXPECT_EQ(response->message, "ok");

  const char * unadvertise_json = R"({
    "op": "unadvertise_service",
    "service": "/test_advertised_set_bool"
  })";
  auto unadvertise_response =
    handler->process_message_rapid(unadvertise_json, strlen(unadvertise_json));
  EXPECT_TRUE(unadvertise_response.empty());
}

TEST_F(ClientHandlerFixture, subsribe_to_topic_is_thread_safe)
{
  auto server_node = std::make_shared<rclcpp::Node>("server_node");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(server_node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);
  auto pub = server_node->create_generic_publisher("/test", "std_msgs/msg/String", rclcpp::QoS(10));
  const char* json_str = R"(
    {
      "compression": "none",
      "op": "subscribe",
      "topic": "/test"
    }
  )";
  const size_t json_len = strlen(json_str);
  const int nodes_count = 50;

  std::vector<std::thread> threads;
  std::vector<std::shared_ptr<rws::ClientHandler>> nodes;
  for (int ti = 0; ti < nodes_count; ti++) {
    nodes.push_back(std::make_shared<rws::ClientHandler>(
      ti, node_interface, connector, true, [](std::string &) {},
      [](std::vector<std::uint8_t> &) {}));
    threads.push_back(std::thread([ti, nodes, json_str, json_len]() {
      for (int i = 0; i < 1000; i++) {
        nodes[ti]->process_message_rapid(json_str, json_len);
      }
    }));
  }

  for (int ti = 0; ti < nodes_count; ti++) {
    threads[ti].join();
  }

  EXPECT_NE(server_node, nullptr);
}

TEST_F(ClientHandlerFixture, advertise_topic_is_thread_safe)
{
  auto server_node = std::make_shared<rclcpp::Node>("server_node");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(server_node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);
  const char* json_str = R"(
    {
      "op": "advertise",
      "history_depth": 10,
      "type": "std_msgs/msg/String",
      "topic": "/test",
      "latch": false
    }
  )";
  const size_t json_len = strlen(json_str);
  const int nodes_count = 50;

  std::vector<std::thread> threads;
  std::vector<std::shared_ptr<rws::ClientHandler>> nodes;
  for (int ti = 0; ti < nodes_count; ti++) {
    nodes.push_back(std::make_shared<rws::ClientHandler>(
      ti, node_interface, connector, false, [](std::string &) {},
      [](std::vector<std::uint8_t> &) {}));
    threads.push_back(std::thread([ti, nodes, json_str, json_len]() {
      for (int i = 0; i < 1000; i++) {
        nodes[ti]->process_message_rapid(json_str, json_len);
      }
    }));
  }

  for (int ti = 0; ti < nodes_count; ti++) {
    threads[ti].join();
  }

  EXPECT_NE(server_node, nullptr);
}

TEST_F(ClientHandlerFixture, rosapi_topic_and_raw_types_is_thread_safe)
{
  auto server_node = std::make_shared<rclcpp::Node>("server_node");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(server_node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);
  const char* json_str = R"(
    {
      "op": "call_service",
      "service": "/rosapi/topics_and_raw_types"
    }
  )";
  const size_t json_len = strlen(json_str);
  const int nodes_count = 20;

  std::vector<std::thread> threads;
  std::vector<std::shared_ptr<rws::ClientHandler>> nodes;
  for (int ti = 0; ti < nodes_count; ti++) {
    nodes.push_back(std::make_shared<rws::ClientHandler>(
      ti, node_interface, connector, true, [](std::string &) {},
      [](std::vector<std::uint8_t> &) {}));
    threads.push_back(std::thread([ti, nodes, json_str, json_len]() {
      for (int i = 0; i < 1000; i++) {
        nodes[ti]->process_message_rapid(json_str, json_len);
      }
    }));
  }

  for (int ti = 0; ti < nodes_count; ti++) {
    threads[ti].join();
  }

  EXPECT_NE(server_node, nullptr);
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
