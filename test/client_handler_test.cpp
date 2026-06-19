
#include "rws/client_handler.hpp"

#include <chrono>
#include <condition_variable>
#include <gtest/gtest.h>
#include <iostream>
#include <mutex>
#include <thread>

#include "generic_publisher_mock.hpp"
#include "rapidjson/document.h"
#include "rws/node_interface_impl.hpp"
#include "std_msgs/msg/string.hpp"
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

template <typename Predicate>
bool wait_for_condition(Predicate predicate, std::chrono::milliseconds timeout)
{
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return predicate();
}

TEST_F(ClientHandlerFixture, ros_topic_publish_reaches_websocket_subscriber_quickly)
{
  auto server_node = std::make_shared<rclcpp::Node>("rws_ros_to_ws_server_test");
  auto publisher_node = std::make_shared<rclcpp::Node>("rws_ros_to_ws_publisher_test");
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

  rclcpp::executors::MultiThreadedExecutor executor;
  executor.add_node(server_node);
  executor.add_node(publisher_node);
  ExecutorSpinGuard spin_guard(executor);

  auto publisher =
    publisher_node->create_publisher<std_msgs::msg::String>("/test_ros_to_ws_topic", 10);

  const char * subscribe_json = R"({
    "op": "subscribe",
    "topic": "/test_ros_to_ws_topic",
    "type": "std_msgs/msg/String",
    "compression": "none"
  })";
  auto subscribe_response =
    handler->process_message_rapid(subscribe_json, strlen(subscribe_json));
  EXPECT_TRUE(subscribe_response.empty());
  ASSERT_TRUE(wait_for_condition(
    [&publisher]() { return publisher->get_subscription_count() > 0; },
    2s));

  std_msgs::msg::String message;
  message.data = "hello from ros";
  const auto started_at = std::chrono::steady_clock::now();
  publisher->publish(message);

  std::string outbound_message;
  {
    std::unique_lock<std::mutex> lock(messages_mutex);
    ASSERT_TRUE(messages_cv.wait_for(
      lock, 2s,
      [&outbound_messages]()
      {
        return !outbound_messages.empty();
      }));
    outbound_message = outbound_messages.back();
  }
  const auto latency = std::chrono::steady_clock::now() - started_at;
  std::cout
    << "ROS-to-websocket topic latency: "
    << std::chrono::duration_cast<std::chrono::milliseconds>(latency).count()
    << "ms" << std::endl;
  EXPECT_LT(latency, 500ms);

  rapidjson::Document outbound_doc;
  outbound_doc.Parse(outbound_message.c_str());
  ASSERT_FALSE(outbound_doc.HasParseError());
  ASSERT_TRUE(outbound_doc.HasMember("op"));
  EXPECT_STREQ(outbound_doc["op"].GetString(), "publish");
  ASSERT_TRUE(outbound_doc.HasMember("topic"));
  EXPECT_STREQ(outbound_doc["topic"].GetString(), "/test_ros_to_ws_topic");
  ASSERT_TRUE(outbound_doc.HasMember("msg"));
  ASSERT_TRUE(outbound_doc["msg"].HasMember("data"));
  EXPECT_STREQ(outbound_doc["msg"]["data"].GetString(), "hello from ros");
}

// End-to-end latched-topic test (the fix): a transient_local publisher
// publishes a retained sample *before* any subscriber exists. A subscriber that
// requests durability "transient_local" must still receive that retained
// sample, even though it joined late.
TEST_F(ClientHandlerFixture, transient_local_subscriber_receives_latched_message_published_before_subscribe)
{
  auto server_node = std::make_shared<rclcpp::Node>("rws_latched_server_test");
  auto publisher_node = std::make_shared<rclcpp::Node>("rws_latched_publisher_test");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(server_node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);

  std::mutex messages_mutex;
  std::condition_variable messages_cv;
  std::vector<std::string> outbound_messages;
  auto websocket_callback = [&](std::string & msg) {
    {
      std::lock_guard<std::mutex> lock(messages_mutex);
      outbound_messages.push_back(msg);
    }
    messages_cv.notify_all();
  };

  auto handler = std::make_shared<rws::ClientHandler>(
    1, node_interface, connector, true, websocket_callback,
    [](std::vector<std::uint8_t> &) {});

  rclcpp::executors::MultiThreadedExecutor executor;
  executor.add_node(server_node);
  executor.add_node(publisher_node);
  ExecutorSpinGuard spin_guard(executor);

  // Latched publisher with a retained sample, published before any subscriber.
  rclcpp::QoS latched_qos(1);
  latched_qos.transient_local();
  auto publisher =
    publisher_node->create_publisher<std_msgs::msg::String>("/test_latched_topic", latched_qos);
  std_msgs::msg::String latched_message;
  latched_message.data = "latched hello";
  publisher->publish(latched_message);

  // Subscribe late, explicitly requesting transient_local durability.
  const char * subscribe_json = R"({
    "op": "subscribe",
    "topic": "/test_latched_topic",
    "type": "std_msgs/msg/String",
    "durability": "transient_local"
  })";
  auto subscribe_response = handler->process_message_rapid(subscribe_json, strlen(subscribe_json));
  EXPECT_TRUE(subscribe_response.empty());

  // The retained sample must be delivered to the late subscriber.
  std::string outbound_message;
  {
    std::unique_lock<std::mutex> lock(messages_mutex);
    ASSERT_TRUE(messages_cv.wait_for(
      lock, 4s, [&outbound_messages]() { return !outbound_messages.empty(); }));
    outbound_message = outbound_messages.back();
  }

  rapidjson::Document outbound_doc;
  outbound_doc.Parse(outbound_message.c_str());
  ASSERT_FALSE(outbound_doc.HasParseError());
  EXPECT_STREQ(outbound_doc["op"].GetString(), "publish");
  EXPECT_STREQ(outbound_doc["topic"].GetString(), "/test_latched_topic");
  ASSERT_TRUE(outbound_doc["msg"].HasMember("data"));
  EXPECT_STREQ(outbound_doc["msg"]["data"].GetString(), "latched hello");
}

// End-to-end contrast (why the param matters): the same scenario but the
// subscriber requests durability "volatile". DDS guarantees a volatile
// subscriber never receives a sample published before it matched, so the
// retained latched sample is *not* delivered. This is the failure mode that
// silently happens whenever a subscriber would be created volatile -- exactly
// what the discovery race produces when no durability is requested.
TEST_F(ClientHandlerFixture, volatile_subscriber_does_not_receive_latched_message_published_before_subscribe)
{
  auto server_node = std::make_shared<rclcpp::Node>("rws_volatile_server_test");
  auto publisher_node = std::make_shared<rclcpp::Node>("rws_volatile_publisher_test");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(server_node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);

  std::mutex messages_mutex;
  std::condition_variable messages_cv;
  std::vector<std::string> outbound_messages;
  auto websocket_callback = [&](std::string & msg) {
    {
      std::lock_guard<std::mutex> lock(messages_mutex);
      outbound_messages.push_back(msg);
    }
    messages_cv.notify_all();
  };

  auto handler = std::make_shared<rws::ClientHandler>(
    1, node_interface, connector, true, websocket_callback,
    [](std::vector<std::uint8_t> &) {});

  rclcpp::executors::MultiThreadedExecutor executor;
  executor.add_node(server_node);
  executor.add_node(publisher_node);
  ExecutorSpinGuard spin_guard(executor);

  rclcpp::QoS latched_qos(1);
  latched_qos.transient_local();
  auto publisher =
    publisher_node->create_publisher<std_msgs::msg::String>("/test_volatile_latched_topic", latched_qos);
  std_msgs::msg::String latched_message;
  latched_message.data = "latched hello";
  publisher->publish(latched_message);

  const char * subscribe_json = R"({
    "op": "subscribe",
    "topic": "/test_volatile_latched_topic",
    "type": "std_msgs/msg/String",
    "durability": "volatile"
  })";
  auto subscribe_response = handler->process_message_rapid(subscribe_json, strlen(subscribe_json));
  EXPECT_TRUE(subscribe_response.empty());

  // The subscriber is QoS-compatible and does match the publisher...
  ASSERT_TRUE(wait_for_condition(
    [&publisher]() { return publisher->get_subscription_count() > 0; }, 4s));

  // ...but it must NOT receive the sample retained before it joined.
  {
    std::unique_lock<std::mutex> lock(messages_mutex);
    EXPECT_FALSE(messages_cv.wait_for(
      lock, 1s, [&outbound_messages]() { return !outbound_messages.empty(); }));
  }
}

TEST_F(ClientHandlerFixture, websocket_topic_publish_reaches_ros_subscriber_quickly)
{
  auto server_node = std::make_shared<rclcpp::Node>("rws_ws_to_ros_server_test");
  auto subscriber_node = std::make_shared<rclcpp::Node>("rws_ws_to_ros_subscriber_test");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(server_node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);

  auto handler = std::make_shared<rws::ClientHandler>(
    1, node_interface, connector, true, [](std::string &) {},
    [](std::vector<std::uint8_t> &) {});

  std::mutex received_mutex;
  std::condition_variable received_cv;
  std::vector<std::string> received_messages;
  auto subscription = subscriber_node->create_subscription<std_msgs::msg::String>(
    "/test_ws_to_ros_topic", 10,
    [&](const std_msgs::msg::String::SharedPtr msg)
    {
      {
        std::lock_guard<std::mutex> lock(received_mutex);
        received_messages.push_back(msg->data);
      }
      received_cv.notify_all();
    });

  rclcpp::executors::MultiThreadedExecutor executor;
  executor.add_node(server_node);
  executor.add_node(subscriber_node);
  ExecutorSpinGuard spin_guard(executor);

  const char * advertise_json = R"({
    "op": "advertise",
    "topic": "/test_ws_to_ros_topic",
    "type": "std_msgs/msg/String",
    "queue_size": 10
  })";
  auto advertise_response =
    handler->process_message_rapid(advertise_json, strlen(advertise_json));
  EXPECT_TRUE(advertise_response.empty());
  ASSERT_TRUE(wait_for_condition(
    [&subscription]() { return subscription->get_publisher_count() > 0; },
    2s));

  const char * publish_json = R"({
    "op": "publish",
    "topic": "/test_ws_to_ros_topic",
    "msg": {
      "data": "hello from websocket"
    }
  })";
  const auto started_at = std::chrono::steady_clock::now();
  auto publish_response =
    handler->process_message_rapid(publish_json, strlen(publish_json));
  EXPECT_TRUE(publish_response.empty());

  std::string received_message;
  {
    std::unique_lock<std::mutex> lock(received_mutex);
    ASSERT_TRUE(received_cv.wait_for(
      lock, 2s,
      [&received_messages]()
      {
        return !received_messages.empty();
      }));
    received_message = received_messages.back();
  }
  const auto latency = std::chrono::steady_clock::now() - started_at;
  std::cout
    << "Websocket-to-ROS topic latency: "
    << std::chrono::duration_cast<std::chrono::milliseconds>(latency).count()
    << "ms" << std::endl;
  EXPECT_LT(latency, 500ms);
  EXPECT_EQ(received_message, "hello from websocket");
}

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
