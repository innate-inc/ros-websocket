
#include "rws/client_handler.hpp"

#include <gtest/gtest.h>

#include "generic_publisher_mock.hpp"
#include "rws/node_interface_impl.hpp"

namespace rws
{

class ClientHandlerFixture : public testing::Test
{
public:
  ClientHandlerFixture() {}

  void SetUp() override {}

  void TearDown() override {}

protected:
};

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
