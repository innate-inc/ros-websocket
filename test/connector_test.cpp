#include "rws/connector.hpp"
#include <gtest/gtest.h>

#include "node_mock.hpp"

namespace rws
{

using testing::_;
using testing::Invoke;
using testing::MockFunction;
using testing::Return;

using ConstSharedMessage = std::shared_ptr<const rclcpp::SerializedMessage>;

// Build a TopicEndpointInfo describing a publisher with the given durability,
// as would be returned by get_publishers_info_by_topic once discovery completes.
static rclcpp::TopicEndpointInfo make_publisher_info(rmw_qos_durability_policy_t durability)
{
  rcl_topic_endpoint_info_t info{};
  info.node_name = "publisher_node";
  info.node_namespace = "/";
  info.topic_type = "std_msgs/msg/String";
  info.endpoint_type = RMW_ENDPOINT_PUBLISHER;
  info.qos_profile = rmw_qos_profile_default;
  info.qos_profile.durability = durability;
  return rclcpp::TopicEndpointInfo(info);
}

class ConnectorFixture : public testing::Test
{
public:
  ConnectorFixture() {}

  void SetUp() override
  {
    for(int i = 0; i < 6; i++) {
      messages.push_back(std::make_shared<const rclcpp::SerializedMessage>());
    }
  }

  void TearDown() override { messages.clear(); }

protected:
  std::vector<ConstSharedMessage> messages;
};

TEST_F(ConnectorFixture, subscribe_to_topic_calls_create_generic_subscription_with_correct_params)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  topic_params params("/topic", "std_msgs/msg/String");
  EXPECT_EQ(params.topic, "/topic");
  EXPECT_EQ(params.type, "std_msgs/msg/String");
  EXPECT_EQ(params.history_depth, 10);
  EXPECT_EQ(params.throttle_rate.nanoseconds(), 0);
  EXPECT_EQ(params.compression, "none");

  EXPECT_CALL(
    *node, create_generic_subscription("/topic", "std_msgs/msg/String", rclcpp::QoS(10), _, _))
    .Times(1)
    .WillRepeatedly(Return(nullptr));

  auto handler = [](topic_params, ConstSharedMessage) {};
  connector.subscribe_to_topic(0, params, handler);
}

TEST_F(ConnectorFixture, subscribe_to_topic_calls_create_generic_subscription_only_once)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  topic_params params("/topic", "std_msgs/msg/String");
  EXPECT_EQ(params.topic, "/topic");
  EXPECT_EQ(params.type, "std_msgs/msg/String");
  EXPECT_EQ(params.history_depth, 10);
  EXPECT_EQ(params.throttle_rate.nanoseconds(), 0);
  EXPECT_EQ(params.compression, "none");

  EXPECT_CALL(*node, create_generic_subscription(_, _, _, _, _)).Times(1).WillOnce(Return(nullptr));

  auto handler = [](topic_params, ConstSharedMessage) {};
  connector.subscribe_to_topic(0, params, handler);
  connector.subscribe_to_topic(0, params, handler);
}

TEST_F(
  ConnectorFixture,
  subscribe_to_topic_calls_message_handler_for_corresponding_client_with_the_same_topic)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);
  topic_params params("/topic", "std_msgs/msg/String");

  std::function<void(ConstSharedMessage)> topic_callback;
  EXPECT_CALL(
    *node, create_generic_subscription("/topic", "std_msgs/msg/String", rclcpp::QoS(10), _, _))
    .Times(1)
    .WillRepeatedly(
      Invoke([&topic_callback](
               const std::string &, const std::string &,
               const rclcpp::QoS &, std::function<void(ConstSharedMessage)> callback,
               const rclcpp::SubscriptionOptions &) {
        topic_callback = callback;
        return nullptr;
      }));

  // Client 1
  std::vector<ConstSharedMessage> client0_msgs;
  connector.subscribe_to_topic(
    0, params, [&client0_msgs](topic_params, ConstSharedMessage message) { client0_msgs.push_back(message); });

  // Client 2
  std::vector<ConstSharedMessage> client1_msgs;
  connector.subscribe_to_topic(
    1, params, [&client1_msgs](topic_params, ConstSharedMessage message) { client1_msgs.push_back(message); });

  // "Publish" messages
  topic_callback(messages[0]);
  topic_callback(messages[1]);

  // Expect client 0 to receive both messages
  EXPECT_EQ(client0_msgs.size(), 2);
  EXPECT_EQ(client0_msgs[0], messages[0]);
  EXPECT_EQ(client0_msgs[1], messages[1]);

  // Expect client 1 to receive both messages
  EXPECT_EQ(client1_msgs.size(), 2);
  EXPECT_EQ(client1_msgs[0], messages[0]);
  EXPECT_EQ(client1_msgs[1], messages[1]);
}

TEST_F(
  ConnectorFixture,
  subscribe_to_topic_calls_message_handler_for_corresponding_client_with_different_topics)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  topic_params params0("/topic0", "std_msgs/msg/String");
  topic_params params1("/topic1", "std_msgs/msg/String");

  std::function<void(ConstSharedMessage)> topic0_callback;
  EXPECT_CALL(
    *node, create_generic_subscription("/topic0", "std_msgs/msg/String", rclcpp::QoS(10), _, _))
    .Times(1)
    .WillRepeatedly(
      Invoke([&topic0_callback](
               const std::string &, const std::string &,
               const rclcpp::QoS &, std::function<void(ConstSharedMessage)> callback,
               const rclcpp::SubscriptionOptions &) {
        topic0_callback = callback;
        return nullptr;
      }));

  std::function<void(ConstSharedMessage)> topic1_callback;
  EXPECT_CALL(
    *node, create_generic_subscription("/topic1", "std_msgs/msg/String", rclcpp::QoS(10), _, _))
    .Times(1)
    .WillRepeatedly(
      Invoke([&topic1_callback](
               const std::string &, const std::string &,
               const rclcpp::QoS &, std::function<void(ConstSharedMessage)> callback,
               const rclcpp::SubscriptionOptions &) {
        topic1_callback = callback;
        return nullptr;
      }));

  // Client 1
  std::vector<ConstSharedMessage> client0_msgs;
  connector.subscribe_to_topic(
    0, params0, [&client0_msgs](topic_params, ConstSharedMessage message) { client0_msgs.push_back(message); });

  // Client 2
  std::vector<ConstSharedMessage> client1_msgs;
  connector.subscribe_to_topic(
    1, params1, [&client1_msgs](topic_params, ConstSharedMessage message) { client1_msgs.push_back(message); });

  // "Publish" messages
  topic0_callback(messages[0]);
  topic1_callback(messages[1]);

  // Expect client 0 to receive one message
  EXPECT_EQ(client0_msgs.size(), 1);
  EXPECT_EQ(client0_msgs[0], messages[0]);

  // Expect client 1 to receive one message
  EXPECT_EQ(client1_msgs.size(), 1);
  EXPECT_EQ(client1_msgs[0], messages[1]);
}

TEST_F(ConnectorFixture, connector_unsubscribes_from_topic_when_no_more_clients_are_subscribed)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  topic_params topic0_p("/topic", "std_msgs/msg/String");
  topic_params topic1_p("/another_topic", "std_msgs/msg/BoundedArray");
  topic_params topic2_p("/third_topic", "std_msgs/msg/Float32");

  // Client 1
  auto client0_unsub_t0 =
    connector.subscribe_to_topic(0, topic0_p, [](topic_params, ConstSharedMessage) {});
  EXPECT_EQ(connector.is_subscribed_to_topic(topic0_p), true);

  // Client 2
  auto client1_unsub_t0 = connector.subscribe_to_topic(1, topic0_p, [](topic_params, ConstSharedMessage) {});
  EXPECT_EQ(connector.is_subscribed_to_topic(topic0_p), true);
  auto client1_unsub_t1 = connector.subscribe_to_topic(1, topic1_p, [](topic_params, ConstSharedMessage) {});
  EXPECT_EQ(connector.is_subscribed_to_topic(topic1_p), true);
  auto client1_unsub_t2 = connector.subscribe_to_topic(1, topic2_p, [](topic_params, ConstSharedMessage) {});
  EXPECT_EQ(connector.is_subscribed_to_topic(topic2_p), true);

  // Unsubscribe client 0 from topic 0
  client0_unsub_t0();

  // Topic 0 is still subscribed
  EXPECT_EQ(connector.is_subscribed_to_topic(topic0_p), true);

  // Unsubscribe client 1 from topic 0(ascending order)
  client1_unsub_t0();
  EXPECT_EQ(connector.is_subscribed_to_topic(topic0_p), false);

  // Topic 1 and 2 is still subscribed
  EXPECT_EQ(connector.is_subscribed_to_topic(topic1_p), true);
  EXPECT_EQ(connector.is_subscribed_to_topic(topic2_p), true);

  // Unsubscribe client 1 from topic 2(descending order)
  client1_unsub_t2();
  EXPECT_EQ(connector.is_subscribed_to_topic(topic2_p), false);

  // Topic 1 is still subscribed
  EXPECT_EQ(connector.is_subscribed_to_topic(topic1_p), true);

  // Unsubscribe client 1 from topic 1
  client1_unsub_t1();
  EXPECT_EQ(connector.is_subscribed_to_topic(topic1_p), false);
}

TEST_F(ConnectorFixture, advertise_topic_callc_create_generic_publisher_with_correct_params)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  topic_params params("/topic", "std_msgs/msg/String");
  std::function<void(ConstSharedMessage)> publisher_callback;
  EXPECT_CALL(*node, create_generic_publisher("/topic", "std_msgs/msg/String", rclcpp::QoS(10), _))
    .Times(1);
  connector.advertise_topic(0, params, publisher_callback);

  // latched publisher
  EXPECT_CALL(
    *node,
    create_generic_publisher("/topic", "std_msgs/msg/String", rclcpp::QoS(10).transient_local(), _))
    .Times(1);
  params.latch = true;
  connector.advertise_topic(1, params, publisher_callback);
}

TEST_F(ConnectorFixture, advertise_topic_return_callback_for_publishing_message)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  auto publisher = std::make_shared<testing::NiceMock<GenericPublisherMock>>();
  Connector<GenericPublisherMock> connector(node);

  topic_params params("/topic", "std_msgs/msg/String");

  std::function<void(ConstSharedMessage)> publisher_callback;
  EXPECT_CALL(*node, create_generic_publisher(_, _, _, _)).Times(1).WillOnce(Return(publisher));

  auto unadvertise = connector.advertise_topic(0, params, publisher_callback);

  EXPECT_CALL(*publisher, publish(_)).Times(1);
  publisher_callback(std::make_shared<const rclcpp::SerializedMessage>());
}

TEST_F(ConnectorFixture, connector_unadvertise_when_no_more_clients_are_advertising)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  auto publisher = std::make_shared<testing::NiceMock<GenericPublisherMock>>();
  Connector<GenericPublisherMock> connector(node);

  topic_params topic0_p("/topic", "std_msgs/msg/String");
  topic_params topic1_p("/another_topic", "std_msgs/msg/BoundedArray");
  topic_params topic2_p("/third_topic", "std_msgs/msg/Float32");

  std::function<void(ConstSharedMessage)> publisher_cb;

  // Client 1
  auto client0_unadv_t0 = connector.advertise_topic(0, topic0_p, publisher_cb);
  EXPECT_EQ(connector.is_advertising_topic(topic0_p), true);

  // Client 2
  auto client1_unadv_t0 = connector.advertise_topic(1, topic0_p, publisher_cb);
  EXPECT_EQ(connector.is_advertising_topic(topic0_p), true);
  auto client1_unadv_t1 = connector.advertise_topic(1, topic1_p, publisher_cb);
  EXPECT_EQ(connector.is_advertising_topic(topic1_p), true);
  auto client1_unadv_t2 = connector.advertise_topic(1, topic2_p, publisher_cb);
  EXPECT_EQ(connector.is_advertising_topic(topic2_p), true);

  // Unadvertise client 0 from topic 0
  client0_unadv_t0();

  // Topic 0 is still advertised
  EXPECT_EQ(connector.is_advertising_topic(topic0_p), true);

  // Unadvertise client 1 from topic 0(ascending order)
  client1_unadv_t0();
  EXPECT_EQ(connector.is_advertising_topic(topic0_p), false);

  // Topic 1 and 2 is still advertised
  EXPECT_EQ(connector.is_advertising_topic(topic1_p), true);
  EXPECT_EQ(connector.is_advertising_topic(topic2_p), true);

  // Unadvertise client 1 from topic 2(descending order)
  client1_unadv_t2();
  EXPECT_EQ(connector.is_advertising_topic(topic2_p), false);

  // Topic 1 is still advertised
  EXPECT_EQ(connector.is_advertising_topic(topic1_p), true);

  // Unadvertise client 1 from topic 1
  client1_unadv_t1();
  EXPECT_EQ(connector.is_advertising_topic(topic1_p), false);
}

TEST_F(ConnectorFixture, subscription_callback_throttles_messages)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  topic_params params;
  params.topic = "/test_topic";
  params.type = "std_msgs/msg/String";
  params.throttle_rate = rclcpp::Duration(0, 100 * 1000000);

  std::function<void(ConstSharedMessage)> topic_callback;
  EXPECT_CALL(
    *node, create_generic_subscription("/test_topic", "std_msgs/msg/String", rclcpp::QoS(10), _, _))
    .Times(1)
    .WillRepeatedly(
      Invoke([&topic_callback](
               const std::string &, const std::string &,
               const rclcpp::QoS &, std::function<void(ConstSharedMessage)> callback,
               const rclcpp::SubscriptionOptions &) {
        topic_callback = callback;
        return nullptr;
      }));

  rclcpp::Time now(1000, 0, RCL_ROS_TIME);
  ON_CALL(*node, now()).WillByDefault(Invoke([&now](){
    now += rclcpp::Duration(0, 50 * 1000000);
    return now;
  }));

  // Received messages
  std::vector<ConstSharedMessage> client_msgs;
  connector.subscribe_to_topic(
    0, params, [&client_msgs](topic_params, ConstSharedMessage message) { client_msgs.push_back(message); });
  
  // "Publish" messages
  topic_callback(messages[0]); // last_sent = 0, 0
  topic_callback(messages[1]); //             1000, 50
  topic_callback(messages[2]); //             1000, 100
  topic_callback(messages[3]); //             1000, 150

  // Expect client to receive first and last message
  EXPECT_EQ(client_msgs.size(), 2);
  EXPECT_EQ(client_msgs[0], messages[0]);
  EXPECT_EQ(client_msgs[1], messages[3]);
}

// ---------------------------------------------------------------------------
// Latched-topic discovery race tests.
//
// A transient_local ("latched") publisher retains its last sample for late
// subscribers. A subscriber only receives that retained sample if it is itself
// transient_local. Historically rws derived the subscriber durability from
// get_publishers_info_by_topic at subscribe time, so a subscriber that
// connected before the publisher was discovered would be created Volatile and
// silently miss the latched sample. Letting the client request durability
// explicitly removes that dependency on discovery timing.
// ---------------------------------------------------------------------------

// Reproduces the race: without an explicit durability request, a subscriber
// that connects before the publisher is discovered is created Volatile and
// would miss the latched sample.
TEST_F(ConnectorFixture, subscribe_without_durability_is_volatile_when_publisher_not_yet_discovered)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  // Publisher not discovered yet -> empty info (the racy window).
  EXPECT_CALL(*node, get_publishers_info_by_topic("/latched", _))
    .WillRepeatedly(Return(std::vector<rclcpp::TopicEndpointInfo>{}));

  topic_params params("/latched", "std_msgs/msg/String");
  ASSERT_EQ(params.durability, rclcpp::DurabilityPolicy::SystemDefault);

  // Created Volatile (plain QoS(10)), not transient_local -> would miss latch.
  EXPECT_CALL(*node, create_generic_subscription("/latched", "std_msgs/msg/String", rclcpp::QoS(10), _, _))
    .Times(1)
    .WillOnce(Return(nullptr));

  connector.subscribe_to_topic(0, params, [](topic_params, ConstSharedMessage) {});
}

// The fix: an explicit transient_local request makes the subscriber
// transient_local regardless of whether the publisher has been discovered, and
// without consulting discovery at all.
TEST_F(ConnectorFixture, subscribe_with_transient_local_is_transient_local_even_when_publisher_not_yet_discovered)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  // Discovery must not be relied upon at all when durability is explicit.
  EXPECT_CALL(*node, get_publishers_info_by_topic(_, _)).Times(0);

  topic_params params(
    "/latched", "std_msgs/msg/String", 10, "none", rclcpp::Duration(0, 0),
    rclcpp::DurabilityPolicy::TransientLocal);

  EXPECT_CALL(
    *node,
    create_generic_subscription("/latched", "std_msgs/msg/String", rclcpp::QoS(10).transient_local(), _, _))
    .Times(1)
    .WillOnce(Return(nullptr));

  connector.subscribe_to_topic(0, params, [](topic_params, ConstSharedMessage) {});
}

// Legacy auto-match still works when no explicit durability is requested and
// the publisher has already been discovered as transient_local.
TEST_F(ConnectorFixture, subscribe_without_durability_auto_matches_discovered_transient_local_publisher)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  EXPECT_CALL(*node, get_publishers_info_by_topic("/latched", _))
    .WillRepeatedly(Return(std::vector<rclcpp::TopicEndpointInfo>{
      make_publisher_info(RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL)}));

  topic_params params("/latched", "std_msgs/msg/String");

  EXPECT_CALL(
    *node,
    create_generic_subscription("/latched", "std_msgs/msg/String", rclcpp::QoS(10).transient_local(), _, _))
    .Times(1)
    .WillOnce(Return(nullptr));

  connector.subscribe_to_topic(0, params, [](topic_params, ConstSharedMessage) {});
}

// An explicit volatile request overrides auto-matching: even with a discovered
// transient_local publisher the subscriber stays Volatile (explicit > magic).
TEST_F(ConnectorFixture, subscribe_with_explicit_volatile_overrides_transient_local_publisher)
{
  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);

  // Discovery is not consulted when durability is explicit.
  EXPECT_CALL(*node, get_publishers_info_by_topic(_, _)).Times(0);

  topic_params params(
    "/latched", "std_msgs/msg/String", 10, "none", rclcpp::Duration(0, 0),
    rclcpp::DurabilityPolicy::Volatile);

  EXPECT_CALL(*node, create_generic_subscription("/latched", "std_msgs/msg/String", rclcpp::QoS(10), _, _))
    .Times(1)
    .WillOnce(Return(nullptr));

  connector.subscribe_to_topic(0, params, [](topic_params, ConstSharedMessage) {});
}

// Subscribers that differ only in requested durability are distinct, so each
// gets its own subscription with the correct QoS rather than being collapsed.
TEST_F(ConnectorFixture, params_with_different_durability_are_not_equal)
{
  topic_params volatile_params(
    "/latched", "std_msgs/msg/String", 10, "none", rclcpp::Duration(0, 0),
    rclcpp::DurabilityPolicy::Volatile);
  topic_params transient_params(
    "/latched", "std_msgs/msg/String", 10, "none", rclcpp::Duration(0, 0),
    rclcpp::DurabilityPolicy::TransientLocal);

  EXPECT_FALSE(volatile_params == transient_params);

  auto node = std::make_shared<testing::NiceMock<NodeMock>>();
  Connector<GenericPublisherMock> connector(node);
  EXPECT_CALL(*node, create_generic_subscription(_, _, rclcpp::QoS(10), _, _))
    .Times(1)
    .WillOnce(Return(nullptr));
  EXPECT_CALL(*node, create_generic_subscription(_, _, rclcpp::QoS(10).transient_local(), _, _))
    .Times(1)
    .WillOnce(Return(nullptr));

  connector.subscribe_to_topic(0, volatile_params, [](topic_params, ConstSharedMessage) {});
  connector.subscribe_to_topic(0, transient_params, [](topic_params, ConstSharedMessage) {});
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
