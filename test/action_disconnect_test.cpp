// Copyright 2026 Innate Inc
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

// Regression tests for the client-disconnect-mid-action-goal use-after-free:
// destroying a ClientHandler while its action goal was in flight left the
// goal's callbacks (capturing the dead handler) registered, and the next
// feedback/result dispatch segfaulted the whole bridge.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rapidjson/document.h"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_action/rclcpp_action.hpp"
#include "rws/client_handler.hpp"
#include "rws/node_interface_impl.hpp"
#include "test_msgs/action/fibonacci.hpp"

namespace rws
{

using namespace std::chrono_literals;
using Fibonacci = test_msgs::action::Fibonacci;
using FibonacciGoalHandle = rclcpp_action::ServerGoalHandle<Fibonacci>;

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

/// A slow Fibonacci action server: one feedback per 100 ms step, honors cancel.
class SlowFibonacciServer
{
public:
  explicit SlowFibonacciServer(rclcpp::Node::SharedPtr node, const std::string & action_name)
  {
    server_ = rclcpp_action::create_server<Fibonacci>(
      node, action_name,
      [](const rclcpp_action::GoalUUID &, std::shared_ptr<const Fibonacci::Goal>) {
        return rclcpp_action::GoalResponse::ACCEPT_AND_EXECUTE;
      },
      [this](std::shared_ptr<FibonacciGoalHandle>) {
        cancels_received_++;
        return rclcpp_action::CancelResponse::ACCEPT;
      },
      [this](std::shared_ptr<FibonacciGoalHandle> goal_handle) {
        std::thread([this, goal_handle]() { execute(goal_handle); }).detach();
      });
  }

  int cancels_received() const { return cancels_received_; }
  int goals_finished() const { return goals_finished_; }

private:
  void execute(std::shared_ptr<FibonacciGoalHandle> goal_handle)
  {
    auto feedback = std::make_shared<Fibonacci::Feedback>();
    auto result = std::make_shared<Fibonacci::Result>();
    const int order = goal_handle->get_goal()->order;
    for (int i = 0; i < order && rclcpp::ok(); ++i) {
      if (goal_handle->is_canceling()) {
        goal_handle->canceled(result);
        goals_finished_++;
        return;
      }
      feedback->sequence.push_back(i);
      goal_handle->publish_feedback(feedback);
      std::this_thread::sleep_for(100ms);
    }
    goal_handle->succeed(result);
    goals_finished_++;
  }

  rclcpp_action::Server<Fibonacci>::SharedPtr server_;
  std::atomic<int> cancels_received_{0};
  std::atomic<int> goals_finished_{0};
};

/// Collects a handler's outbound websocket frames; flags any frame delivered
/// after the handler was destroyed.
struct OutboundRecorder
{
  std::function<void(std::string &)> callback()
  {
    return [this](std::string & msg) {
      std::lock_guard<std::mutex> lock(mutex);
      if (handler_destroyed) {
        frames_after_destroy++;
      }
      frames.push_back(msg);
      cv.notify_all();
    };
  }

  bool wait_for_op(const std::string & op, std::chrono::milliseconds timeout)
  {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, timeout, [&]() { return count_op_locked(op) > 0; });
  }

  int count_op(const std::string & op)
  {
    std::lock_guard<std::mutex> lock(mutex);
    return count_op_locked(op);
  }

  std::string last_frame_with_op(const std::string & op)
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto it = frames.rbegin(); it != frames.rend(); ++it) {
      if (frame_has_op(*it, op)) {
        return *it;
      }
    }
    return "";
  }

  std::mutex mutex;
  std::condition_variable cv;
  std::vector<std::string> frames;
  std::atomic<bool> handler_destroyed{false};
  std::atomic<int> frames_after_destroy{0};

private:
  static bool frame_has_op(const std::string & frame, const std::string & op)
  {
    rapidjson::Document doc;
    doc.Parse(frame.c_str());
    return !doc.HasParseError() && doc.HasMember("op") && doc["op"].IsString() &&
           op == doc["op"].GetString();
  }

  int count_op_locked(const std::string & op)
  {
    int n = 0;
    for (const auto & frame : frames) {
      if (frame_has_op(frame, op)) {
        n++;
      }
    }
    return n;
  }
};

static std::string send_goal_json(const std::string & action, const std::string & id)
{
  return std::string(R"({"op":"send_action_goal","id":")") + id +
         R"(","action":")" + action +
         R"(","action_type":"test_msgs/action/Fibonacci","args":{"order":100},"feedback":true,"goal_id":")" +
         id + R"(_goal"})";
}

TEST(ActionDisconnectTest, disconnect_mid_goal_severs_callbacks_and_cancels_goal)
{
  auto node = std::make_shared<rclcpp::Node>("rws_action_disconnect_test");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);
  SlowFibonacciServer server(node, "/rws_test_fibonacci_a");

  rclcpp::executors::MultiThreadedExecutor executor;
  executor.add_node(node);
  ExecutorSpinGuard spin_guard(executor);

  OutboundRecorder recorder;
  auto handler = std::make_shared<rws::ClientHandler>(
    1, node_interface, connector, true, recorder.callback(),
    [](std::vector<std::uint8_t> &) {});

  const auto goal_json = send_goal_json("/rws_test_fibonacci_a", "act_1");
  handler->process_message_rapid(goal_json.c_str(), goal_json.size());
  ASSERT_TRUE(recorder.wait_for_op("action_feedback", 3s)) << "goal never produced feedback";

  // Disconnect mid-goal. Pre-fix this left callbacks capturing the destroyed
  // handler registered on the action client — the next feedback dispatch was
  // a use-after-free that crashed the process.
  recorder.handler_destroyed = true;
  handler.reset();

  EXPECT_TRUE(wait_for_condition([&]() { return server.cancels_received() > 0; }, 3s))
    << "disconnect did not cancel the orphaned goal";
  EXPECT_TRUE(wait_for_condition([&]() { return server.goals_finished() > 0; }, 3s));

  // A few feedback periods with the handler gone: nothing may be delivered.
  std::this_thread::sleep_for(500ms);
  EXPECT_EQ(recorder.frames_after_destroy, 0)
    << "callback fired after the handler was destroyed";
}

TEST(ActionDisconnectTest, surviving_client_is_unaffected_and_cancel_delivers_canceled_result)
{
  auto node = std::make_shared<rclcpp::Node>("rws_action_isolation_test");
  auto node_interface = std::make_shared<rws::NodeInterfaceImpl>(node);
  auto connector = std::make_shared<rws::Connector<>>(node_interface);
  SlowFibonacciServer server(node, "/rws_test_fibonacci_b");

  // Action clients are shared per action: both handlers use one instance.
  auto client_a = node_interface->create_generic_action_client(
    "/rws_test_fibonacci_b", "test_msgs/action/Fibonacci");
  auto client_b = node_interface->create_generic_action_client(
    "/rws_test_fibonacci_b", "test_msgs/action/Fibonacci");
  EXPECT_EQ(client_a.get(), client_b.get());

  rclcpp::executors::MultiThreadedExecutor executor;
  executor.add_node(node);
  ExecutorSpinGuard spin_guard(executor);

  OutboundRecorder recorder_a;
  auto handler_a = std::make_shared<rws::ClientHandler>(
    1, node_interface, connector, true, recorder_a.callback(),
    [](std::vector<std::uint8_t> &) {});
  OutboundRecorder recorder_b;
  auto handler_b = std::make_shared<rws::ClientHandler>(
    2, node_interface, connector, true, recorder_b.callback(),
    [](std::vector<std::uint8_t> &) {});

  const auto goal_a = send_goal_json("/rws_test_fibonacci_b", "act_a");
  handler_a->process_message_rapid(goal_a.c_str(), goal_a.size());
  const auto goal_b = send_goal_json("/rws_test_fibonacci_b", "act_b");
  handler_b->process_message_rapid(goal_b.c_str(), goal_b.size());
  ASSERT_TRUE(recorder_a.wait_for_op("action_feedback", 3s));
  ASSERT_TRUE(recorder_b.wait_for_op("action_feedback", 3s));

  // Destroying B must only sever B: A's goal keeps streaming feedback.
  recorder_b.handler_destroyed = true;
  handler_b.reset();
  EXPECT_TRUE(wait_for_condition([&]() { return server.cancels_received() == 1; }, 3s));
  const int a_feedback_before = recorder_a.count_op("action_feedback");
  EXPECT_TRUE(wait_for_condition(
    [&]() { return recorder_a.count_op("action_feedback") > a_feedback_before; }, 3s))
    << "surviving client stopped receiving feedback";
  EXPECT_EQ(recorder_b.frames_after_destroy, 0);

  // An explicit cancel still delivers the CANCELED action_result (status 5).
  const auto feedback_frame = recorder_a.last_frame_with_op("action_feedback");
  rapidjson::Document feedback_doc;
  feedback_doc.Parse(feedback_frame.c_str());
  ASSERT_TRUE(feedback_doc.HasMember("goal_id"));
  const std::string cancel_json = std::string(
    R"({"op":"cancel_action_goal","id":"act_a","action":"/rws_test_fibonacci_b","goal_id":")") +
    feedback_doc["goal_id"].GetString() + R"("})";
  handler_a->process_message_rapid(cancel_json.c_str(), cancel_json.size());

  ASSERT_TRUE(recorder_a.wait_for_op("action_result", 3s))
    << "cancelled goal never delivered its result";
  rapidjson::Document result_doc;
  result_doc.Parse(recorder_a.last_frame_with_op("action_result").c_str());
  ASSERT_TRUE(result_doc.HasMember("status"));
  EXPECT_EQ(result_doc["status"].GetInt(), 5);  // GoalStatus.STATUS_CANCELED
}

}  // namespace rws

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();
  rclcpp::shutdown();
  return result;
}
