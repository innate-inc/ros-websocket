// Copyright 2026 Innate
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef RWS__GENERIC_SERVICE_HPP_
#define RWS__GENERIC_SERVICE_HPP_

#include <memory>
#include <string>

#include "rcl/service.h"
#include "rclcpp/exceptions.hpp"
#include "rclcpp/expand_topic_or_service_name.hpp"
#include "rclcpp/serialized_message.hpp"
#include "rclcpp/service.hpp"
#include "rclcpp/typesupport_helpers.hpp"
#include "rmw/rmw.h"
#include "rosidl_typesupport_introspection_cpp/service_introspection.hpp"
#include "rws/typesupport_helpers.hpp"

namespace rws
{

using rosidl_typesupport_introspection_cpp::ServiceMembers;

class GenericService : public rclcpp::ServiceBase
{
public:
  using SharedRequest = std::shared_ptr<rclcpp::SerializedMessage>;
  using SharedResponse = std::shared_ptr<rclcpp::SerializedMessage>;
  using CallbackType = std::function<void(std::shared_ptr<rmw_request_id_t>, SharedRequest)>;

  RCLCPP_SMART_PTR_DEFINITIONS(GenericService)

  GenericService(
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_base,
    const std::string & service_name,
    const std::string & service_type,
    rcl_service_options_t & service_options,
    CallbackType callback)
  : rclcpp::ServiceBase(node_base->get_shared_rcl_node_handle()),
    service_type_(service_type),
    callback_(callback)
  {
    srv_ts_lib_ = rclcpp::get_typesupport_library(service_type, rws::ts_identifier_srv);
    srv_ts_hdl_ =
      rws::get_service_typesupport_handle(service_type, rws::ts_identifier_srv, *srv_ts_lib_);

    srv_intro_lib_ = rclcpp::get_typesupport_library(service_type, rws::ts_identifier);
    srv_intro_hdl_ =
      rws::get_service_typesupport_handle(service_type, rws::ts_identifier, *srv_intro_lib_);
    auto srv_members = static_cast<const ServiceMembers *>(srv_intro_hdl_->data);

    auto request_type = get_type_from_message_members(srv_members->request_members_);
    req_ts_srv_lib_ = rclcpp::get_typesupport_library(request_type, rws::ts_identifier_srv);
    req_ts_srv_hdl_ =
      rclcpp::get_typesupport_handle(request_type, rws::ts_identifier_srv, *req_ts_srv_lib_);

    auto response_type = get_type_from_message_members(srv_members->response_members_);
    res_ts_srv_lib_ = rclcpp::get_typesupport_library(response_type, rws::ts_identifier_srv);
    res_ts_srv_hdl_ =
      rclcpp::get_typesupport_handle(response_type, rws::ts_identifier_srv, *res_ts_srv_lib_);

    service_handle_ = std::shared_ptr<rcl_service_t>(
      new rcl_service_t, [handle = node_handle_](rcl_service_t * service)
      {
        if (rcl_service_fini(service, handle.get()) != RCL_RET_OK) {
          RCLCPP_ERROR(
            rclcpp::get_node_logger(handle.get()).get_child("rclcpp"),
            "Error in destruction of rcl service handle: %s",
            rcl_get_error_string().str);
          rcl_reset_error();
        }
        delete service;
      });
    *service_handle_.get() = rcl_get_zero_initialized_service();

    rcl_ret_t ret = rcl_service_init(
      service_handle_.get(),
      node_handle_.get(),
      srv_ts_hdl_,
      service_name.c_str(),
      &service_options);
    if (ret != RCL_RET_OK) {
      if (ret == RCL_RET_SERVICE_NAME_INVALID) {
        rcl_reset_error();
        rclcpp::expand_topic_or_service_name(
          service_name,
          rcl_node_get_name(node_handle_.get()),
          rcl_node_get_namespace(node_handle_.get()),
          true);
      }

      rclcpp::exceptions::throw_from_rcl_error(ret, "could not create generic service");
    }
  }

  std::shared_ptr<void> create_request() override
  {
    auto srv_members = static_cast<const ServiceMembers *>(srv_intro_hdl_->data);
    return allocate_message(srv_members->request_members_);
  }

  std::shared_ptr<rmw_request_id_t> create_request_header() override
  {
    return std::make_shared<rmw_request_id_t>();
  }

  void handle_request(
    std::shared_ptr<rmw_request_id_t> request_header,
    std::shared_ptr<void> request) override
  {
    auto serialized_request = std::make_shared<rclcpp::SerializedMessage>();
    rmw_ret_t ret =
      rmw_serialize(request.get(), req_ts_srv_hdl_, &serialized_request->get_rcl_serialized_message());
    if (ret != RMW_RET_OK) {
      RCUTILS_LOG_ERROR_NAMED(
        "rws", "Failed to serialize advertised service request: %s",
        rcutils_get_error_string().str);
      rcutils_reset_error();
      return;
    }

    callback_(request_header, serialized_request);
  }

  void send_serialized_response(
    std::shared_ptr<rmw_request_id_t> request_header,
    SharedResponse serialized_response)
  {
    auto srv_members = static_cast<const ServiceMembers *>(srv_intro_hdl_->data);
    auto response = allocate_message(srv_members->response_members_);
    const rmw_serialized_message_t * sm = &serialized_response->get_rcl_serialized_message();
    rmw_ret_t rmw_ret = rmw_deserialize(sm, res_ts_srv_hdl_, response.get());
    if (rmw_ret != RMW_RET_OK) {
      rclcpp::exceptions::throw_from_rcl_error(rmw_ret, "failed to deserialize service response");
    }

    rcl_ret_t ret = rcl_send_response(get_service_handle().get(), request_header.get(), response.get());
    if (ret == RCL_RET_TIMEOUT) {
      RCLCPP_WARN(
        node_logger_.get_child("rclcpp"),
        "failed to send response to %s (timeout): %s",
        this->get_service_name(), rcl_get_error_string().str);
      rcl_reset_error();
      return;
    }
    if (ret != RCL_RET_OK) {
      rclcpp::exceptions::throw_from_rcl_error(ret, "failed to send service response");
    }
  }

  const std::string & service_type() const { return service_type_; }

private:
  RCLCPP_DISABLE_COPY(GenericService)

  std::string service_type_;
  CallbackType callback_;
  std::shared_ptr<rcpputils::SharedLibrary> srv_ts_lib_;
  const rosidl_service_type_support_t * srv_ts_hdl_;
  std::shared_ptr<rcpputils::SharedLibrary> srv_intro_lib_;
  const rosidl_service_type_support_t * srv_intro_hdl_;
  std::shared_ptr<rcpputils::SharedLibrary> req_ts_srv_lib_;
  const rosidl_message_type_support_t * req_ts_srv_hdl_;
  std::shared_ptr<rcpputils::SharedLibrary> res_ts_srv_lib_;
  const rosidl_message_type_support_t * res_ts_srv_hdl_;
};

}  // namespace rws

#endif  // RWS__GENERIC_SERVICE_HPP_
