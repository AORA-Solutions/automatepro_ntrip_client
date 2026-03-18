// Copyright 2023 Australian Robotics Supplies & Technology
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

#include <cstdio>
#include <curl/curl.h>
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "rtcm_msgs/msg/message.hpp"
#include "ntrip_client/visibility_control.h"

using namespace std::chrono_literals;
using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

namespace ntrip_client
{
struct CurlHandle
{
  CURL * handle;
  CurlHandle()
  : handle(curl_easy_init()) {
    RCLCPP_WARN(rclcpp::get_logger("ntrip_client"), "CurlHandle constructor entered");
  }
  ~CurlHandle() {
    RCLCPP_WARN(rclcpp::get_logger("ntrip_client"), "CurlHandle destructor entered");
    curl_easy_cleanup(handle);
  }
};

class NTRIPClientNode : public rclcpp::Node
{
public:
  NTRIP_CLIENT_NODE_PUBLIC
  explicit NTRIPClientNode(const rclcpp::NodeOptions & options)
  : Node("ntrip_client",
      rclcpp::NodeOptions(options)),
    curlHandle_(std::make_shared<CurlHandle>()),
    streaming_exit_(false),
    desired_count_reached_(false),
    callback_count_(0),
    stream_attempt_count_(0)
  {
    RCLCPP_WARN(this->get_logger(), "[DEBUG] NTRIPClientNode constructor entered");
    RCLCPP_INFO(this->get_logger(), "starting %s", get_name());

    declare_parameter("use_https", false);
    declare_parameter("host", "rtk2go.com");
    declare_parameter("port", 2101);
    declare_parameter("mountpoint", "Prittlebach");
    declare_parameter("username", "noname");
    declare_parameter("password", "password");

    use_https_ = get_parameter("use_https").as_bool();
    host_ = get_parameter("host").as_string();
    port_ = get_parameter("port").as_int();
    mountpoint_ = get_parameter("mountpoint").as_string();
    username_ = get_parameter("username").as_string();
    password_ = get_parameter("password").as_string();

    std::string url = ConnectionUrl();
    RCLCPP_INFO(this->get_logger(), "ntrip connection url: '%s'", url.c_str());

    std::string userpwd = username_ + ":" + password_;
    RCLCPP_DEBUG(this->get_logger(), "userpwd: '%s'", userpwd.c_str());

    rtcm_pub_ = this->create_publisher<rtcm_msgs::msg::Message>("/rtcm", 10);

    curl_global_init(CURL_GLOBAL_DEFAULT);

    int desiredCount = 10;
    auto handle = curlHandle_->handle;
    if (handle) {
      curl_easy_setopt(handle, CURLOPT_URL, url.c_str());
      curl_easy_setopt(handle, CURLOPT_HTTP09_ALLOWED, true);
      curl_easy_setopt(handle, CURLOPT_USERPWD, userpwd.c_str());
      curl_easy_setopt(handle, CURLOPT_USERAGENT, "NTRIP ros2/ublox_dgnss");
      curl_easy_setopt(handle, CURLOPT_FAILONERROR, true);
      curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, &NTRIPClientNode::WriteCallback);
      curl_easy_setopt(handle, CURLOPT_WRITEDATA, this);
      curl_easy_setopt(handle, CURLOPT_PRIVATE, reinterpret_cast<void *>(desiredCount));

      RCLCPP_WARN(this->get_logger(), "[DEBUG] Configured forced stream cutoff path. desiredCount=%d stored via CURLOPT_PRIVATE=%p", desiredCount, reinterpret_cast<void *>(desiredCount));

      streamingThread_ = std::thread(&NTRIPClientNode::DoStreaming, this);
      RCLCPP_WARN(this->get_logger(), "[DEBUG] Streaming thread started, joinable=%s", streamingThread_.joinable() ? "true" : "false");
    } else {
      RCLCPP_ERROR(this->get_logger(), "curl_easy_init returned null handle");
    }
  }

private:
  std::shared_ptr<CurlHandle> curlHandle_;
  std::thread streamingThread_;
  bool streaming_exit_;
  bool desired_count_reached_;

  bool use_https_;
  std::string host_;
  int port_;
  std::string mountpoint_;
  std::string username_;
  std::string password_;

  rclcpp::Publisher<rtcm_msgs::msg::Message>::SharedPtr rtcm_pub_;

  std::atomic<uint64_t> callback_count_;
  std::atomic<uint64_t> stream_attempt_count_;

  std::string ConnectionUrl()
  {
    RCLCPP_WARN(this->get_logger(), "ConnectionUrl() entered");
    if (use_https_) {
      return "https://" + host_ + ":" + std::to_string(port_) + "/" + mountpoint_;
    }
    return "http://" + host_ + ":" + std::to_string(port_) + "/" + mountpoint_;
  }

  static size_t WriteCallback(char * ptr, size_t size, size_t nmemb, void * userdata)
  {
    auto * node = reinterpret_cast<NTRIPClientNode *>(userdata);
    RCLCPP_WARN(node->get_logger(), "[DEBUG] WriteCallback entered");
    if (!node) {
      RCLCPP_ERROR(rclcpp::get_logger("ntrip_client"), "WriteCallback: node is null");
      return 0;
    }
    if (!node->curlHandle_ || !node->curlHandle_->handle) {
      RCLCPP_ERROR(node->get_logger(), "WriteCallback: curlHandle or handle is null");
      return 0;
    }
    const size_t bytes = size * nmemb;
    const auto cb_num = ++node->callback_count_;
    RCLCPP_INFO(node->get_logger(), "WriteCallback enter: cb=%lu bytes=%zu size=%zu nmemb=%zu desired_count_reached_=%s", static_cast<unsigned long>(cb_num), bytes, size, nmemb, node->desired_count_reached_ ? "true" : "false");
    auto message = std::make_unique<rtcm_msgs::msg::Message>();
    message->header.stamp = node->get_clock()->now();
    message->header.frame_id = node->mountpoint_;
    message->message.assign(ptr, ptr + bytes);
    RCLCPP_WARN(node->get_logger(), "[DEBUG] Publishing RTCM message: cb=%lu bytes=%zu size=%zu nmemb=%zu mountpoint=%s", static_cast<unsigned long>(cb_num), bytes, size, nmemb, node->mountpoint_.c_str());
    node->rtcm_pub_->publish(std::move(message));
    RCLCPP_WARN(node->get_logger(), "[DEBUG] After publish: subscription_count=%zu", node->rtcm_pub_->get_subscription_count());
    if (!node->rtcm_pub_->get_subscription_count()) {
      RCLCPP_WARN(node->get_logger(), "No subscribers on /rtcm topic at cb=%lu", static_cast<unsigned long>(cb_num));
    }
    static int recordCount = 0;
    recordCount++;
    int desiredCount = -1;
    CURLcode info_res = curl_easy_getinfo(node->curlHandle_->handle, CURLINFO_PRIVATE, &desiredCount);
    RCLCPP_WARN(node->get_logger(), "[DEBUG] WriteCallback state: cb=%lu recordCount=%d curl_easy_getinfo(CURLINFO_PRIVATE)=%d desiredCount=%d", static_cast<unsigned long>(cb_num), recordCount, static_cast<int>(info_res), desiredCount);
    if (recordCount >= desiredCount) {
      recordCount = 0;
      node->desired_count_reached_ = true;
      RCLCPP_ERROR(node->get_logger(), "WriteCallback forcing short return: cb=%lu bytes=%zu returning=%zu desiredCount=%d", static_cast<unsigned long>(cb_num), bytes, bytes - 1, desiredCount);
      RCLCPP_WARN(node->get_logger(), "[DEBUG] Stream cutoff triggered: cb=%lu, desiredCount=%d", static_cast<unsigned long>(cb_num), desiredCount);
      return bytes - 1;
    }
    RCLCPP_DEBUG(node->get_logger(), "WriteCallback normal return: cb=%lu bytes=%zu", static_cast<unsigned long>(cb_num), bytes);
    return bytes;
  }

  void DoStreaming()
  {
    RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming() entered");
    while (!streaming_exit_) {
      RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming loop: streaming_exit_=%s", streaming_exit_ ? "true" : "false");
      desired_count_reached_ = false;
      const auto attempt = ++stream_attempt_count_;
      char * effective_url = nullptr;
      curl_easy_getinfo(curlHandle_->handle, CURLINFO_EFFECTIVE_URL, &effective_url);
      RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming begin: attempt=%lu url=%s callback_count=%lu streaming_exit_=%s", static_cast<unsigned long>(attempt), effective_url ? effective_url : "<null>", static_cast<unsigned long>(callback_count_.load()), streaming_exit_ ? "true" : "false");
      if (!curlHandle_ || !curlHandle_->handle) {
        RCLCPP_ERROR(this->get_logger(), "DoStreaming: curlHandle or handle is null");
        break;
      }
      CURLcode res = curl_easy_perform(curlHandle_->handle);
      RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming curl_easy_perform returned code=%d (%s)", static_cast<int>(res), curl_easy_strerror(res));
      long response_code = 0;
      curl_easy_getinfo(curlHandle_->handle, CURLINFO_RESPONSE_CODE, &response_code);
      curl_easy_getinfo(curlHandle_->handle, CURLINFO_EFFECTIVE_URL, &effective_url);
      RCLCPP_ERROR(this->get_logger(), "DoStreaming end: attempt=%lu res=%d (%s) response_code=%ld desired_count_reached_=%s callback_count=%lu url=%s streaming_exit_=%s", static_cast<unsigned long>(attempt), static_cast<int>(res), curl_easy_strerror(res), response_code, desired_count_reached_ ? "true" : "false", static_cast<unsigned long>(callback_count_.load()), effective_url ? effective_url : "<null>", streaming_exit_ ? "true" : "false");
      if (res != CURLE_OK) {
        if (desired_count_reached_) {
          RCLCPP_ERROR(this->get_logger(), "DoStreaming exited immediately after callback forced cutoff; sleeping 100 ms before retry");
          rclcpp::sleep_for(std::chrono::milliseconds(100));
          RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming retry after forced cutoff, attempt=%lu", static_cast<unsigned long>(attempt));
        } else {
          RCLCPP_ERROR(this->get_logger(), "DoStreaming real error path; sleeping 1 s before retry");
          rclcpp::sleep_for(std::chrono::seconds(1));
          RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming retry after error, attempt=%lu", static_cast<unsigned long>(attempt));
        }
      } else {
        RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming returned CURLE_OK; loop will restart unless streaming_exit_ is true");
        if (streaming_exit_) {
          RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming exiting loop due to streaming_exit_ true");
        }
      }
    }
    RCLCPP_INFO(this->get_logger(), "DoStreaming exiting because streaming_exit_ became true");
    RCLCPP_WARN(this->get_logger(), "[DEBUG] DoStreaming exit: streamingThread.joinable()=%s", streamingThread_.joinable() ? "true" : "false");
  }

public:
  ~NTRIPClientNode()
  {
    RCLCPP_WARN(this->get_logger(), "[DEBUG] ~NTRIPClientNode() entered");
    streaming_exit_ = true;
    if (streamingThread_.joinable()) {
      streamingThread_.join();
      RCLCPP_WARN(this->get_logger(), "[DEBUG] streamingThread joined in destructor");
    }
    curlHandle_.reset();
    curl_global_cleanup();
    RCLCPP_WARN(this->get_logger(), "NTRIPClientNode destructor called, streaming_exit_=%s", streaming_exit_ ? "true" : "false");
    RCLCPP_INFO(this->get_logger(), "finished");
  }
};
}  // namespace ntrip_client

RCLCPP_COMPONENTS_REGISTER_NODE(ntrip_client::NTRIPClientNode)
