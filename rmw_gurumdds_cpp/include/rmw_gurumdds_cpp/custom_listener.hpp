// Copyright 2022 GurumNetworks, Inc.
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

#ifndef RMW_GURUMDDS_CPP__CUSTOM_LISTENER_HPP_
#define RMW_GURUMDDS_CPP__CUSTOM_LISTENER_HPP_

#include <condition_variable>
#include <mutex>
#include <unordered_set>
#include <unordered_map>

#include "rmw_gurumdds_cpp/dds_include.hpp"
#include "rmw_gurumdds_cpp/types.hpp"
#include "rmw_gurumdds_cpp/guid.hpp"

class ServicePubListener;

enum class client_stuts_t
{
  FAILURE,  // an error occurred when checking
  MAYBE,    // reader not matched, writer still matched
  YES,      // reader matched
  GONE      // not found writer, reader from client
};

class ServicePubListener : public dds_DataWriterListener
{
  using subscription_set_t =
    std::unordered_set<Guid_t>;
  using client_endpoints_map_t =
    std::unordered_map<dds_GUID_t, 
    dds_GUID_t>;

public:
  explicit ClientPubListener(
    GurumddsServiceInfo * info)
  {
    (void) info;
  }

  void on_publication_matched(
  const dds_DataWriter * writer,
  const dds_PublicationMatchedStatus * status)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (status == nullptr) {
      return;
    }
    dds_GUID_t guid;
    dds_Retcode_t ret = dds_RETCODE_OK;
    if (status->current_count_change == 1) {
      ret = dds_DataWriter_get_guid_from_subscription_handle(writer, status.last_subscription_handle, &guid);
      if (ret != dds_RETCODE_OK) {
        return;
      }
      subscriptions_.insert(guid);
    } else if (status->current_count_change == -1) {
      ret = dds_DataWriter_get_guid_from_subscription_handle(writer, status.last_subscription_handle, &guid);
      if (ret != dds_RETCODE_OK) {
        return;
      }
      subscriptions_.erase(guid);
      auto endpoint = clients_endpoints_.find(guid);
      if (endpoint != clients_endpoints_.end()) {
        clients_endpoints_.erase(endpoint->second);
        clients_endpoints_.erase(guid);
      }
    } else {
      return;
    }
    cv_.notify_all();
  }

  template<class Rep, class Period>
  bool
  wait_for_subscription(
    const dds_GUID_t & guid,
    const std::chrono::duration<Rep, Period> & rel_time)
  {
    auto guid_is_present = [this, guid]() RCPPUTILS_TSA_REQUIRES(mutex_)->bool
    {
      return subscriptions_.find(guid) != subscriptions_.end();
    };

    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, rel_time, guid_is_present);
  }

  template<class Rep, class Period>
  client_stuts_t
  check_for_subscription(
    const dds_GUID_t & guid,
    const std::chrono::duration<Rep, Period> & max_blocking_time)
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // Check if the guid is still in the map
      if (clients_endpoints_.find(guid) == clients_endpoints_.end()) {
        // Client is gone
        return client_stuts_t::GONE;
      }
    }
    // Wait for subscription
    if (!wait_for_subscription(guid, max_blocking_time)) {
      return client_stuts_t::MAYBE;
    }
    return client_stuts_t::YES;
  }

  void endpoint_erase_if_exists(
    const dds_GUID_t & endpointGuid)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto endpoint = clients_endpoints_.find(endpointGuid);
    if (endpoint != clients_endpoints_.end()) {
      clients_endpoints_.erase(endpoint->second);
      clients_endpoints_.erase(endpointGuid);
    }
  }

  void endpoint_add_reader_and_writer(
    const dds_GUID_t & readerGuid,
    const dds_GUID_t & writerGuid)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    clients_endpoints_.emplace(readerGuid, writerGuid);
    clients_endpoints_.emplace(writerGuid, readerGuid);
  }

private:
  std::mutex mutex_;
  subscription_set_t subscriptions_ RCPPUTILS_TSA_GUARDED_BY(mutex_);
  client_endpoints_map_t client_endpoints_ RCPPUTILS_TSA_GUARDED_BY(mutex_);
  std::condition_variable cv_;
}


#endif  // RMW_GURUMDDS_CPP__CUSTOM_LISTENER_HPP_
