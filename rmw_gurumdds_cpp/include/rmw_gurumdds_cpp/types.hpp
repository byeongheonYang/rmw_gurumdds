// Copyright 2019 GurumNetworks, Inc.
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

#ifndef RMW_GURUMDDS_CPP__TYPES_HPP_
#define RMW_GURUMDDS_CPP__TYPES_HPP_

#include <atomic>
#include <cassert>
#include <exception>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <condition_variable>
#include <mutex>
#include <unordered_set>
#include <unordered_map>

#include "rcpputils/thread_safety_annotations.hpp"

#include "rmw/ret_types.h"

#include "rmw_gurumdds_cpp/dds_include.hpp"
#include "rmw_gurumdds_cpp/guid.hpp"
#include "rmw_gurumdds_cpp/visibility_control.h"

void on_request_subscription_matched(
  const dds_DataReader* the_reader,
  const dds_SubscriptionMatchedStatus* status);

void on_response_publication_matched(
  const dds_DataWriter * writer,
  const dds_PublicationMatchedStatus * status);

void on_participant_changed(
  const dds_DomainParticipant * a_participant,
  const dds_ParticipantBuiltinTopicData * data,
  dds_InstanceHandle_t handle);

void on_publication_changed(
  const dds_DomainParticipant * a_participant,
  const dds_PublicationBuiltinTopicData * data,
  dds_InstanceHandle_t handle);

void on_subscription_changed(
  const dds_DomainParticipant * a_participant,
  const dds_SubscriptionBuiltinTopicData * data,
  dds_InstanceHandle_t handle);

enum class client_status_t
{
  FAILURE,  // an error occurred when checking
  MAYBE,    // reader not matched, writer still matched
  YES,      // reader matched
  GONE      // not found writer, reader from client
};

class ServiceListener;
class ServicePubListener;

typedef struct _GurumddsWaitSetInfo
{
  dds_WaitSet * wait_set;
  dds_ConditionSeq * active_conditions;
  dds_ConditionSeq * attached_conditions;
} GurumddsWaitSetInfo;

typedef struct _GurumddsEventInfo
{
  virtual ~_GurumddsEventInfo() = default;
  virtual rmw_ret_t get_status(const dds_StatusMask mask, void * event) = 0;
  virtual dds_StatusCondition * get_statuscondition() = 0;
  virtual dds_StatusMask get_status_changes() = 0;
} GurumddsEventInfo;

typedef struct _GurumddsPublisherInfo : GurumddsEventInfo
{
  rmw_gid_t publisher_gid;
  dds_DataWriter * topic_writer;
  const rosidl_message_type_support_t * rosidl_message_typesupport;
  const char * implementation_identifier;
  int64_t sequence_number;
  rmw_context_impl_t * ctx;

  rmw_ret_t get_status(dds_StatusMask mask, void * event) override;
  dds_StatusCondition * get_statuscondition() override;
  dds_StatusMask get_status_changes() override;
} GurumddsPublisherInfo;

typedef struct _GurumddsPublisherGID
{
  uint8_t publication_handle[16];
} GurumddsPublisherGID;

typedef struct _GurumddsSubscriberInfo : GurumddsEventInfo
{
  rmw_gid_t subscriber_gid;
  dds_DataReader * topic_reader;
  dds_ReadCondition * read_condition;
  const rosidl_message_type_support_t * rosidl_message_typesupport;
  const char * implementation_identifier;
  rmw_context_impl_t * ctx;

  rmw_ret_t get_status(dds_StatusMask mask, void * event) override;
  dds_StatusCondition * get_statuscondition() override;
  dds_StatusMask get_status_changes() override;
} GurumddsSubscriberInfo;

typedef struct _GurumddsClientInfo
{
  const rosidl_service_type_support_t * service_typesupport;

  rmw_gid_t publisher_gid;
  rmw_gid_t subscriber_gid;
  dds_DataWriter * request_writer;
  dds_DataReader * response_reader;
  dds_ReadCondition * read_condition;

  const char * implementation_identifier;
  rmw_context_impl_t * ctx;

  int64_t sequence_number;
  int8_t writer_guid[16];
  int8_t reader_guid[16];
} GurumddsClientInfo;

typedef struct _GurumddsServiceInfo
{
  const rosidl_service_type_support_t * service_typesupport;

  rmw_gid_t publisher_gid;
  rmw_gid_t subscriber_gid;
  dds_DataWriter * response_writer;
  dds_DataReader * request_reader;
  dds_ReadCondition * read_condition;

  ServiceListener * listener_{nullptr}; // requester listener
  ServicePubListener * pub_listener_{nullptr}; // response listener

  const char * implementation_identifier;
  rmw_context_impl_t * ctx;
} GurumddsServiceInfo;

class ServicePubListener
{
  using subscription_set_t =
    std::unordered_set<Guid_t,
      hash_guid>;
  using client_endpoints_map_t =
    std::unordered_map<Guid_t,
      Guid_t,
      hash_guid>;

public:
  ServicePubListener(
    GurumddsServiceInfo * info)
  {
    (void) info;
    listener.on_liveliness_lost = nullptr;
    listener.on_offered_deadline_missed = nullptr;
    listener.on_offered_incompatible_qos = nullptr;
    listener.on_publication_matched = on_response_publication_matched;
  }

  template<class Rep, class Period>
  bool
  wait_for_subscription(
    const dds_GUID_t & guid,
    const std::chrono::duration<Rep, Period> & rel_time)
  {
    auto guid_is_present = [this, guid]()->bool
    {
      Guid_t guid_(guid);
      return subscriptions_.find(guid_) != subscriptions_.end();
    };

    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, rel_time, guid_is_present);
  }

  template<class Rep, class Period>
  client_status_t
  check_for_subscription(
    const dds_GUID_t & guid,
    const std::chrono::duration<Rep, Period> & max_blocking_time)
  {
    Guid_t guid_(guid);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // Check if the guid is still in the map
      if (clients_endpoints_.find(guid_) == clients_endpoints_.end()) {
        // Client is gone
        return client_status_t::GONE;
      }
    }
    // Wait for subscription
    if (!wait_for_subscription(guid_, max_blocking_time)) {
      return client_status_t::MAYBE;
    }
    return client_status_t::YES;
  }

  void endpoint_erase_if_exists(
    const Guid_t & endpointGuid)
  {
    Guid_t guid(endpointGuid);
    std::lock_guard<std::mutex> lock(mutex_);
    auto endpoint = clients_endpoints_.find(guid);
    if (endpoint != clients_endpoints_.end()) {
      clients_endpoints_.erase(endpoint->second);
      clients_endpoints_.erase(guid);
    }
  }

  void endpoint_add_reader_and_writer(
    const dds_GUID_t & readerGuid,
    const dds_GUID_t & writerGuid)
  {
    Guid_t reader_guid(readerGuid);
    Guid_t writer_guid(writerGuid);
    std::lock_guard<std::mutex> lock(mutex_);
    clients_endpoints_.emplace(reader_guid, writer_guid);
    clients_endpoints_.emplace(writer_guid, reader_guid);
  }

  const dds_DataWriterListener* get_listener()
  {
    return &listener;
  }

  void add_subscription_guid(const Guid_t & subscription_guid)
  {
    subscriptions_.insert(subscription_guid);
  }

  void endpoint_remove_guid(const Guid_t & endpointGuid)
  {
    subscriptions_.erase(endpointGuid);
    auto endpoint = clients_endpoints_.find(endpointGuid);
    if (endpoint != clients_endpoints_.end()) {
      clients_endpoints_.erase(endpoint->second);
      clients_endpoints_.erase(endpointGuid);
    }
  }

  std::mutex & get_mutex()
  {
    return mutex_;
  }

  void condition_notify_all()
  {
    cv_.notify_all();
  }

private:
  dds_DataWriterListener listener;
  std::mutex mutex_;
  subscription_set_t subscriptions_;
  client_endpoints_map_t clients_endpoints_;
  std::condition_variable cv_;
};

class ServiceListener {
public:
   explicit ServiceListener(
    GurumddsServiceInfo * info)
  : info_(info)
  {
    listener_.on_subscription_matched = on_request_subscription_matched;
  }

  void on_subscription_matched(
    dds_DataReader* reader,
    const dds_SubscriptionMatchedStatus* status)
  {
    if(status->current_count_change == -1) {
      Guid_t guid{0};
      if(dds_DataReader_get_guid_from_publication_handle(reader, status->last_publication_handle, guid.value) != dds_RETCODE_OK) {
        return;
      }
      info_->pub_listener_->endpoint_erase_if_exists(guid);
    }
  }

  dds_DataReaderListener * get_listener()
  {
    return &listener_;
  }

private:
  dds_DataReaderListener listener_;
  GurumddsServiceInfo * info_;
};

#endif  // RMW_GURUMDDS_CPP__TYPES_HPP_
