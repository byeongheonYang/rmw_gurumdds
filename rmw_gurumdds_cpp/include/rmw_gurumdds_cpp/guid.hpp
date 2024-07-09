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

#ifndef RMW_GURUMDDS_CPP__GUID_HPP_
#define RMW_GURUMDDS_CPP__GUID_HPP_

#include <cstring>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "rmw_gurumdds_cpp/dds_include.hpp"

typedef uint8_t octet;

/**
 * Structure to hold GUID information for DDS instances.
 * http://www.eprosima.com/docs/fast-rtps/1.6.0/html/_guid_8h_source.html
 *
 */

struct Guid_t
{
  static constexpr size_t kSize = 16;
  static constexpr size_t prefix_size = 12;
  static constexpr size_t entity_size = 4;
  octet value[kSize];

  Guid_t()
  {
    memset(value, 0, kSize);
  }

  explicit Guid_t(octet guid[kSize])
  {
    memcpy(value, guid, kSize);
  }

  Guid_t(const Guid_t & g)
  {
    memcpy(value, g.value, kSize);
  }

  Guid_t(Guid_t && g) noexcept
  {
    memmove(value, g.value, kSize);
  }

  Guid_t(const dds_GUID_t & guid) {
    memcpy(value, guid.prefix, prefix_size);
    memcpy(&value[prefix_size], &guid.entityId, entity_size);
  }

  Guid_t & operator=(const Guid_t & guidpre)
  {
    memcpy(value, guidpre.value, kSize);
    return *this;
  }

  Guid_t & operator=(dds_GUID_t & guid) noexcept
  {
    memcpy(value, guid.prefix, prefix_size);
    memcpy(&value[prefix_size], &guid.entityId, entity_size);
    return *this;
  }

  Guid_t & operator=(Guid_t && guidpre) noexcept
  {
    memmove(value, guidpre.value, kSize);
    return *this;
  }

#ifndef DOXYGEN_SHOULD_SKIP_THIS_PUBLIC

  bool operator==(const Guid_t & prefix) const
  {
    return memcmp(value, prefix.value, kSize) == 0;
  }

  bool operator!=(const Guid_t & prefix) const
  {
    return memcmp(value, prefix.value, kSize) != 0;
  }

#endif
};

inline bool operator<(const Guid_t & g1, const Guid_t & g2)
{
  for (uint8_t i = 0; i < Guid_t::kSize; ++i) {
    if (g1.value[i] < g2.value[i]) {
      return true;
    } else if (g1.value[i] > g2.value[i]) {
      return false;
    }
  }
  return false;
}

inline std::ostream & operator<<(std::ostream & output, const Guid_t & guiP)
{
  output << std::hex;
  for (uint8_t i = 0; i < Guid_t::kSize - 1; ++i) {
    output << static_cast<int>(guiP.value[i]) << ".";
  }
  output << static_cast<int>(guiP.value[Guid_t::kSize - 1]);
  return output << std::dec;
}

inline void dds_BuiltinTopicKey_to_GUID(
  struct Guid_t * guid,
  dds_BuiltinTopicKey_t btk)
{
  memset(guid->value, 0, Guid_t::kSize);
#if BIG_ENDIAN
  memcpy(guid->value, reinterpret_cast<octet *>(btk.value), Guid_t::prefix_size);
#else
  octet const * keyBuffer = reinterpret_cast<octet *>(btk.value);
  for (uint8_t i = 0; i < 3; ++i) {
    octet * guidElement = &(guid->value[i * 4]);
    octet const * keyBufferElement = keyBuffer + (i * 4);
    guidElement[0] = keyBufferElement[3];
    guidElement[1] = keyBufferElement[2];
    guidElement[2] = keyBufferElement[1];
    guidElement[3] = keyBufferElement[0];
  }
#endif
}

struct hash_guid
{
    std::size_t operator()(const Guid_t & guid) const
  {
    union u_convert {
      uint8_t plain_value[sizeof(guid)];
      uint32_t plain_ints[sizeof(guid) / sizeof(uint32_t)];
    } u {};

    static_assert(
      sizeof(guid) == 16 &&
      sizeof(u.plain_value) == sizeof(u.plain_ints) &&
      offsetof(u_convert, plain_value) == offsetof(u_convert, plain_ints),
      "Plain guid should be easily convertible to uint32_t[4]");

    memcpy(u.plain_value, guid.value, Guid_t::kSize);

    constexpr std::size_t prime_1 = 7;
    constexpr std::size_t prime_2 = 31;
    constexpr std::size_t prime_3 = 59;

    size_t ret_val = prime_1 * u.plain_ints[0];
    ret_val = prime_2 * (u.plain_ints[1] + ret_val);
    ret_val = prime_3 * (u.plain_ints[2] + ret_val);
    ret_val = u.plain_ints[3] + ret_val;

    return ret_val;
  }
};

#endif  // RMW_GURUMDDS_CPP__GUID_HPP_
