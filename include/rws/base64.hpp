// Copyright 2022 Vasily Kiniv
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

#ifndef RWS__BASE64_HPP_
#define RWS__BASE64_HPP_

#include <cstddef>
#include <cstdint>
#include <string>

// Self-contained base64 codec used to encode/decode byte-array (uint8[]/char[])
// fields per the rosbridge protocol. Standard alphabet, '=' padding.

namespace rws
{

/// Encode raw bytes to a standard base64 string.
inline std::string base64_encode(const unsigned char * data, size_t len)
{
  static const char tbl[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve(((len + 2) / 3) * 4);

  size_t i = 0;
  for (; i + 3 <= len; i += 3) {
    uint32_t n = (uint32_t(data[i]) << 16) | (uint32_t(data[i + 1]) << 8) | data[i + 2];
    out.push_back(tbl[(n >> 18) & 0x3F]);
    out.push_back(tbl[(n >> 12) & 0x3F]);
    out.push_back(tbl[(n >> 6) & 0x3F]);
    out.push_back(tbl[n & 0x3F]);
  }

  const size_t rem = len - i;
  if (rem == 1) {
    uint32_t n = uint32_t(data[i]) << 16;
    out.push_back(tbl[(n >> 18) & 0x3F]);
    out.push_back(tbl[(n >> 12) & 0x3F]);
    out.push_back('=');
    out.push_back('=');
  } else if (rem == 2) {
    uint32_t n = (uint32_t(data[i]) << 16) | (uint32_t(data[i + 1]) << 8);
    out.push_back(tbl[(n >> 18) & 0x3F]);
    out.push_back(tbl[(n >> 12) & 0x3F]);
    out.push_back(tbl[(n >> 6) & 0x3F]);
    out.push_back('=');
  }
  return out;
}

/// Decode a base64 string to raw bytes. Non-alphabet characters (padding,
/// whitespace, or otherwise invalid input) are skipped so malformed input
/// degrades gracefully rather than throwing.
inline std::string base64_decode(const char * data, size_t len)
{
  auto sextet = [](unsigned char c) -> int {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
  };

  std::string out;
  out.reserve((len / 4) * 3);
  int buf = 0;
  int bits = 0;
  for (size_t i = 0; i < len; ++i) {
    const int v = sextet(static_cast<unsigned char>(data[i]));
    if (v < 0) {
      continue;
    }
    buf = (buf << 6) | v;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push_back(static_cast<char>((buf >> bits) & 0xFF));
    }
  }
  return out;
}

/// Convenience overload for std::string input.
inline std::string base64_decode(const std::string & s)
{
  return base64_decode(s.data(), s.size());
}

}  // namespace rws

#endif  // RWS__BASE64_HPP_
