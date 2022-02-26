// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

/*
 * Little-endian mode low positon place memory low address
 * Big-endian mode low positon place memory hight address
 * es. 0x1234
 * 0x4000 hight address
 * 0x4001 low address
 * Little-endain    Big-endian 
 * 0x4000  0x34     0x12
 * 0x4001  0x12     0x34
 * */
// 32位整型数值转换成char*
void EncodeFixed32(char* buf, uint32_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    // Big-Endian 0xff = 1111 1111 (8 bytes)
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

// 64位整型数值转换成char*
void EncodeFixed64(char* buf, uint64_t value) {
  if (port::kLittleEndian) 
  {
    memcpy(buf, &value, sizeof(value));
  } 
  else 
  {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

// 32位整型数值转换成string
void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  // func of cstring,append a string at tail,push_back can be add a char at tail 
  dst->append(buf, sizeof(buf));
}

// 64位整型数值转换成string
void PutFixed64(std::string* dst, uint64_t value) 
{
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

// 32位整型转变长字符串
char* EncodeVarint32(char* dst, uint32_t v) 
{
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1<<7)) 
  {
	// v无符号整形,转换成char型保存
    *(ptr++) = v;
  } 
  else if (v < (1<<14)) 
  {
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
  } 
  else if (v < (1<<21)) 
  {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } 
  else if (v < (1<<28)) 
  {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } 
  else 
  {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst, uint32_t v) 
{
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

char* EncodeVarint64(char* dst, uint64_t v) 
{
  static const int B = 128;
  // char*型转换成unsigned char*无符号整型
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) 
  {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

void PutVarint64(std::string* dst, uint64_t v) 
{
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

void PutLengthPrefixedSlice(std::string* dst, const Slice& value) 
{
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value) 
{
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) 
  {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
	// byte 大于128
    if (byte & 128) 
	{
      // More bytes are present
      result |= ((byte & 127) << shift);
    } 
	else 
	{
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

// 解码
bool GetVarint32(Slice* input, uint32_t* value) 
{
  // p input起始位置,q input结束位置
  const char* p = input->data();
  const char* limit = p + input->size();
  // input 首字符保存了长度,value 保存了长度的值, p 为去掉长度的input字符串
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == NULL) 
  {
    return false;
  } 
  else 
  {
    *input = Slice(q, limit - q);
    return true;
  }
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) 
{
  uint32_t len;
  // p的第一个字符保存了字符串的长度
  p = GetVarint32Ptr(p, limit, &len);
  if (p == NULL) return NULL;
  if (p + len > limit) return NULL;
  *result = Slice(p, len);
  return p + len;
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) 
{
  uint32_t len;
  if (GetVarint32(input, &len) &&
      input->size() >= len) 
  {
    *result = Slice(input->data(), len);
	// 去掉len长度的前缀
    input->remove_prefix(len);
    return true;
  } 
  else 
  {
    return false;
  }
}

}  // namespace leveldb
