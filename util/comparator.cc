// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
// 逐个字节比较
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  // 直接调用Slice的compare函数
  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }
  
  // FindShortestSeparator找到start、limit之间最短的字符串，如“helloworld”和”hellozoomer”之间最短的key可以是”hellox”。
  // *start: hellow
  // limit: helloz
  // 返回：*start变为hellox
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const 
  {
    // Find length of common prefix
    // 找到共同前缀的长度
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) 
	{
      diff_index++;
    }

    // 如果一个字符串是另个一字符串的前缀，无需做截断操作，否则进入else。
    if (diff_index >= min_length) 
	{
      // Do not shorten if one string is a prefix of the other
    } 
	else 
	{
      // start < limit，就把start修改为*start和limit的共同前缀的ascii加1
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) // 找到最短最近的长度 
	  {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  // 直接对key中第一个以uint8方式+1的字节+1，清除该位后面的数据。
  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

// 安全的单例模式,C++11保证了静态局部变量的初始化过程是线性安全的
const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule);
  return bytewise;
}

}  // namespace leveldb
