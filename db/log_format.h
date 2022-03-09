// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.txt for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

// log 文件内容是一系列 blocks, 每个 block 大小为 32KB(有时候最后一个 block 可能装不满)。
// 每个 block 由一系列 records 构成。
// 如果当前 block 恰好剩余 7 个字节(正好可以容纳 record 中的 checksum + length + type), 
// 并且一个新的非 0 长度的 record 要被写入, 那么 writer 必须在此处写入一个 FIRST 类型的 
// record(但是 length 字段值为 0, data 字段为空. 用户数据 data 部分需要写入下个 block, 
// 而且下个 block 起始还是要写入一个 header 不过其 type 为 middle)来填满该 block 尾部的 7 个字节, 
// 然后在接下来的 blocks 中写入全部用户数据.
enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,  // FULL 类型的 record 包含了一个完整的用户 record 的内容。

  // For fragments
  kFirstType = 2,  // FIRST 表示某个用户 record 的第一个 fragment
  kMiddleType = 3, // LAST 表示某个用户 record 的最后一个 fragment
  kLastType = 4    // MIDDLE 表示某个用户 record 的中间 fragments
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
