// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
/*  block的结构
               +----------------------------+ <----+
               |                            |      |
               |                            |      |
               |         C1                 |      |
               |                            |      |
               |                            |      |
+------------> +----------------------------+      |
|              |         C2                 |      |
|              |                            |      |
|      +-----> +----------------------------+      |
|      |       |                            |      |
|      |       |                            |      |
|      |       |         C3                 |      |
|      |       |                            |      |
|      |       |                            |      |
|      |       |                            |      |
|      |       +----------------------------+ <----------+
|      |       |                            |      |     |
|      |       |         C4                 |      |     |
|      |       |                            |      |     |
|      |       +----------------------------+      |     |
|      |       |     offset of c1           |      |     |
|      |       |                            +------+     |
|      |       +----------------------------+            |
+--------------+     offset of c2           |            |
       |       |                            |            |
       |       +----------------------------+            |
       +-------+     offset of c3           |            |
               |                            |            |
               +----------------------------+            |
               |     offset of c4           |------------+
               +----------------------------+
               |     TAIL                   |
               +----------------------------+
*/
#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

class Block 
{
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  uint32_t NumRestarts() const;

  // 数据区
  const char* data_;
  // 数据区的大小
  size_t size_;
  // 偏移位置
  uint32_t restart_offset_;     // Offset in data_ of restart array
  // 表示是否拥有这个数据，自己负责数据的申请和释放
  bool owned_;                  // Block owns data_[]

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);

  class Iter;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
