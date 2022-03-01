// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
//#include "port/port.h"
#include "port/atomic_pointer.h"
#include "port/port_posix.h"

namespace leveldb {

// ---------------------------------------------------
// |  kBlockSize  |  kBlockSize  |  used  |  unused  |
// ---------------------------------------------------
//                                        |  alloc_bytes_remaining_ptr
//                                  alloc_ptr_
class Arena {
 public:
  Arena();
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  // 分配bytes大小的内存块，返回指向该内存块的指针
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  // 基于malloc的字节对齐内存分配
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  // 返回整个内存池使用内存的总大小（不精确）,这里只计算了已分配内存块的总大小和
  // 存储各个内存块指针所用的空间。并未计算alloc_ptr_和alloc_bytes_remaining_
  // 等数据成员的大小。
  size_t MemoryUsage() const {
    return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  // 当前内存块(block)偏移量指针，也就是未使用内存的首地址
  char* alloc_ptr_;
  // 表示当前内存块(block)中未使用的空间大小
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  // 用来存储每一次向系统请求分配的内存块的指针
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  // 迄今为止分配的内存块的总大小
  port::AtomicPointer memory_usage_;

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  // 当前剩余字节数够分配，直接分配
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  // 因为alloc_bytes_remaining_初始为0，因此第一次调用Allocate实际上直接调用的是AllocateFallback
  // 如果需求的内存大于内存块中剩余的内存，也会调用AllocateFallback
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
