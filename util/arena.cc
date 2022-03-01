// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <assert.h>

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena() : memory_usage_(0) {
  alloc_ptr_ = NULL;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
}

// 释放整个内存池所占内存
Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  // 如果需求的内存大于内存块中剩余的内存，而且大于1K，则给这内存单独分配一块bytes大小的内存。
  // 这样可以避免浪费过多的空间（因为如果bytes大于1K也从4K的内存块去取用，那么如果当前内存块中刚好剩余
  // 1K，只能再新建一个4K的内存块，并且取用bytes。此时新建的内存块是当前内存块，后续操作都是基于当前内
  // 存块的，那么原内存块中的1K空间就浪费了）
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  // 如果需求的内存大于内存块中剩余的内存，而且小于1K，则重新分配一个内存块，默认大小4K，
  // 原内存块中剩余的内存浪费掉（这样虽然也会浪费，但是浪费的空间小于1K）。并在新内存块
  // 中取用bytes大小的内存。
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

// 提供了字节对齐内存分配，一般情况是4字节或8个字节对齐分配，
// 对齐内存的好处简单的说就是加速内存访问。
// 首先获取一个指针的大小const int align = sizeof(void*)，
// 很明显，在32位系统下是4 ,64位系统下是8 ，为了表述方便，我们假设是32位系统，即align ＝ 4,
// 然后将我们使用的char * 指针地址转换为一个无符号整型(reinterpret_cast<uintptr_t>(result):
// It is an unsigned int that is guaranteed to be the same size as a pointer.)，通过与操作来
// 获取size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);当前指针模4
// 的值，有了这个值以后，我们就容易知道，还差 slop = align - current_mod多个字节，内存才是对齐的，
// 所以有了result = alloc_ptr + slop。那么获取bytes大小的内存，实际上需要的大小为needed = bytes + slop。
char* Arena::AllocateAligned(size_t bytes) 
{
  // 32bit system pointer size is 4bytes,64bit system pointer size is 8bytes.
  // allocate memory size with pointer multiple,it can speed cup fast,called Address Alignment
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // It is a best method to judege number power of 2
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  // In Linux system uintptr_t-->unsigned long int,intptr-->long int
  // 32bit system long int sizeof 4,64bit system long int sizeof 8
  // reinterpret_cast can convert from different type variable. es,char *_->long int
  // after convert with the same bits
  // alloc_ptr mod align-1
  // 这里就判断出了当前指针地址和字节对齐整数倍所差的个数
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1); // alloc_ptr=4,align=4,current_mod == 0
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  // result should be multiple by align 
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}

// 分配新的内存块
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}

}  // namespace leveldb
