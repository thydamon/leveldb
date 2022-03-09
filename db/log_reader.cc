// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

bool Reader::SkipToInitialBlock() {
  // 块中偏移
  size_t offset_in_block = initial_offset_ % kBlockSize;
  // 需要跳过的块的位置
  // 这个变量的意思是说，后面在读的时候，要读的块的开头地址是什么？
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  // 如果给定的初始位置的块中偏移
  // 刚好掉在了尾巴上的6个bytes以内。那么
  // 这个时候，应该是需要直接切入到下一个block的。
  if (offset_in_block > kBlockSize - 6) {
    offset_in_block = 0;
    block_start_location += kBlockSize;
  }

  // 注意end_of_buffer_offset的设置是块的开始地址。
  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  // last_record_offset_ 表示上一次调用 ReadRecord 方法返回的
  // record 的起始偏移量, 注意这个 record 是逻辑的。
  // initial_offset_ 表示用户创建 Reader 时指定的在文件中寻找第一个 record 的起始地址。
  // 如果条件成立, 表示当前方法是首次被调用。
  if (last_record_offset_ < initial_offset_) {
    // 跳到我们要读取的第一个 block 起始位置
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  // 指示正在处理的 record 是否被分片了, 
  // 除非逻辑 record 对应的物理 record 类型是 full, 否则就是被分片了。
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  // 记录我们正在读取的逻辑 record 的起始偏移量. 初值为 0 无实际意义仅为编译器不发警告。
  // 为啥叫逻辑 record 呢？
  // 因为 block 大小限制, 所以 record 可能被分成多个分片(fragment). 
  // 我们管 fragment 叫物理 record, 一个或多个物理 record 构成一个逻辑 record. 
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    // 计算返回的当前 record 在 log file 中的起始地址
    //    = 当前文件待读取位置
    //      - buffer 剩余字节数
    //      - 刚读取的 record 头大小
    //      - 刚读取 record 数据部分大小
    // end_of_buffer_offset_ 表示 log file 待读取字节位置
    // buffer_ 表示是对一整个 block 数据的封装, 底层存储为 backing_store_, 
    //    每次执行 ReadPhysicalRecord 时会移动 buffer_ 指针.
    uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();
    // 从文件读取一个物理 record 并将其 data 部分保存到 fragment, 
    // 同时返回该 record 的 type，这里是读一个物理上的record。并不是一个完整的slice信息。
    const unsigned int record_type = ReadPhysicalRecord(&fragment);
    // resyncing_主要是指需要跳过的部分。
    // 跳过的时候是跳过一个完整的record。
    if (resyncing_) {  // 初始值为构造函数中initial_offset > 0
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    // 到这里的时候，读取的就是一个完整的slice的开头了。
    // 所以这里才开始正常的处理。
    switch (record_type) {
      case kFullType:
      // 如果处理“读在中间”的状态。应变报错。
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        // scratch就是用来缓存<firstRecord, middleRecord, lastRecord>
        // 不断地把这些record的数据区放到scratch里面缓存并且拼接起来。
        // 如果读到的是一个full type的record，还拼接啥啊。
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        // 记录下最后一个record的偏移量
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        // “读在中间”的状态也不应该遇到kFirstType。
        // 也就是不应该读到下一个record的开头。
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        // 当遇到middle type的时候。必然是“读在中间”状态。如果不是，报错！！
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        // 读到lastType的时候，也必然是处在“读在中间”的状态。如果不是，报错！！
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        // 文件都读结束了，还处在“读在中间”状态。说明写入的时候没有写入一个完整的
        // record。没办法，直接向客户端返回没有完整的slice数据了。
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        // 如果读到了坏的record，又刚好处理“读在中间”的状态。那么返回出错!!
        // 如果这个坏掉的record不是在读的record范围里面。直接返回读失败。
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        // 不应该有其他type。直接报错!!
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != NULL &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

// 首先需要想到，写入的时候是按照32KB一个block来写入。在读取的时候，就可以32KB来读入了。
// 所以在读入的时候，肯定是以32KB为单位来读的。下面就是读入32KB block的代码。
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    // 如果发现buffer的大小已经小于kHeaderSize了
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        // 如果还没有遇到结束
        // 上一次的读是一个完整的读。那么可能这里有一点尾巴需要处理。
        buffer_.clear();
        // 这里是读kBlockSize个字符串到buffer_里面。
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        // 先把偏移处理了
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        // 注意：如果buffer_是非空的。我们有一个truncated header在文件的尾巴。
        // 这可能是由于在写header时crash导致的。
        // 与其把这个失败的写入当成错误来处理，还不如直接当成EOF呢。
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    // 接下来的代码部分：从一个block里面取出一个完整的record
    // 这么一长串的代码都是在解析头部
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    // 如果头部记录的数据长度比实际的buffer_.size还要大。那肯定是出错了。
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    // 如果是zero type。那么返回Bad Record
    // 这种情况是有可能的。比如写入record到block里面之后。可能会遇到
    // 还余下7个bytes的情况。这个时候只能写入一个空的record。
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    // 移除头部
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
