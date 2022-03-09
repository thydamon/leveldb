// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <stdint.h>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

// log 文件(*.log)保存着数据库最近一系列更新操作, 它相当于 leveldb 的 WAL(write-ahead logging). 
// 当前在用的 log 文件内容同时也会被记录到一个内存数据结构中(即 memtable ). 
// 每个更新操作都被追加到当前的 log 文件和 memtable 中。当 log 文件大小达到一个预定义的大小时(默认大约 4MB), 
// 这个 log 文件对应的 memtable 就会被转换为一个 sorted table 文件落盘然后一个新的 log 文件就会被创建以保存未来的更新操作。
class Reader 
{
	public:
		// Interface for reporting errors.
		class Reporter 
		{
			public:
				virtual ~Reporter();

				// Some corruption was detected.  "size" is the approximate number
				// of bytes dropped due to the corruption.
				// corruption 变体
				// detectef 发现
				// approximate 近似的
				virtual void Corruption(size_t bytes, const Status& status) = 0;
		};

		// Create a reader that will return log records from "*file".
		// "*file" must remain live while this Reader is in use.
		//
		// If "reporter" is non-NULL, it is notified whenever some data is
		// dropped due to a detected corruption.  "*reporter" must remain
		// live while this Reader is in use.
		//
		// If "checksum" is true, verify checksums if available.
		//
		// The Reader will start reading at the first record located at physical
		// position >= initial_offset within the file.
		// 创建一个 Reader 来从 file 中读取和解析 records, 
		// 读取的第一个 record 的起始位置位于文件 initial_offset 或其之后的物理地址. 
		// 如果 reporter 不为空, 则在检测到数据损坏时汇报要丢弃的数据估计大小. 
		// 如果 checksum 为 true, 则在可行的条件比对校验和. 
		// 注意, file 和 reporter 的生命期不能短于 Reader 对象. 
		Reader(SequentialFile* file, Reporter* reporter, bool checksum,
				uint64_t initial_offset);

		~Reader();

		// Read the next record into *record.  Returns true if read
		// successfully, false if we hit end of the input.  May use
		// "*scratch" as temporary storage.  The contents filled in *record
		// will only be valid until the next mutating operation on this
		// reader or the next mutation to *scratch.
		// 方法负责从 log 文件读取内容并反序列化为 Record。 
		// 该方法会在 db 的 Open 方法中调用, 负责将磁盘上的 log 文件转换为内存中 memtable。
		// 其它数据库恢复场景也会用到该方法.
		// 所做的事情, 概括地讲就是从文件读取下一个 record 到 *record 中。
		// 如果读取成功, 返回 true; 遇到文件尾返回 false。
		// 如果当前读取的 record 没有被分片, 那就用不到 *scratch 参数来为 *record 做底层存储了。
		// 其它情况需要借助 *scratch 来拼装分片的 record data 部分, 最后封装为一个 Slice 赋值给 *record。
		bool ReadRecord(Slice* record, std::string* scratch);

		// Returns the physical offset of the last record returned by ReadRecord.
		//
		// Undefined before the first call to ReadRecord.
		uint64_t LastRecordOffset();

	private:
		SequentialFile* const file_;
		Reporter* const reporter_;
		bool const checksum_;
		char* const backing_store_;
		Slice buffer_;
		bool eof_;   // Last Read() indicated EOF by returning < kBlockSize

		// Offset of the last record returned by ReadRecord.
		uint64_t last_record_offset_;
		// Offset of the first location past the end of buffer_.
		uint64_t end_of_buffer_offset_;

		// Offset at which to start looking for the first record to return
		uint64_t const initial_offset_;

		// True if we are resynchronizing after a seek (initial_offset_ > 0). In
		// particular, a run of kMiddleType and kLastType records can be silently
		// skipped in this mode
		bool resyncing_;

		// Extend record types with the following special values
		enum {
			kEof = kMaxRecordType + 1,
			// Returned whenever we find an invalid physical record.
			// Currently there are three situations in which this happens:
			// * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
			// * The record is a 0-length record (No drop is reported)
			// * The record is below constructor's initial_offset (No drop is reported)
			kBadRecord = kMaxRecordType + 2
		};

		// Skips all blocks that are completely before "initial_offset_".
		//
		// Returns true on success. Handles reporting.
		bool SkipToInitialBlock();

		// Return type, or one of the preceding special values
		unsigned int ReadPhysicalRecord(Slice* result);

		// Reports dropped bytes to the reporter.
		// buffer_ must be updated to remove the dropped bytes prior to invocation.
		void ReportCorruption(uint64_t bytes, const char* reason);
		void ReportDrop(uint64_t bytes, const Status& reason);

		// No copying allowed
		Reader(const Reader&);
		void operator=(const Reader&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
