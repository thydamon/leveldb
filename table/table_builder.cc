// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  // 传进来的选项
  Options options;
  // data block index的选项
  // 实际上,这里可以节省空间的,因为有用的只有一个option
  Options index_block_options;
  // 文件写出指针
  WritableFile* file;
  // 当前写的offset，写到哪了。
  // 注意：这个offset初始值为0，也就是说，
  // 它假设的是这个文件一开始的内容就是空的。
  // 或者说根本不关心当前文件的指针位置
  uint64_t offset;
  // 写的状态，之前的写入是否出错了
  Status status;
  // data block
  // 真正存放key/value的地方
  BlockBuilder data_block;
  // 用来记录data block index的block
  BlockBuilder index_block;
  // 记录最后添加的key
  // 每个新来的key要进来的时候，都要与这个key进行比较，进而保证
  // 整体的顺序是有序的
  std::string last_key;
  // 总共的条目数
  int64_t num_entries;
  // 是否关闭了
  bool closed;          // Either Finish() or Abandon() has been called.
  // meta block
  // 由于meta block只有一个
  // 所以当写完meta block之后
  // 立即可以写入meta block index块
  // 这也就是为什么没有meta block的原因
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        // 根据情况来看是否需要生成相应的filter policy
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        // 一开始并没有pending
        // 这个flag为true表明需要添加data block index item.
        pending_index_entry(false) {
    // 一开始把这个restart 设置为1
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    // 一开始设置为0，那么也就是说，目前还不会生成filter block
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  // rep_->必须已经被调用了
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  // filter_block是申请出来的
  // 这里应该是需要判空的
  delete rep_->filter_block;
  delete rep_;
}

// 实际上这个接口没有什么用，可以删掉
Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

/*
  当生成好TableBuilder之后，可能会想着开始添加第一个key/value了。 
  只是在看代码的时候，需要记住data block index这个块的结构是:
  //          |key  | BlockHandle    |
  //          | key | blockHandle    |
  //          | offset1        |
  //          | offset2        |
  //          | number_of_items|
  //          | compress type 1byte|
  //          | crc32 4byte        |
*/
// 里需要添加一个key/value
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  // 如果已经添了加很多entry
  // 那么与最后一条进行比较
  // 一定要保证有序性n
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 1.构建index_block
  // 当一个data Block被写入到磁盘时，为true
  // 当刚初始化好的时候，这个pending = false并不成立
  // 这里的意思是说，当需要生成一个新的data block的时候，
  // 这里需要生成一个新的data block index item.
  if (r->pending_index_entry) {
    // 此时data_block肯定为空
    assert(r->data_block.empty());
    // 那么找一个能够分隔开的key
    // 考虑这两个key"the quick brown fox"和"the who"， 进FindShortestSeparator
    // 处理后，r->last_key=the r。这样的话r->last_key就大于上一个data block的
    // 所有key，并且小于后面所有data block的key。
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    // 找一段内存存放handle的编码
    std::string handle_encoding;
    // 这里把pending_handle编码到handle内存中
    r->pending_handle.EncodeTo(&handle_encoding);
    // 把block index添加到这段内存中
    // 这里注意需要按照data block index的格式来设置
    // 其格式需  |key | BlockHandle    |
    //          | key | blockHandle    |
    //          | offset1        |
    //          | offset2        |
    //          | number_of_items|
    //          | compress type 1byte|
    //          | crc32 4byte        |
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    // 添加完data block index之后
    // 清空需要添加flag
    r->pending_index_entry = false;
  }

  // 2. 构建过滤器
  // 如果filter block不为空，那么就把这个key添加到filter里面。
  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  // 3. 记录数据
  // 设置最后一个key
  r->last_key.assign(key.data(), key.size());
  // 已经存入table中的item
  r->num_entries++; // 把key/value放到data block里面 r->data_block.Add(key, value);
  // 4. 数据块(Data Block)大小已达上限，写入文件
  // 如果Data Block的block_data字段大小满足要求，准备写入到磁盘
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  // 如果预计的大小大于等于block_size
  // 那么就需要刷写了
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  // assert 这个时候并不需要写data block index item
  // 这个要等到下一轮的时候再刷写
  assert(!r->pending_index_entry);
  // 刷写一个data block
  WriteBlock(&r->data_block, &r->pending_handle);
  // 如果刷写成功，那么也需要刷写一次data block index item
  if (ok()) {
    // 这里设置一个标志位，让i+1个block去刷写
    r->pending_index_entry = true;
    // 文件真正落盘
    r->status = r->file->Flush();
  }
  // 每次刷写都会根据情况来查看是否需要重新生成key的filter
  if (r->filter_block != NULL) {
    // 只有当超出2k data bytes，才会重新创建
    // 一个filter item
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 这里获取block的原始内存内容
  Slice raw = block->Finish();

  Slice block_contents;
  // 看一下是否需要压缩
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  // 压缩类型
  switch (type) {
    // 如果没有压缩，这里直接就设置成raw格式
    case kNoCompression:
      block_contents = raw;
      break;
    // 如果采用snappy压缩
    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      // 如果压缩率达到一定程度才用
      // 如果效果不好，那么就不用压缩了。
      // 所以这里需要根据压缩类型来选择
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        // 如果压缩效果不好，那么就设置成不要压缩
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // 直接写内存
  WriteRawBlock(block_contents, type, handle);
  // 清除压缩后的内存
  r->compressed_output.clear();
  // 重置block
  block->Reset();
}

// 这里真正的开始刷写Block的内存
// | block的内存       |
// | compress type 1B |
// | crc32 4 Byte     |
// 这里handle传进来没有别的用法，只是修改offset/size
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  // 修改offset/size
  Rep* r = rep_;
  // 注意：这里去修改了pending_handle的值
  // 这个函数后面并没有再使用这个handle
  // 这个值被修改之后
  // 需要等到下一个block的第一个key被添加进来的时候，才
  // 会用到
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  // 直接把内容Append到文件后面
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    // 开始写尾部的5个byte
    char trailer[kBlockTrailerSize];
    // 设置压缩类型
    trailer[0] = type;
    // 开始计算crc32
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    // 将type也进行计算
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    // 将校验结果进行编码，然后放到trailer的后面4个byte里面
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    // 把trailer也写入到文件中
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 记住写入的offset
      // 后面写data block index item的时候要用
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

/*
    [data block 1]
    [data block 2]
    ...
    [data block N]
    [meta block]  <-- 只有一个
    [metaindex block] <-- 只有一个
    [index block]
    [Footer]
*/
// 生成最终的结果，并且写到文件中
// 前面一直在写的是data block
// 这里开始要把meta block = filter block
// meta block index = filter block index
// data block index
// footer写下去
Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 把data block里面还没有刷写的，刷写下去
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != NULL) {
    // 写filter block的时候，直接获取filter block的内存
    // 然后采用无压缩的方式写入到文件中
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);  // 这里会用来更新和记录offset/size
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      // 前面filter_block_handle记录了相应的offset/size
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      // 把结果放到meta block index中
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  // 这里再写data block index
  if (ok()) {
    if (r->pending_index_entry) {
      // 由于是最后的刷写
      // 所以pending_index_entry为真为false已经不重要了。
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
