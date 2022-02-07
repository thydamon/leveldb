// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

// TableCache中的Value结构
struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

// 删除TableCache一条kv记录
// 1、删除leveldb在内存中的数据
// 2、关闭leveldb文件句柄 
static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

// 从构造函数中可以看出TableCache内部管理的是LRUCache对象
// 根据传入的entries（KV个数）创建TabelCache
TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) 
{
}

TableCache::~TableCache() {
  delete cache_;
}

// 查找sst文件对应的cache记录
// file_number：就是sst文件名
// file_size：sst文件大小
// handle：要返回的sst对应的cache实体
//查找流程是：
//1、file_number就是key，先去TableCache中查找，若找到则直接返回。
//2、TableCache中未找到，则需要打开此文件，先以后缀.ldb格式打开。
//3、若打开失败，尝试用文件后缀.sst格式打开。
//4、打开文件成功之后要创建Table实体，用于管理ldb文件内容。
//5、将打开的文件插入到TableCache中。
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  // 去缓存中查找
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    // 先以.ldb格式打开
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      // 尝试以后缀.sst格式打开
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      // 创建Table实体
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      // 插入缓存
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

//创建访问ldb文件的迭代器。
//1、先根据文件名找到ldb文件结构；
//2、根据找到的ldb结构，对table结构创建一个二层指针迭代器；
//3、注册迭代器销毁时的操作函数。
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) 
{
  if (tableptr != NULL) 
  {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) 
  {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) 
  {
    *tableptr = table;
  }
  return result;
}

//此方法就是查找ldb文件中是否存在key，若存在则执行handle_result函数。
//InternalGet()流程如下：
//1、先去ldb文件的index_block中查找key对应的block offset；
//2、根据block offset去Filter Block（若开启的话）中去查找；
//3、若确定存在，则去实际的DataBlock中去读取，同时执行handle_result方法。
Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

//删除ldb文件在tableCache中缓存
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
