// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/logging.h"

/*
一、原理分析：
这里讲的Cache缓存是指内存缓存，既然是内存缓存，因为内存有限，所以缓存肯定有一个容量大小capacity。通常我会将此缓存分解成多个小份的缓存。
下面的步骤，我们来模拟下LevelDB缓存创建和使用：
1、模拟创建一个缓存时，LevelDB的Cache对象结构。
1.1、LevelDB可以创建一个容量大小capacity 的Cache，
1.2、Cache子类ShardedLRUCache将容量大小capacity的缓存分成了很多个小缓存LRUCache。
1.3、小缓存LRUCache里实现了一个双向链表lru_和一个二级指针数组table_用来缓存数据。双向链表lru_用来保证当缓存容量饱满时，清除比较旧的缓存数据；二级指针数组table_用来保证缓存数据的快速查找。

2、模拟缓存一个数据时，LevelDB的Cache工作流程。
2.1、调用Cache的insert方法插入缓存数据data，子类ShardedLRUCache将缓存数据data进行hash操作，得到的hash值定位得到属于哪个小缓存LRUCache，LRUCache将缓存数据封装成LRUHandle数据对象，进行存储。
2.2、先将缓存数据添加到双向链表lru_中，由于lru_.pre存储比较新的数据，lru_.next存储比较旧的数据，所以将缓存数据添加在lru_.pre上。
2.3、再存储到二级指针数组table_里，存储之前，先查找数据是否存在。查找根据缓存数据的hash值，定位缓存数据属于哪个一级指针，然后遍历这一级指针上存放的二级指针链表，查找缓存数据。
2.4、最后如果缓存数据的总大小大于缓存LRUCache的容量大小，则循环从双向链表lru_的next取缓存数据，将其从双向链表lru_和二级指针数组table_中移除，直到缓存数据的总大小小于缓存LRUCache的容量大小。
*/

/*
 * 1. 创建一个缓存
 * 1) leveldb可以创建一个容量大小capacity的Cache
 * 2) Cache子类ShardedLRUCache将容量大小capacity的缓存分成了很多个小缓存LRUCache
 * 3) 小缓存LRUCache里实现了一个双向链表lru_和一个二级指针输出table_用来缓存数据,
 *    双向链表lru_用来保证当缓存容量饱满时,清除比较旧的缓存数据,二级指针数组table_
 *    用来保证缓存数据的快速查找。
 * 2. 插入一个缓存
 * 1) 调用Cache的insert方法插入缓存数据data,子类ShardedLRUCache将缓存数据data进行
 *    hash操作,得到的hash值定位到属于哪个小缓存LRUCache,LRUCache将缓存数据封装成
 *    LRUHandle数据对象存储
 * 2) 先将缓存数据添加到双向链表lru_中，由于lru_.pre存储比较新的数据,lru_.next存
 *    储比较旧的数据,所以将缓存数据添加在lru_.pre上。
 * 3) 再存储到二级指针数组table_里,存储之前,先查找数据是否存在。查找根据缓存数据
 *    的hash值,定位缓存数据属于哪个一级指针,然后遍历这一级指针上存放的二级指针链
 *    表,查找缓存数据。
 * 4) 最后如果缓存数据的总大小大于缓存LRUCache的容量大小，则循环从双向链表lru_的
 *    next取缓存数据，将其从双向链表lru_和二级指针数组table_中移除，直到缓存数据
 *    的总大小小于缓存LRUCache的容量大小。
 *    
 */ 

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle 
{
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// leveldb自己实现的hash，随机读speeds up by ~5% 相对于内置hash。
class HandleTable 
{
 public:
  // 构造函数里面的初始化也就只会创建4个item的空间
  // 类似于int **list_ = (int**)(malloc(sizeof(int*)*4));
  // 只不过这里需要把int换成LRUHandle
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) 
  {
    return *FindPointer(key, hash);
  }

  // Case 1. bucket为空，也就是所在地址的那个链表还没有建立起来
  // Case 2. bucket不空，但是没有找到item
  // Case 3. bucket不空，但是找到了相同item.
  // 注意: 当old不为NULL时，表示hash表中已存在一个与要插入的元素的
  // hash值完全相同的元素，这样只是替换元素，不会改变元素个数
  LRUHandle* Insert(LRUHandle* h) 
  {
    // Case 1. 这个时候返回的是bucket的地址，也就是&list_[i];
    // Case 2. 返回的是链表里面最后一个&(tail->next_hash)
    // Case 3. 如果能够找到相等的key/hash, 假设链表a->b->nullptr
    //         b->hash b->key与h的相等
    //         那么这里拿到的是&(a->next_hash)
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    // Case 1. 取得list_[i]
    // Case 2. 取得node->next_hash的值
    // Case 3. old = a->next_hash, 也就是old指向b结点
    LRUHandle* old = *ptr;
    // Case 1. 这个时候old肯定为nullptr
    //         那么新加入的结点的next_hash_就设置为nullptr
    // Case 2. old的值也是nullptr. 相当于拿到了tail->next_hash
    //         那么这里使h->next_hash = nullptr.
    // Case 3. 此时就不为空了, h->next_hash = old->next_hash
    //         h->next = b->next_hash
    //         指向相等元素的下一个结点
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    // Case 1. list_[i] = h; 实际上就是修改了头指针
    // Case 2. 相当于是修改了tail->next_hash
    //         tail->next_hash = h;
    // Case 3. a->next_hash = h
    *ptr = h;
    // Case 1. 如果没有找到相应的元素，那么这里新插入了一个entry/slot
    //         elems_加加.
    // Case 2. 如果旧有的tail->next_hash值，注意新的tail->next_hash
    //         已经指向h了
    if (old == NULL) 
	{
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        // 如果元素已经增长到了length_
        // 那么重新hash
        Resize();
      }
    }
    // Case 1. 这里返回旧有的list_[i]的值，也就是nullptr.
    // Case 2. 这里返回旧的tail->next_hash
    return old;
  }

  // Case 1. bucket为空，也就是所在地址的那个链表还没有建立起来
  // Case 2. bucket不空，但是没有找到item
  // Case 3. bucket不空，但是找到了相同item.
  // 通过key/bash来移除一个item
  // 其中1. 2得到的*ptr都是为空
  // 所以不会有真正的删除动作
  // 那么下面的代码只考虑3.
  LRUHandle* Remove(const Slice& key, uint32_t hash) 
  {
    // 这里返回值只可能有两种情况
    // Case 3:
    //      a. &list[i];
    //      b.  a->b->nullptr; 如果b->hash && b->key相等
    //          那么这里返回&(a->next_hash);
    LRUHandle** ptr = FindPointer(key, hash);
    // Case 3:
    //      a. result = list[i]的值
    //      b. result = a->next_hash的值，也就是b
    LRUHandle* result = *ptr;
    // 只考虑Case 3.那么result肯定不为空
    if (result != NULL) {
      // *ptr理解成为前面的next_hash指针
      // 前面的next_hash指针指向找到的结点的后一个元素
      // 也就是前面的next_hash = b->next_hash
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  // 这个hash table是由一系列bucket组成
  // 每个bucket是由一个链表组成
  // length_是bucket的数目
  uint32_t length_;
  // elems_是存放的item的数目
  uint32_t elems_;
  // 指针，指向bucket数组
  // 一个bucket[i]就是LRUHandle*
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) 
  {
    // 这里取模的时候，用的是length_ - 1
    // 实际上是会把最后一个bucket给浪费掉
    // ptr这个时候，指向bucket的地址
    // 如果把bucket看成一个链表
    // ptr就指向链表头的地址
    // ptr == &list_[i];
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    // 如果ptr不为空，取next_hash依次遍历查找，直到找到要找的节点
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) 
	{
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  // 1.申请2倍内存空间
  // 2.依次处理每个bucket
  // 3.每个bucket里面的每个链表的每个元素，放到新的new_list[i]里面的时候，采用头插法，插入到链表中。
  void Resize() 
  {
	// Fist time alloc 4
    // // 需要新申请的内存的长度
    uint32_t new_length = 4;
	// if new length is short,next one is double value
    // 如果新的长度小于elems_
    // 那么长度直接2倍
    while (new_length < elems_) 
	{
      new_length *= 2;
    }
	// follow work is to renew the hash of element
	// alloc memory for hash array 
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    // count记录旧有的item数目
    uint32_t count = 0;
     // 这里依次遍历每个旧有的item
    for (uint32_t i = 0; i < length_; i++) 
	{
	  // variable h is array elements,it represent a LRUHand pointer
      // 取出每个item
      // 这里是重新建立了一把hash
      // 处理第i条hash链表上的节点
      LRUHandle* h = list_[i];
      while (h != NULL) 
	    {
        // 保存下一个hash节点
        LRUHandle* next = h->next_hash;
        // 当前节点的hash值
        uint32_t hash = h->hash;
        // 与前面链表的方法类似，得到&new_list_[i];
        // re-hash，只不过这个i会发生变化。
        // 注意这里取模的技巧
        // 由于内存分配部是2^n
        // 所以 & (2^n-1)可以更加快速，而不是用%法。
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        // 相当于链表头插入法
        h->next_hash = *ptr;
        *ptr = h;
        // 移动到下一个结点
        h = next;
        // 计数加加
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
// Cache中存储的entry，除了保存key-value以外还保存了一些维护信息。
class LRUCache 
{
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  size_t usage_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRUCache里实现了一个双向链表lru_和一个二级指针数组table_用来缓存数据。
  // 双向链表lru_用来保证当缓存容量饱满时，清除比较旧的缓存数据
  LRUHandle lru_;

  // 二级指针数组table_用来保证缓存数据的快速查找
  /* 
    二级指针数组，链表没有大小限制，动态扩展大小，保证数据快速查找，
    hash定位一级指针，得到存放在一级指针上的二级指针链表，遍历查找数据
  */
  HandleTable table_;
};

LRUCache::LRUCache()
    : usage_(0) 
{
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache::~LRUCache()
{
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e);
    e = next;
  }
}

void LRUCache::Unref(LRUHandle* e) 
{
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
    usage_ -= e->charge;
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* e) 
{
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) 
{
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    e->refs++;
    /*
      为什么要先删除，再加入。
      由于当缓存不够时，会清除lru_的next处的数据，保证清除比较旧的数据。
    */
    LRU_Remove(e);
    LRU_Append(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) 
{
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) 
{
  MutexLock l(&mutex_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());
  // insert into link first place
  LRU_Append(e);
  usage_ += charge;

  LRUHandle* old = table_.Insert(e);
  if (old != NULL) 
  {
    LRU_Remove(old);
    Unref(old);
  }

  // 缓存不够，清除比较旧的数据
  while (usage_ > capacity_ && lru_.next != &lru_) 
  {
    LRUHandle* old = lru_.next;
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    Unref(old);
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Erase(const Slice& key, uint32_t hash) 
{
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);
  if (e != NULL) {
    LRU_Remove(e);
    Unref(e);
  }
}

void LRUCache::Prune() 
{
  MutexLock l(&mutex_);
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    if (e->refs == 1) {
      table_.Remove(e->key(), e->hash);
      LRU_Remove(e);
      Unref(e);
    }
    e = next;
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits; // 2^4 == 16

// leveldb对外暴露的LRUCache
class ShardedLRUCache : public Cache 
{
 private:
  // 16个LRUCache
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) 
  {
    return Hash(s.data(), s.size(), 0);
  }

  // 得到shared_数组的下标
  static uint32_t Shard(uint32_t hash) 
  {
  /*
   * hash是4个字节,32位,向右移动28位,则剩下高4位有效位
   * 最小是0000,最大1111等于15,得到数字在[0,15]范围内.
   * */
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) 
  {
     /*
      将容量平均分成kNumShards份，如果有剩余，将剩余的补全。为什么要补全呢？
      例如设置容量大小为10，则最多就能放下大小为10的数据，现在将容量分成3份，
      如果不补全，余量被丢弃，每份容量则为3，总容量为9，需要放大小为10的数据则放不下了。
      如果补全，剩余量1加上2，每份就多得1个容量，也就每份容量为4，总容量为12，能保证数据都放下。
    */
    /*
      补全块，
      如果capacity除以kNumShards有余数，那么余数加上(kNumShards - 1)，
      除以kNumShards，就能多得到一块。
      如果如果capacity除以kNumShards无余数，那么0加上(kNumShards - 1)，
      除以kNumShards，还是0
    */
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) 
	  {
      shard_[s].SetCapacity(per_shard);
    }
  }

  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) 
  {
    const uint32_t hash = HashSlice(key);
	// Shard(hash) return a uint32_t number,which less then 16
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }

  virtual Handle* Lookup(const Slice& key) 
  {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }

  virtual void Release(Handle* handle) 
  {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) 
  {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) 
  {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() 
  {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual void Prune() 
  {
    for (int s = 0; s < kNumShards; s++) 
	{
      shard_[s].Prune();
    }
  }
  virtual size_t TotalCharge() const 
  {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) 
	{
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  DEBUG("new Cache success.");
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
