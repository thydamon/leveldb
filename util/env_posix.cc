// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <deque>
#include <set>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

class PosixSequentialFile: public SequentialFile 
{
	private:
		std::string filename_;
		FILE* file_;

	public:
		PosixSequentialFile(const std::string& fname, FILE* f)
			: filename_(fname), file_(f) { }
		virtual ~PosixSequentialFile() { fclose(file_); }

		// 从文件中读取n个字节存放到 "scratch[0..n-1]"， 然后将"scratch[0..n-1]"转化为Slice类型并存放到*result中
		// 如果正确读取，则返回OK status，否则返回non-OK status
		virtual Status Read(size_t n, Slice* result, char* scratch) 
		{
			Status s;
            // size_t fread_unlocked(void *ptr, size_t size, size_t n,FILE *stream);
            // ptr:用于接收数据的内存地址
            // size:要读的每个数据项的字节数，单位是字节
            // n:要读n个数据项，每个数据项size个字节
            // stream:输入流
            // 返回值：返回实际读取的数据大小
            // 因为函数名带了"_unlocked"后缀，所以它不是线程安全的
			size_t r = fread_unlocked(scratch, 1, n, file_);  // #define fread_unlocked fread
			// 将读取到的scratch转换成result
			*result = Slice(scratch, r);
			if (r < n) 
			{
				if (feof(file_)) 
				{
					DEBUG("PosixSequentialFile::Read return seccess.");
					// We leave status as ok if we hit the end of the file
					// 如果r<n，且feof(file_)非零，说明到了文件结尾，什么都不用做，函数结束后会返回OK Status
				} 
				else 
				{
					// A partial read with an error: return a non-ok status
					// 否则返回错误信息
					s = IOError(filename_, errno);
				}
			}
			return s;
		}

		// 跳过n字节的内容，这并不比读取n字节的内容慢，而且会更快。
        // 如果到达了文件尾部，则会停留在文件尾部，并返回OK Status。
        // 否则，返回错误信息
		virtual Status Skip(uint64_t n) {
            // int fseek(FILE *stream, long offset, int origin);
            // stream:文件指针
            // offset:偏移量，整数表示正向偏移，负数表示负向偏移
            // origin:设定从文件的哪里开始偏移, 可能取值为：SEEK_CUR、 SEEK_END 或 SEEK_SET
            // SEEK_SET： 文件开头
            // SEEK_CUR： 当前位置
            // SEEK_END： 文件结尾
            // 其中SEEK_SET, SEEK_CUR和SEEK_END和依次为0，1和2.
            // 举例：
            // fseek(fp, 100L, 0); 把fp指针移动到离文件开头100字节处；
            // fseek(fp, 100L, 1); 把fp指针移动到离文件当前位置100字节处；
            // fseek(fp, 100L, 2); 把fp指针退回到离文件结尾100字节处。
            // 返回值：成功返回0，失败返回非0
			if (fseek(file_, n, SEEK_CUR)) {
				return IOError(filename_, errno);
			}
			return Status::OK();
  }
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~PosixRandomAccessFile() { close(fd_); }
  
  // 这里与顺序读的同名函数相比，多了一个参数offset，offset用来指定
  // 读取位置距离文件起始位置的偏移量，这样就可以实现随机读了
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const 
  {
    Status s;
	// 在非windows系统上使用pread进行随机读
	// fseek (seek) 定位到访问点，调用 fread (read) 来从特定位置开始访问 FILE* (fd)。
	// 然而，这两个操作组合在一起并不是原子的，即 fseek 和 fread 之间可能会插入其他线程的文件操作。
	// 相比之下 pread 由系统来保证实现原子的定位和读取组合功能。需要注意的是，pread 操作不会更新文件指针。
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) 
	{
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
// 防止Mmap文件过多,造成虚拟内存被跑满,或者防止由于虚拟内存空间使用过多
// 造成内核的性能问题,Mmap最多允许1000个文件
class MmapLimiter 
{
 public:
  // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
  MmapLimiter() 
  {
    SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
  }

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  bool Acquire() 
  {
    if (GetAllowed() <= 0) 
	{
      return false;
    }
	// 加锁保证修改Mmap的文件数量是原子操作
    MutexLock l(&mu_);
    intptr_t x = GetAllowed();
    if (x <= 0) 
	{
      return false;
    } 
	else 
	{
      SetAllowed(x - 1);
      return true;
    }
  }

  // Release a slot acquired by a previous call to Acquire() that returned true.
  void Release() 
  {
    MutexLock l(&mu_);
    SetAllowed(GetAllowed() + 1);
  }

 private:
  port::Mutex mu_;
  port::AtomicPointer allowed_;

  intptr_t GetAllowed() const 
  {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
  }

  // REQUIRES: mu_ must be held
  void SetAllowed(intptr_t v) 
  {
    allowed_.Release_Store(reinterpret_cast<void*>(v));
  }

  MmapLimiter(const MmapLimiter&);
  void operator=(const MmapLimiter&);
};

// mmap() based random-access
// 使用mmap共享内存来提高性能,mmap()函数可以把一个文件或者Posix共享内存对象映射到调用进程的地址空间
// 映射分为两种:
// a、文件映射：文件映射将一个文件的一部分直接映射到调用进程的虚拟内存中。
//    一旦一个文件被映射之后就可以通过在相应的内存区域中操作字节来访问文件内容了。
//    映射的分页会在需要的时候从文件中（自动）加载。这种映射也被称为基于文件的映射或内存映射文件。
// b、匿名映射：一个匿名映射没有对应的文件。相反，这种映射的分页会被初始化为 0。
class PosixMmapReadableFile: public RandomAccessFile 
{
 private:
  std::string filename_;
  // 指向内存映射基地址
  void* mmapped_region_;
  size_t length_;
  MmapLimiter* limiter_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname), mmapped_region_(base), length_(length),
        limiter_(limiter) 
	{
  }

  virtual ~PosixMmapReadableFile() 
  {
    munmap(mmapped_region_, length_);
    limiter_->Release();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const 
  {
    Status s;
    if (offset + n > length_) 
	{
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } 
	else 
	{
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }
};

class PosixWritableFile : public WritableFile 
{
 private:
  std::string filename_;
  FILE* file_;

 public:
  // 构造函数初始化成员变量
  PosixWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  
  // 释放关闭文件句柄
  ~PosixWritableFile() 
  {
    if (file_ != NULL) 
	{
      // Ignoring any potential errors
      fclose(file_);
    }
  }
  
  // 文件中写入数据
  virtual Status Append(const Slice& data) 
  {
	// 写入数据
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
    if (r != data.size()) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  // 关闭写入流
  virtual Status Close() 
  {
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
  }

  // 手动刷新写入流
  virtual Status Flush() 
  {
    if (fflush_unlocked(file_) != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
  
  // 检查文件和目录存在
  Status SyncDirIfManifest() 
  {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) 
	{
      dir = ".";
      basename = f;
    } 
	else 
	{
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) 
	{
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) 
	  {
        s = IOError(dir, errno);
      } 
	  else 
	  {
        if (fsync(fd) < 0) 
		{
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  // 同步文件中的数据
  virtual Status Sync() 
  {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();
    if (!s.ok()) 
	{
      return s;
    }
    if (fflush_unlocked(file_) != 0 ||
        fdatasync(fileno(file_)) != 0) 
	{
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
  }
};

static int LockOrUnlock(int fd, bool lock) 
{
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  // 文件锁类型,读写锁或者解锁
  // l_whence = SEEK_SET,l_strat=0,l_len=0表示加锁整个文件
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  // SEEK_SET文件头
  // SEEK_CUR当前位置
  // SEEK_END文件尾
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  // 1.复制一个现有描述符,cmd=F_DUPFD
  // 2.获取/设置文件描述符,cmd=F_GETFD/SETFD
  // 3.获取/设置文件状态标记,cmd=F_GETFL/SETFL
  // 4.获取/设置异步I/O所有权,cmd=F_GETOWN/SETOWN
  // 5.获取/设置记录锁,cmd=F_GETLK/SETLK/F_SETLKW
  return fcntl(fd, F_SETLK, &f);
}

class PosixFileLock : public FileLock 
{
 public:
  int fd_;
  std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
class PosixLockTable 
{
 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_;
 public:
  bool Insert(const std::string& fname) 
  {
    MutexLock l(&mu_);
    return locked_files_.insert(fname).second;
  }
  void Remove(const std::string& fname) 
  {
    MutexLock l(&mu_);
    locked_files_.erase(fname);
  }
};

class PosixEnv : public Env 
{
	public:
		PosixEnv();
		virtual ~PosixEnv() 
		{
			char msg[] = "Destroying Env::Default()\n";
			fwrite(msg, 1, sizeof(msg), stderr);
			abort();
		}

		virtual Status NewSequentialFile(const std::string& fname,
				SequentialFile** result) 
		{
			DEBUG("Begin Open %s.",fname.c_str());
			FILE* f = fopen(fname.c_str(), "r");
			if (f == NULL) 
			{
				*result = NULL;
				return IOError(fname, errno);
			} 
			else 
			{
				DEBUG("Open %s Success.",fname.c_str());
				*result = new PosixSequentialFile(fname, f);
				return Status::OK();
			}
		}

		virtual Status NewRandomAccessFile(const std::string& fname,
				RandomAccessFile** result) {
			*result = NULL;
			Status s;
			int fd = open(fname.c_str(), O_RDONLY);
			if (fd < 0) 
			{
				s = IOError(fname, errno);
			} 
			else if (mmap_limit_.Acquire()) 
			{
				uint64_t size;
				s = GetFileSize(fname, &size);
				if (s.ok()) 
				{
					void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
					if (base != MAP_FAILED) 
					{
						*result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
					} 
					else 
					{
						s = IOError(fname, errno);
					}
				}
				close(fd);
				if (!s.ok()) {
					mmap_limit_.Release();
				}
			} 
			else 
			{
				*result = new PosixRandomAccessFile(fname, fd);
			}
			return s;
		}

		virtual Status NewWritableFile(const std::string& fname,
				WritableFile** result) {
			Status s;
			FILE* f = fopen(fname.c_str(), "w");
			if (f == NULL) {
				*result = NULL;
				s = IOError(fname, errno);
			} else {
				*result = new PosixWritableFile(fname, f);
			}
			return s;
		}

		virtual Status NewAppendableFile(const std::string& fname,
				WritableFile** result) {
			Status s;
			FILE* f = fopen(fname.c_str(), "a");
			if (f == NULL) {
				*result = NULL;
				s = IOError(fname, errno);
			} else {
				*result = new PosixWritableFile(fname, f);
			}
			return s;
		}

		virtual bool FileExists(const std::string& fname) 
		{
			return access(fname.c_str(), F_OK) == 0;
		}
        
		// 获取dir目录下的所有文件名
		virtual Status GetChildren(const std::string& dir,
				std::vector<std::string>* result) 
		{
			result->clear();
			DIR* d = opendir(dir.c_str());
			if (d == NULL) 
			{
				return IOError(dir, errno);
			}
			struct dirent* entry;
			while ((entry = readdir(d)) != NULL) 
			{
				result->push_back(entry->d_name);
			}
			closedir(d);
			return Status::OK();
		}

		virtual Status DeleteFile(const std::string& fname) {
			Status result;
			if (unlink(fname.c_str()) != 0) {
				result = IOError(fname, errno);
			}
			return result;
		}

		virtual Status CreateDir(const std::string& name) 
		{
			Status result;
			if (mkdir(name.c_str(), 0755) != 0) 
			{
				result = IOError(name, errno);
			}
			DEBUG("create[%s] Success.",name.c_str());
			return result;
		}

		virtual Status DeleteDir(const std::string& name) {
			Status result;
			if (rmdir(name.c_str()) != 0) {
				result = IOError(name, errno);
			}
			return result;
		}

		virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
			Status s;
			struct stat sbuf;
			if (stat(fname.c_str(), &sbuf) != 0) {
				*size = 0;
				s = IOError(fname, errno);
			} else {
				*size = sbuf.st_size;
			}
			return s;
		}

		virtual Status RenameFile(const std::string& src, const std::string& target) {
			Status result;
			if (rename(src.c_str(), target.c_str()) != 0) {
				result = IOError(src, errno);
			}
			return result;
		}

		virtual Status LockFile(const std::string& fname, FileLock** lock) 
		{
			*lock = NULL;
			Status result;
			int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
			if (fd < 0) 
			{
				result = IOError(fname, errno);
			} 
			else if (!locks_.Insert(fname)) 
			{
				close(fd);
				result = Status::IOError("lock " + fname, "already held by process");
			}
			else if (LockOrUnlock(fd, true) == -1) 
			{
				result = IOError("lock " + fname, errno);
				close(fd);
				locks_.Remove(fname);
			} 
			else 
			{
				PosixFileLock* my_lock = new PosixFileLock;
				my_lock->fd_ = fd;
				my_lock->name_ = fname;
				*lock = my_lock;
			}
			DEBUG("Read[%s] Success.",fname.c_str());
			return result;
		}

		virtual Status UnlockFile(FileLock* lock) {
			PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
			Status result;
			if (LockOrUnlock(my_lock->fd_, false) == -1) {
				result = IOError("unlock", errno);
			}
			locks_.Remove(my_lock->name_);
			close(my_lock->fd_);
			delete my_lock;
			return result;
		}

		virtual void Schedule(void (*function)(void*), void* arg);

		virtual void StartThread(void (*function)(void* arg), void* arg);

		virtual Status GetTestDirectory(std::string* result) {
			const char* env = getenv("TEST_TMPDIR");
			if (env && env[0] != '\0') {
				*result = env;
			} else {
				char buf[100];
				snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
				*result = buf;
			}
			// Directory may already exist
			CreateDir(*result);
			return Status::OK();
		}

		static uint64_t gettid() {
			pthread_t tid = pthread_self();
			uint64_t thread_id = 0;
			memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
			return thread_id;
		}

		virtual Status NewLogger(const std::string& fname, Logger** result) 
		{
			FILE* f = fopen(fname.c_str(), "w");
			if (f == NULL) 
			{
				*result = NULL;
				return IOError(fname, errno);
			} 
			else 
			{
				*result = new PosixLogger(f, &PosixEnv::gettid);
				return Status::OK();
			}
		}

		virtual uint64_t NowMicros() {
			struct timeval tv;
			gettimeofday(&tv, NULL);
			return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
		}

		virtual void SleepForMicroseconds(int micros) {
			usleep(micros);
		}

	private:
		void PthreadCall(const char* label, int result) {
			if (result != 0) {
				fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
				abort();
			}
		}

		// BGThread() is the body of the background thread
		void BGThread();
		static void* BGThreadWrapper(void* arg) {
			reinterpret_cast<PosixEnv*>(arg)->BGThread();
			return NULL;
		}

		pthread_mutex_t mu_;
		pthread_cond_t bgsignal_;
		pthread_t bgthread_;
		bool started_bgthread_;

		// Entry per Schedule() call
		struct BGItem { void* arg; void (*function)(void*); };
		typedef std::deque<BGItem> BGQueue;
		BGQueue queue_;

		PosixLockTable locks_;
		MmapLimiter mmap_limit_;
};

PosixEnv::PosixEnv() : started_bgthread_(false) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
	// 发送队列将会有任务的信号，让等待线程准备处理任务，但是没有解锁互斥量，仍会阻塞
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;
  // 解锁互斥量，此时等待线程才能真正苏醒，等待线程收到信号后，难道一直在循环尝试解锁？
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
	// 先锁住互斥量
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
	  // 队列为空则阻塞等待，并释放互斥锁
	  // 收到信号还需解锁，若解锁不成功仍会阻塞
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace leveldb
