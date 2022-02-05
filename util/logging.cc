// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/logging.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace leveldb {

int writelog(const char *file, int line, const char* fmt, ... )
{               
	va_list ap;
	char tmp[1024];
	memset(tmp,0x00,sizeof(tmp));
    va_start(ap,fmt);   
	vsprintf(tmp,fmt,ap); 
	va_end(ap); 
	printf("%s|%d|%s\n",file,line,tmp); 
}

void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value) 
{
  for (size_t i = 0; i < value.size(); i++) 
  {
    char c = value[i];
	// 根据ASIIC码表,可显示字符最小为32(空格),
	// 最大可显示字符为126(~),该if条件判断字符可显示
    if (c >= ' ' && c <= '~') 
	{
      str->push_back(c);
    } 
	else 
	{
      char buf[10];
	  // 输出不可见字符的16进制,比如可见字符空格16进制为20
	  // 则输出[\x20]
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string EscapeString(const Slice& value) 
{
  std::string r;
  // 处理不可见字符
  AppendEscapedStringTo(&r, value);
  return r;
}

bool ConsumeDecimalNumber(Slice* in, uint64_t* val) 
{
	uint64_t v = 0;
	int digits = 0;
	while (!in->empty()) 
	{
		char c = (*in)[0];
		if (c >= '0' && c <= '9') 
		{
			++digits;
			const int delta = (c - '0');
			static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
			if (v > kMaxUint64/10 ||
					(v == kMaxUint64/10 && delta > kMaxUint64%10)) 
			{
				// Overflow
				return false;
			}
			v = (v * 10) + delta;
			in->remove_prefix(1);
		} 
		else 
		{
			break;
		}
	}
	*val = v;
	return (digits > 0);
}

}  // namespace leveldb
