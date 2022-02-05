/*************************************************************************
    > File Name: test-main.cpp
    > Author: Damon
    > Mail: thydamon@gmail.com 
    > Created Time: Mon 04 Jan 2016 08:10:22 AM PST
 ************************************************************************/

#include <assert.h>
#include <string>
#include <leveldb/db.h>
#include <iostream>

int main()
{
	leveldb::DB* db;
	leveldb::Options options;
	options.create_if_missing = true;
	leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
	assert(status.ok());	

	const int WRITE_TIMES = 10000;
	int i = 0;
	std::string key = "key";
	std::string value = "value";
	status = db->Put(leveldb::WriteOptions(), key, value);
	assert(status.ok());

	status = db->Get(leveldb::ReadOptions(), key, &value);
	assert(status.ok());
	std::cout << value << std::endl;

	delete db;

	return 0;
}

