/*************************************************************************
    > File Name: leveldbdemo.cpp
    > Author: Damon
    > Mail: thydamon@gmail.com 
    > Created Time: Sun 30 Jul 2017 09:06:23 AM PDT
 ************************************************************************/

#include <iostream>
#include <assert.h>
#include <leveldb/db.h>
#include <string.h>

int main()
{
	leveldb::DB* db;
	leveldb::Options options;
	options.create_if_missing = true;
	leveldb::Status status = leveldb::DB::Open(options,"/home/lion/tmp/dbdemo", &db);
	assert(status.ok());

	// write key1,value1
	std::string key="key";
    std::string value = "value";

	status = db->Put(leveldb::WriteOptions(), key,value);
	assert(status.ok());
	
	for ( int i = 0; i < 1000000; i++) 
	{
	    char str[100];
	    snprintf(str, sizeof(str), "%d", i);
	    status = db->Put(leveldb::WriteOptions(), str, str);
	}
	
	status = db->Get(leveldb::ReadOptions(), key, &value);
	assert(status.ok());
	std::cout<<value<<std::endl;
	std::string key2 = "key2";
	
	//move the value under key to key2
	status = db->Put(leveldb::WriteOptions(),key2,value);
	assert(status.ok());
	status = db->Delete(leveldb::WriteOptions(), key);
	
	assert(status.ok());
	
	status = db->Get(leveldb::ReadOptions(),key2, &value);
	
	assert(status.ok());
	std::cout<<key2<<"==="<<value<<std::endl;
	
	status = db->Get(leveldb::ReadOptions(),key, &value);
	
	if(!status.ok()) std::cerr<<key<<"  "<<status.ToString()<<std::endl;
	else std::cout<<key<<"==="<<value<<std::endl;
	
	delete db;
	return 0;
}

