#pragma once

#include <vector>
#include "kvstore_api.h"
#include "MemTable.h"
#include "SSTable.h"

using namespace std;


#define MAX_LEVEL 8
#define MAX_MEM (1 << 21)
#define BLOOM_FILTER_SIZE 10240


class KVStore : public KVStoreAPI
{
    string dir;
    MemTable *memTable = nullptr;
    SSTable *ssTable = nullptr;

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;
};
