//
// Created by 38466 on 2022/5/10.
//

#ifndef UNTITLED_SSTABLE_H
#define UNTITLED_SSTABLE_H

#include <cstdint>
#include <vector>
#include <list>
#include <string>
#include <fstream>
#include <algorithm>

#include "utils.h"
#include "MurmurHash3.h"

#define BLOOM_FILTER_SIZE 10240
#define INITIAL_MEM (10240 + 32)
#define MAX_MEM (1 << 21)
#define DELETED "~DELETED~"

extern uint64_t globalStamp;
extern uint64_t fileId;

using namespace std;

class SSTable {
    struct Buffer {
        string path;
        uint64_t stamp = 0;
        uint64_t size = 0;
        uint64_t minkey = UINT64_MAX;
        uint64_t maxkey = 0;

        bool bloomFilter[BLOOM_FILTER_SIZE] = {0};
        vector<uint64_t> keys;
        vector<uint32_t> offsets;

        Buffer() = default;

        Buffer(uint32_t stamp, uint32_t size, uint32_t minkey, uint32_t maxkey);

        static bool filter(bool *bloomFilter, uint64_t key);

        bool operator==(const Buffer &another);
    };

    vector<list<Buffer>> levels;
    string root;

    static bool cmp(Buffer a, Buffer b);

    void initLevels(int level = -1);

    int binarySearch(const vector<uint64_t> &keys, uint64_t key, size_t left, size_t right);

    void loadData(const string &path, pair<uint64_t, list<pair<uint64_t, string>>> &pairsWithStamp);

    void write(int level, Buffer buffer, const list<pair<uint64_t, string>> &pairs);

    void generateBloomFilter(bool bloomFilter[BLOOM_FILTER_SIZE], const list<pair<uint64_t, string>> &pairs);

public:
    SSTable(const string &root);

    bool search(uint64_t key, string &output);

    bool searchWithoutCache(uint64_t key, string &output);

    void compact();

    void updateLevel0();

    void merge(const vector<pair<uint64_t, list<pair<uint64_t, string>>>> &allPairsWithStamp, list<pair<uint64_t, string>> &output, bool removeMark);

    void reset();

    void scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string>> &output);
};


#endif //UNTITLED_SSTABLE_H
