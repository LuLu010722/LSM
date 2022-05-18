//
// Created by 38466 on 2022/5/10.
//

#ifndef UNTITLED_MEMTABLE_H
#define UNTITLED_MEMTABLE_H

#include <vector>
#include <string>
#include <list>
#include <fstream>
#include <getopt.h>

#include "utils.h"
#include "MurmurHash3.h"

#define MAX_LEVEL 8
#define MAX_MEM (1 << 21)
#define INITIAL_MEM (10240 + 32)
#define BLOOM_FILTER_SIZE 10240

extern uint64_t globalStamp;

using namespace std;

enum Type {
    NORMAL,
    HEAD,
    TAIL
};

class MemTable {

    struct Node {
        uint64_t key = 0;
        string data = "";
        Type type = NORMAL;
        vector<Node *> forwards;

        Node(uint64_t key = 0, const string &data = "", Type type = NORMAL);
    };

    Node *head = nullptr;
    Node *rear = nullptr;

    string root;
    uint64_t stamp = 0;
    uint64_t size = 0;
    uint64_t minkey = UINT64_MAX;
    uint64_t maxkey = 0;

    uint64_t memSize = INITIAL_MEM;

    bool *generateBloomFilter();
    void init();

public:
    MemTable(const string &root);

    ~MemTable();

    bool insert(uint64_t key, const string &data);

    bool search(uint64_t key, string &output);

    void write();

    void scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string>> &output);

};


#endif //UNTITLED_MEMTABLE_H
