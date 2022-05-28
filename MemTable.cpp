//
// Created by 38466 on 2022/5/10.
//

#include "MemTable.h"

bool *MemTable::generateBloomFilter() {
    bool *bloomFilter = new bool[BLOOM_FILTER_SIZE]{false};

    Node *iter = head;
    while (iter->forwards[0]->type == NORMAL) {
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&iter->forwards[0]->key, 8, 1, hash);
        bloomFilter[hash[0] % BLOOM_FILTER_SIZE] = 1;
        bloomFilter[hash[1] % BLOOM_FILTER_SIZE] = 1;
        bloomFilter[hash[2] % BLOOM_FILTER_SIZE] = 1;
        bloomFilter[hash[3] % BLOOM_FILTER_SIZE] = 1;
        iter = iter->forwards[0];
    }
    return bloomFilter;
}


void MemTable::init() {
    this->head = new Node(0, "", HEAD);
    this->rear = new Node(UINT64_MAX, "", TAIL);
    this->stamp = globalStamp;
    this->size = 0;
    this->minkey = UINT64_MAX;
    this->maxkey = 0;
    memSize = INITIAL_MEM;

    for (int i = 0; i < MAX_LEVEL; ++i) {
        head->forwards[i] = rear;
    }
}

MemTable::Node::Node(uint64_t key, const string &data, Type type) {
    this->key = key;
    this->data = data;
    this->type = type;
    for (int i = 0; i < MAX_LEVEL; ++i) {
        this->forwards.push_back(nullptr);
    }
}

MemTable::MemTable(const string &root) {
    this->root = root;
    init();
}

MemTable::~MemTable() {

}

/**
 *
 * @param key the key
 * @param data string that needs to be inserted
 * @return ture if there is a write due to exceeding MAX_MEM, false otherwise
 */
bool MemTable::insert(uint64_t key, const string &data) {
    Node *node = head;
    Node *update[MAX_LEVEL];

    for (int i = MAX_LEVEL - 1; i >= 0; i--) {
        while (node->forwards[i]->key < key)
            node = node->forwards[i];
        update[i] = node;
    }

    node = node->forwards[0];
    if (node->key == key && node->type == NORMAL) {

        if (memSize + data.length() - node->data.length() <= MAX_MEM) {
            node->data = data;
            memSize += data.length() - node->data.length();
            return false;
        }

        write();
        init();
        insert(key, data);
        return true;
    }

    if (memSize + data.length() + 1 + 12 <= MAX_MEM) {
        int level = 1;
        while (level < MAX_LEVEL && (double) rand() / RAND_MAX < 0.5)
            ++level;

        auto *newNode = new Node(key, data, NORMAL);
        for (int i = 0; i < level; i++) {
            newNode->forwards[i] = update[i]->forwards[i];
            update[i]->forwards[i] = newNode;
        }

        size++;
        memSize += data.length() + 1 + 12;
        if (key < minkey) minkey = key;
        if (key > maxkey) maxkey = key;
        return false;
    }

    write();
    init();
    insert(key, data);
    return true;
}

/**
 *
 * @param key the key
 * @param output the output string reference, will be set if there is such key
 * @return ture if there is such key, no matter if the value is "~DELETED~" or not, false otherwise
 */
bool MemTable::search(uint64_t key, string &output) {

    Node *node = head;

    for (int i = MAX_LEVEL - 1; i >= 0; i--)
        while (node->forwards[i]->key < key)
            node = node->forwards[i];

    node = node->forwards[0];
    if (node->key == key) {
        output = node->data == "~DELETED~" ? "" : node->data;
        return true;
    }

    return false;
}

void MemTable::write() {
    if (size == 0) return;

    string outPath = root + "/level-0";

    if (!utils::dirExists(outPath))
        utils::mkdir(outPath.c_str());
    stamp = globalStamp++;
    ofstream out(outPath + "/" + to_string(stamp) + "-" + to_string(fileId++) + ".sst", ios::out | ios::binary);
    out.write((char *) (&stamp), 8);
    out.write((char *) (&size), 8);
    out.write((char *) (&minkey), 8);
    out.write((char *) (&maxkey), 8);

    out.write((char *) (generateBloomFilter()), BLOOM_FILTER_SIZE);

    Node *iter = head;
    uint32_t offset = INITIAL_MEM + size * 12;
    while (iter->forwards[0]->type == NORMAL) {
        out.write((char *) (&iter->forwards[0]->key), 8);
        out.write((char *) (&offset), 4);
        offset += iter->forwards[0]->data.length() + 1;
        iter = iter->forwards[0];
    }

    iter = head;
    while (iter->forwards[0]->type == NORMAL) {
        out << iter->forwards[0]->data << '\0';
        iter = iter->forwards[0];
    }

    out.close();
}

void MemTable::scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string>> &output) {
    Node *node = head->forwards[0];

    while (node->key < key1) node = node->forwards[0];

    while (node->key <= key2 && node->type == NORMAL) {
        output.push_back(make_pair(node->key, node->data));
        node = node->forwards[0];
    }

}
