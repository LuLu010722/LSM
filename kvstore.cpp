#include <string>
#include "MurmurHash3.h"
#include "kvstore.h"

uint64_t globalStamp = 0;

KVStore::KVStore(const string &dir) : KVStoreAPI(dir) {
    globalStamp = 0;
    this->dir = dir;
    ssTable = new SSTable(dir);
    memTable = new MemTable(dir);
}

KVStore::~KVStore() {
    memTable->write();
    ssTable->updateLevel0();
    ssTable->compact();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const string &s) {
    if (memTable->insert(key, s)) {
        ssTable->updateLevel0();
        ssTable->compact();
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    string result;

    if (key == 5386) {
        int a = 1;
    }
    if (memTable->search(key, result))
        return result;

    else if (ssTable->search(key, result))
        return result;

    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    if (key == 4546) {
        int a = 1;
    }
    string value = get(key);

    if (value == "" || value == "~DELETED~")
        return false;

    put(key, "~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    ssTable->reset();
    delete memTable;
    delete ssTable;

    globalStamp = 0;
    ssTable = new SSTable(dir);
    memTable = new MemTable(dir);
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string>> &list) {
//    for (uint64_t i = key1; i <= key2; ++i) {
//        string result = get(i);
//        if (result.length()) {
//            list.push_back(make_pair(i, result));
//        }
//    }
    memTable->scan(key1, key2, list);
    ssTable->scan(key1, key2, list);

}
