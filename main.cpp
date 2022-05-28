#include <iostream>
#include <string>
#include <ctime>
#include <random>

#include "kvstore.h"

using namespace std;

int main() {
    srand(time(nullptr));
    KVStore store("data");
    store.reset();

//    for (int i = 0; i < count; ++i) {
//        store.put(i, string(i + 1, 's'));
//    }
//
//    for (int i = 0; i < count; ++i) {
//        store.get(i);
//    }
//
//    for (int i = 0; i < count; ++i) {
//        store.del(i);
//    }

    const int count = 1024 * 5;
    default_random_engine e;
    uint64_t min_key = 0;
    uint64_t max_key = 2 * count;
    uint64_t min_length = 1;
    uint64_t max_length = 2 * count;

    uniform_int_distribution<int> key_range(min_key, max_key);
    uniform_int_distribution<int> length_range(min_length, max_length);
    e.seed(time(nullptr));

    list<pair<uint64_t, size_t>> pairs;
    list<uint64_t> keys_to_get;

    for (int i = 0; i < count; ++i) {
        pairs.push_back(make_pair(key_range(e), length_range(e)));
        keys_to_get.push_back(key_range(e));
    }

    for (auto pair: pairs) {
        store.put(pair.first, string(pair.second, 's'));
    }

    clock_t start = clock();

    for (auto key: keys_to_get) {
        store.get(key);
    }

    clock_t end = clock();

    cout << (double) (end - start) / CLOCKS_PER_SEC << endl;
    cout << count * CLOCKS_PER_SEC / (end - start) << endl;

    return 0;
}