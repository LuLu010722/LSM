//
// Created by 38466 on 2022/5/10.
//

#include "SSTable.h"

SSTable::Buffer::Buffer(uint32_t stamp, uint32_t size, uint32_t minkey, uint32_t maxkey) {
    this->stamp = stamp;
    this->size = size;
    this->minkey = minkey;
    this->maxkey = maxkey;
}

bool SSTable::Buffer::filter(bool *bloomFilter, uint64_t key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    return bloomFilter[hash[0] % BLOOM_FILTER_SIZE] &&
           bloomFilter[hash[1] % BLOOM_FILTER_SIZE] &&
           bloomFilter[hash[2] % BLOOM_FILTER_SIZE] &&
           bloomFilter[hash[3] % BLOOM_FILTER_SIZE];
}

bool SSTable::Buffer::operator==(const Buffer &another) {
    return this->path == another.path &&
           this->stamp == another.stamp &&
           this->size == another.size &&
           this->minkey == another.minkey &&
           this->maxkey == another.maxkey &&
           this->keys == another.keys &&
           this->offsets == another.offsets;
}

bool SSTable::cmp(Buffer a, Buffer b) {
    return a.stamp < b.stamp || a.minkey < b.minkey;
}


/**
 *
 * @param level the level that need to be initialized. -1 for all the levels
 */
void SSTable::initLevels(int level) {

    if (level == -1) {

        levels.clear();
        vector<string> levelNames;

        int levelNum = utils::scanDir(root, levelNames);

        for (int i = 0; i < levelNum; ++i) {
            vector<string> fileNames;
            int fileNum = utils::scanDir(root + "/" + levelNames[i], fileNames);

            list<Buffer> buffers;
            for (int j = 0; j < fileNum; ++j) {
                Buffer buffer;
                buffer.path = root + "/" + levelNames[i] + "/" + fileNames[j];
                ifstream in(buffer.path, ios::in | ios::binary);

                in.read((char *) (&buffer.stamp), 8);
                in.read((char *) (&buffer.size), 8);
                in.read((char *) (&buffer.minkey), 8);
                in.read((char *) (&buffer.maxkey), 8);

                in.read((char *) (buffer.bloomFilter), BLOOM_FILTER_SIZE);

                for (int k = 0; k < buffer.size; ++k) {
                    uint64_t key;
                    uint32_t offset;

                    in.read((char *) (&key), 8);
                    in.read((char *) (&offset), 4);

                    buffer.keys.push_back(key);
                    buffer.offsets.push_back(offset);
                }
                buffers.push_back(buffer);
                if (buffer.stamp > globalStamp) globalStamp = buffer.stamp;

            }

            // sort logic: small stamp first; same stamp, small minkey first
            buffers.sort(cmp);
            levels.push_back(buffers);
        }
        globalStamp++;
        return;
    }

    vector<string> fileNames;
    int fileNum = utils::scanDir(root + "/level-" + to_string(level), fileNames);

    list<Buffer> buffers;
    for (int i = 0; i < fileNum; ++i) {
        Buffer buffer;
        buffer.path = root + "/level-" + to_string(level) + "/" + fileNames[i];

        ifstream in(buffer.path, ios::in | ios::binary);

        in.read((char *) (&buffer.stamp), 8);
        in.read((char *) (&buffer.size), 8);
        in.read((char *) (&buffer.minkey), 8);
        in.read((char *) (&buffer.maxkey), 8);

        in.read((char *) (&buffer.bloomFilter), BLOOM_FILTER_SIZE);

        for (int k = 0; k < buffer.size; ++k) {
            uint64_t key;
            uint32_t offset;

            in.read((char *) (&key), 8);
            in.read((char *) (&offset), 4);

            buffer.keys.push_back(key);
            buffer.offsets.push_back(offset);
        }

        buffers.push_back(buffer);
    }
    buffers.sort(cmp);

    if (level == levels.size()) levels.push_back(buffers);
    else {
        levels[level].clear();
        levels[level] = buffers;
    }
}

/**
 * a binary search used for sst search
 * @return the index of the found key, -1 for not found
 */
int SSTable::binarySearch(const vector<uint64_t> &keys, uint64_t key, size_t left, size_t right) {
    if (left == right) {
        if (keys[left] == key)
            return left;
        return -1;
    }

    uint64_t middle = (left + right) / 2;
    if (keys[middle] >= key)
        return binarySearch(keys, key, left, middle);
    else
        return binarySearch(keys, key, middle + 1, right);
}


void SSTable::loadData(const string &path, pair<uint64_t, list<pair<uint64_t, string>>> &pairsWithStamp) {
    uint64_t stamp;
    uint64_t size;

    ifstream in(path, ios::in | ios::binary);

    in.read((char *) (&stamp), 8);
    in.read((char *) (&size), 8);
    in.seekg(16 + BLOOM_FILTER_SIZE, ios::cur);

    pairsWithStamp.first = stamp;
    pairsWithStamp.second.clear();

    for (int i = 0; i < size; ++i) {
        uint64_t key;
        in.read((char *) (&key), 8);
        in.seekg(4, ios::cur);
        pairsWithStamp.second.push_back(make_pair(key, ""));
    }
    for (auto iter = pairsWithStamp.second.begin(); iter != pairsWithStamp.second.end(); ++iter) {
        string data;
        getline(in, data, '\0');
        iter->second = data;
    }
}

void SSTable::write(int level, Buffer buffer, const list<pair<uint64_t, string>> &pairs) {

    if (!utils::dirExists(root + "/level-" + to_string(level))) {
        utils::mkdir((root + "/level-" + to_string(level)).c_str());
    }
    ofstream out(buffer.path, ios::out | ios::binary);

    out.write((char *) (&buffer.stamp), 8);
    out.write((char *) (&buffer.size), 8);
    out.write((char *) (&buffer.minkey), 8);
    out.write((char *) (&buffer.maxkey), 8);

    out.write((char *) (buffer.bloomFilter), BLOOM_FILTER_SIZE);

    uint32_t offset = INITIAL_MEM + 12 * buffer.size;
    for (auto iter: pairs) {
        out.write((char *) (&iter.first), 8);
        out.write((char *) (&offset), 4);

        offset += iter.second.length() + 1;
    }
    for (auto iter: pairs) {
        out << iter.second << '\0';
    }
    out.close();
}

void SSTable::generateBloomFilter(bool *bloomFilter, const list<pair<uint64_t, string>> &pairs) {
    for (auto iter: pairs) {
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&iter.first, 8, 1, hash);
        bloomFilter[hash[0] % BLOOM_FILTER_SIZE] = 1;
        bloomFilter[hash[1] % BLOOM_FILTER_SIZE] = 1;
        bloomFilter[hash[2] % BLOOM_FILTER_SIZE] = 1;
        bloomFilter[hash[3] % BLOOM_FILTER_SIZE] = 1;
    }
}


SSTable::SSTable(const string &root) {
    this->root = root;
    initLevels(-1);
}

bool SSTable::search(uint64_t key, string &output) {
    vector<uint64_t> stamps;
    vector<string> values;
    int index;

    for (const auto &level: levels) {
        for (auto buffer: level) {
            if (buffer.minkey > key || buffer.maxkey < key)
                continue;

            if (!buffer.filter(buffer.bloomFilter, key))
                continue;

            if ((index = binarySearch(buffer.keys, key, 0, buffer.size - 1)) == -1)
                continue;

            ifstream in(buffer.path);
            string tmp;
            in.seekg(buffer.offsets[index]);
            getline(in, tmp, '\0');
            stamps.push_back(buffer.stamp);
            values.push_back(tmp);
        }
    }

    if (values.empty())
        return false;

    size_t latestStampIndex = 0;
    size_t largestStamp = 0;
    size_t stampNum = stamps.size();
    for (size_t i = 0; i < stampNum; i++) {
        if (stamps[i] > largestStamp) {
            latestStampIndex = i;
            largestStamp = stamps[i];
        }
    }
    output.assign(values[latestStampIndex]);
    if (output == DELETED) return false;
    return true;
}

void SSTable::compact() {

    for (int i = 0; i < levels.size(); ++i) {
        if (levels[i].size() <= 2 * i + 2) return;

        vector<pair<uint64_t, list<pair<uint64_t, string>>>> allPairsWithStamp;
        pair<uint64_t, list<pair<uint64_t, string>>> pairsWithStamp;
        list<string> filesInvolved;

        int minkeyOfLevel = levels[i].begin()->minkey;
        int maxkeyOfLevel = levels[i].begin()->minkey;

        int loadLimit = i == 0 ? levels[i].size() : levels[i].size() - 2 * i - 2;

        // get all the sstables that needs to be compact on level i
        int j = 0;
        int largestStampUsedForNewFiles = 0;
        for (auto iter = levels[i].begin(); j < loadLimit; ++j) {
            if (iter->stamp > largestStampUsedForNewFiles)
                largestStampUsedForNewFiles = iter->stamp;
            loadData(iter->path, pairsWithStamp);
            allPairsWithStamp.push_back(pairsWithStamp);

            if (iter->minkey < minkeyOfLevel) minkeyOfLevel = iter->minkey;
            if (iter->maxkey > maxkeyOfLevel) maxkeyOfLevel = iter->maxkey;
            filesInvolved.push_back(iter->path);
            iter++;
            levels[i].pop_front();
        }

        // find the interleaving sstables on level i + 1
        int interleavingNumber = 0;
        if (i + 1 < levels.size()) {
            for (auto iter = levels[i + 1].begin(); iter != levels[i + 1].end();) {
                if (iter->maxkey < minkeyOfLevel || iter->minkey > maxkeyOfLevel) {
                    ++iter;
                    continue;
                }

                if (iter->stamp > largestStampUsedForNewFiles)
                    largestStampUsedForNewFiles = iter->stamp;
                loadData(iter->path, pairsWithStamp);
                allPairsWithStamp.push_back(pairsWithStamp);
                filesInvolved.push_back(iter->path);
                iter = levels[i + 1].erase(iter);
                interleavingNumber++;
            }
        }

        list<pair<uint64_t, string>> totalResult;

        merge(allPairsWithStamp, totalResult, i == levels.size() - 1);

        auto iter = totalResult.begin();
        int writeTimes = 0;

        while (iter != totalResult.end()) {
            list<pair<uint64_t, string>> pairs;
            Buffer buffer;
            buffer.stamp = largestStampUsedForNewFiles;
            buffer.path = root + "/level-" + to_string(i + 1) + "/" + to_string(buffer.stamp) + "-" + to_string(fileId++) + ".sst";
            buffer.minkey = iter->first;
            buffer.maxkey = iter->first;
            uint32_t memSize = INITIAL_MEM;

            while (iter != totalResult.end() && memSize + iter->second.length() + 1 + 12 <= MAX_MEM) {
                pairs.push_back(*iter);
                if (iter->first < buffer.minkey) buffer.minkey = iter->first;
                if (iter->first > buffer.maxkey) buffer.maxkey = iter->first;
                buffer.size++;
                memSize += iter->second.length() + 1 + 12;
                iter++;
            }
            generateBloomFilter(buffer.bloomFilter, pairs);
            write(i + 1, buffer, pairs);
            uint32_t offset = INITIAL_MEM + 12 * pairs.size();
            writeTimes++;
            for (auto pair: pairs) {
                buffer.keys.push_back(pair.first);
                buffer.offsets.push_back(offset);
                offset += pair.second.length() + 1;
            }

            if (i + 1 == levels.size()) {
                list<Buffer> buffers;
                levels.push_back(buffers);
            }
            levels[i + 1].push_back(buffer);
            if (iter == totalResult.end()) break;
        }

        for (const string &file: filesInvolved) {
            utils::rmfile(file.c_str());
        }
    }
}

/**
 * used when there is a newly write to level-0 due to exceeding MAX_MEM
 */
void SSTable::updateLevel0() {
    initLevels(0);
}

/**
 *
 * @param allPairsWithStamp vector<pair<uint64_t (stamp), list<pair<uint64_t (key), string (data)>>>>
 * @param output output reference
 */
void SSTable::merge(const vector<pair<uint64_t, list<pair<uint64_t, string>>>> &allPairsWithStamp, list<pair<uint64_t, string>> &output,
                    bool removeMark) {

    list<pair<pair<uint64_t, string>, uint64_t>> pairsWithStamp;

    for (auto iter1: allPairsWithStamp) {
        uint64_t stamp = iter1.first;
        for (auto iter2: iter1.second) {
            pairsWithStamp.push_back(make_pair(iter2, stamp));
        }
    }
    pairsWithStamp.sort();
    for (auto iter = pairsWithStamp.begin(); iter != pairsWithStamp.end(); ++iter) {
        uint64_t lastKey = iter->first.first;
        auto lastIter = iter;
        while (lastKey == iter->first.first && iter != pairsWithStamp.end()) {
            lastKey = iter->first.first;
            iter++;
        }

        auto latestIter = lastIter;
        for (auto innerIter = lastIter; innerIter != iter; ++innerIter) {
            if (innerIter->second > latestIter->second) {
                latestIter = innerIter;
            }
        }

        if (removeMark && latestIter->first.second == DELETED) {
            for (auto innerIter = lastIter; innerIter != iter;) {
                innerIter = pairsWithStamp.erase(innerIter);
            }
        } else {
            for (auto innerIter = lastIter; innerIter != iter;) {
                if (innerIter == latestIter) ++innerIter;
                else {
                    innerIter = pairsWithStamp.erase(innerIter);
                }
            }
        }
        --iter;
    }

    for (auto iter: pairsWithStamp) {
        output.push_back(iter.first);
    }

}

void SSTable::reset() {
    levels.clear();

    vector<string> levelNames;

    int levelNumber = utils::scanDir(root, levelNames);

    for (int i = 0; i < levelNumber; ++i) {
        vector<string> fileNames;

        int fileNumber = utils::scanDir(root + "/" + levelNames[i], fileNames);

        for (int j = 0; j < fileNumber; ++j) {
            utils::rmfile((root + "/" + levelNames[i] + "/" + fileNames[j]).c_str());
        }

        utils::rmdir((root + "/" + levelNames[i]).c_str());
    }
}

void SSTable::scan(uint64_t key1, uint64_t key2, list<pair<uint64_t, string>> &output) {

    // key stamp value
    list<pair<pair<uint64_t, uint64_t>, string>> diskList;
    vector<int> a;

    for (int i = 0; i < levels.size(); ++i) {
        for (auto buffer = levels[i].begin(); buffer != levels[i].end(); ++buffer) {
            if (buffer->maxkey < key1 || buffer->minkey > key2) continue;

            int indexStart;
            int count = 0;
            for (int j = 0; j < buffer->size; ++j) {
                if (buffer->keys[j] < key1) continue;

                indexStart = j;

                while (j < buffer->size && buffer->keys[j] <= key2) {
                    count++;
                    j++;
                }
                break;
            }

            ifstream in(buffer->path);
            in.seekg(buffer->offsets[indexStart]);
            uint64_t stamp = buffer->stamp;

            string line;
            for (int j = 0; j < count; ++j) {
                getline(in, line, '\0');
                diskList.push_back(make_pair(make_pair(buffer->keys[indexStart + j], stamp), line));
            }
        }
    }

    for (auto iter = output.begin(); iter != output.end(); ++iter) {
        diskList.push_back(make_pair(make_pair(iter->first, UINT64_MAX), iter->second));
    }

    diskList.sort();

    for (auto iter = diskList.begin(); iter != diskList.end();) {
        auto lastIter = iter;
        while (iter->first.first == lastIter->first.first) {
            ++iter;
        }
        --iter;
        for (auto innerIter = lastIter; innerIter != iter;) {
            innerIter = diskList.erase(innerIter);
        }
        if (iter->second == DELETED) {
            iter = diskList.erase(iter);
        } else ++iter;
    }

    output.clear();

    for (auto iter = diskList.begin(); iter != diskList.end(); ++iter) {
        output.push_back(make_pair(iter->first.first, iter->second));
    }

}
