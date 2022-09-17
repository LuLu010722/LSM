cmake_minimum_required(VERSION 3.10)
project(untitled)

set(CMAKE_CXX_STANDARD 14)

add_executable(LSM
        main.cpp
        kvstore.h
        kvstore_api.h
        kvstore.cpp
        MurmurHash3.h
        utils.h
        MemTable.cpp
        MemTable.h
        SSTable.cpp
        SSTable.h
        )
add_executable(correctness
        correctness.cc
        MurmurHash3.h
        kvstore_api.h
        kvstore.h
        kvstore.cpp
        SSTable.h
        SSTable.cpp
        utils.h
        MemTable.h
        MemTable.cpp)
add_executable(persistence
        persistence.cc
        MurmurHash3.h
        kvstore_api.h
        kvstore.h
        kvstore.cpp
        SSTable.h
        SSTable.cpp
        utils.h
        MemTable.h
        MemTable.cpp)

