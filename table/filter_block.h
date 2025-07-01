// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"

#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::string keys_;           // Flattened key contents //扁平化
  std::vector<size_t> start_;  // Starting index in keys_ of each key
                               // //每个key在keys_中的起始索引
  std::string
      result_;  // Filter data computed so far //到目前为止计算的过滤器数据
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
                                 // //policy_->CreateFilter()的参数
  std::vector<uint32_t>
      filter_offsets_;  // Filter data offsets //过滤器数据偏移量
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;  // Pointer to filter data (at block-start) //过滤器地址
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
                        // //偏移量数组地址
  size_t num_;      // Number of entries in offset array //多少个过滤器
  size_t base_lg_;  // Encoding parameter (see kFilterBaseLg in .cc file) //大小
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
