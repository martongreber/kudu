// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "kudu/cfile/block_pointer.h"
#include "kudu/cfile/index_block.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

namespace fs {
struct IOContext;
}  // namespace fs

namespace cfile {

class BTreeInfoPB;
class CFileReader;
class CFileWriter;
struct WriterOptions;

class IndexTreeBuilder {
 public:
  explicit IndexTreeBuilder(
    const WriterOptions* options,
    CFileWriter* writer);

  // Append the given key into the index.
  // The key is copied into the builder's internal memory.
  Status Append(const Slice& key, const BlockPointer& block_ptr);
  Status Finish(BTreeInfoPB* info);

 private:
  static IndexBlockBuilder* CreateBlockBuilder(bool is_leaf);
  Status Append(const Slice& key, const BlockPointer& block_ptr, size_t level);

  // Finish the current block at the given index level, and then
  // propagate by inserting this block into the next higher-up
  // level index.
  Status FinishBlockAndPropagate(size_t level);

  // Finish the current block at the given level, writing it
  // to the file. Return the location of the written block
  // in 'written'.
  Status FinishAndWriteBlock(size_t level, BlockPointer* written);

  const WriterOptions* const options_;
  CFileWriter* writer_;

  std::vector<std::unique_ptr<IndexBlockBuilder>> idx_blocks_;

  DISALLOW_COPY_AND_ASSIGN(IndexTreeBuilder);
};

class IndexTreeIterator {
 public:
  IndexTreeIterator(
      const fs::IOContext* io_context,
      const CFileReader* reader,
      const BlockPointer& root_blockptr);
  ~IndexTreeIterator();

  Status SeekToFirst();
  Status SeekAtOrBefore(const Slice& search_key);
  bool HasNext() const;
  Status Next();

  // The slice key at which the iterator
  // is currently seeked to.
  const Slice& GetCurrentKey() const;
  const BlockPointer& GetCurrentBlockPointer() const;

  static IndexTreeIterator* Create(
    const fs::IOContext* io_context,
    const CFileReader* reader,
    const BlockPointer& root_blockptr);

  const CFileReader* cfile_reader() const {
    return reader_;
  }

 private:
  IndexBlockIterator* BottomIter();
  const IndexBlockReader* BottomReader() const;
  IndexBlockIterator* seeked_iter(size_t depth);
  const IndexBlockReader* seeked_reader(size_t depth) const;
  Status LoadBlock(const BlockPointer& block, size_t depth);
  Status SeekDownward(const Slice& search_key,
                      const BlockPointer& in_block,
                      size_t cur_depth);
  Status SeekToFirstDownward(const BlockPointer& in_block, size_t cur_depth);

  struct SeekedIndex;

  const CFileReader* reader_;

  const BlockPointer root_block_;

  std::vector<std::unique_ptr<SeekedIndex>> seeked_indexes_;

  const fs::IOContext* io_context_;

  DISALLOW_COPY_AND_ASSIGN(IndexTreeIterator);
};

} // namespace cfile
} // namespace kudu
