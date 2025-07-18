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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/concurrent_btree.h"
#include "kudu/util/barrier.h"
#include "kudu/util/debug/sanitizer_scopes.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/memory/memory.h"
#include "kudu/util/memory/overwrite.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DEFINE_int32(concurrent_rw_benchmark_num_writer_threads, 4,
             "Number of writer threads in TestConcurrentReadWritePerformance");
DEFINE_int32(concurrent_rw_benchmark_num_reader_threads, 4,
             "Number of reader threads in TestConcurrentReadWritePerformance");
DEFINE_int32(concurrent_rw_benchmark_num_inserts, 1000000,
             "Number of inserts in TestConcurrentReadWritePerformance");
// This might be needed, because reads are significantly faster than writes.
DEFINE_int32(concurrent_rw_benchmark_reader_boost, 1,
            "Multiply the amount of values each reader thread reads in "
            "TestConcurrentReadWritePerformance");

namespace kudu {
namespace tablet {
namespace btree {

// In unit tests, we explicitly enforce 8-byte alignment for LeafNode/InternalNode
// instances created on the stack. This is critical because:
// 1. ARM64 architectures require 8-byte alignment for atomic operations on uint64_t.
//    Unaligned access triggers SIGBUS.
// 2. Production code guarantees alignment through:
//    a) PACKED in NodeBase definition
//    b) Arena allocator's alignment-aware allocations
// 3. Stack-allocated test nodes lack these guarantees, making explicit alignment
//    necessary to match production behavior and prevent architecture-specific crashes.
// 4. This ensures test validity across x86 (tolerant to unaligned access) and ARM
//    (strict alignment requirements) platforms.
class TestCBTree : public KuduTest {
 protected:
  template<class T>
  InsertStatus InsertInLeaf(LeafNode<T> *l, ThreadSafeArena *arena,
                                   const Slice &k, const Slice &v) {
    PreparedMutation<T> pm(k);
    pm.arena_ = arena;

    // Must lock the node even in the single threaded test
    // to avoid firing the debug assertions.
    l->Lock();
    l->SetInserting();
    l->PrepareMutation(&pm);
    InsertStatus ret = l->Insert(&pm, v);
    l->Unlock();
    return ret;
  }

  void DoBigKVTest(size_t key_size, size_t val_size) {
    ThreadSafeArena arena(1024);

    char kbuf[key_size];
    char vbuf[val_size];
    OverwriteWithPattern(kbuf, key_size, "KEY");
    OverwriteWithPattern(vbuf, key_size, "VAL");
    Slice key(kbuf, key_size);
    Slice val(vbuf, val_size);

    alignas(sizeof(AtomicVersion)) LeafNode<BTreeTraits> lnode(false);
    ASSERT_EQ(INSERT_SUCCESS,
              InsertInLeaf(&lnode, &arena, key, val));
  }

  template<class Traits>
  void DoTestConcurrentInsert();

};

// Ensure that the template magic to make the nodes sized
// as we expect is working.
// The nodes may come in slightly smaller than the requested size,
// but should not be any larger.
TEST_F(TestCBTree, TestNodeSizes) {
  ThreadSafeArena arena(1024);

  alignas(sizeof(AtomicVersion)) LeafNode<BTreeTraits> lnode(false);
  ASSERT_LE(sizeof(lnode), BTreeTraits::kLeafNodeSize);

  alignas(sizeof(AtomicVersion)) InternalNode<BTreeTraits> inode(Slice("split"), &lnode,
                                             &lnode, &arena);
  ASSERT_LE(sizeof(inode), BTreeTraits::kInternalNodeSize);

}

TEST_F(TestCBTree, TestLeafNode) {
  alignas(sizeof(AtomicVersion)) LeafNode<BTreeTraits> lnode(false);
  ThreadSafeArena arena(1024);

  Slice k1("key1");
  Slice v1("val1");
  ASSERT_EQ(INSERT_SUCCESS,
            InsertInLeaf(&lnode, &arena, k1, v1));
  ASSERT_EQ(INSERT_DUPLICATE,
            InsertInLeaf(&lnode, &arena, k1, v1));

  // Insert another entry after first
  Slice k2("key2");
  Slice v2("val2");
  ASSERT_EQ(INSERT_SUCCESS, InsertInLeaf(&lnode, &arena, k2, v2));
  ASSERT_EQ(INSERT_DUPLICATE, InsertInLeaf(&lnode, &arena, k2, v2));

  // Another entry before first
  Slice k0("key0");
  Slice v0("val0");
  ASSERT_EQ(INSERT_SUCCESS, InsertInLeaf(&lnode, &arena, k0, v0));
  ASSERT_EQ(INSERT_DUPLICATE, InsertInLeaf(&lnode, &arena, k0, v0));

  // Another entry in the middle
  Slice k15("key1.5");
  Slice v15("val1.5");
  ASSERT_EQ(INSERT_SUCCESS, InsertInLeaf(&lnode, &arena, k15, v15));
  ASSERT_EQ(INSERT_DUPLICATE, InsertInLeaf(&lnode, &arena, k15, v15));
  ASSERT_EQ("[key0=val0], [key1=val1], [key1.5=val1.5], [key2=val2]",
            lnode.ToString());

  // Add entries until it is full
  int i;
  bool full = false;
  for (i = 0; i < 1000 && !full; i++) {
    char buf[64];
    snprintf(buf, sizeof(buf), "filler_key_%d", i);
    switch (InsertInLeaf(&lnode, &arena, Slice(buf), Slice("data"))) {
      case INSERT_SUCCESS:
        continue;
      case INSERT_DUPLICATE:
        FAIL() << "Unexpected INSERT_DUPLICATE for " << buf;
        break;
      case INSERT_FULL:
        full = true;
        break;
      default:
        FAIL() << "unexpected result";
    }
  }
  ASSERT_LT(i, 1000) << "should have filled up node before 1000 entries";
}

// Directly test leaf node with keys and values which are large (such that
// only zero or one would fit in the actual allocated space)
TEST_F(TestCBTree, TestLeafNodeBigKVs) {
  alignas(sizeof(AtomicVersion)) LeafNode<BTreeTraits> lnode(false);

  DoBigKVTest(1000, 1000);
}

// Setup the tree to fanout quicker, so we test internal node
// splitting, etc.
struct SmallFanoutTraits : public BTreeTraits {

  static const size_t kInternalNodeSize = 84;
  static const size_t kLeafNodeSize = 92;
};

// Enables yield() calls at interesting points of the btree
// implementation to ensure that we are still correct even
// with adversarial scheduling.
struct RacyTraits : public SmallFanoutTraits {
  static constexpr const size_t kDebugRaciness = 100;
};

void MakeKey(char *kbuf, size_t len, int i) {
  snprintf(kbuf, len, "key_%d%d", i % 10, i / 10);
}

template<class T>
void VerifyEntry(CBTree<T> *tree, int i) {
  char kbuf[64];
  char vbuf[64];
  char vbuf_out[64];

  MakeKey(kbuf, sizeof(kbuf), i);
  snprintf(vbuf, sizeof(vbuf), "val_%d", i);

  size_t len = sizeof(vbuf_out);
  ASSERT_EQ(CBTree<T>::GET_SUCCESS,
            tree->GetCopy(Slice(kbuf), vbuf_out, &len))
    << "Failed to verify entry " << kbuf;
  ASSERT_EQ(string(vbuf, len), string(vbuf_out, len));
}


template<class T>
void InsertRange(CBTree<T> *tree,
                 int start_idx,
                 int end_idx) {
  char kbuf[64];
  char vbuf[64];
  for (int i = start_idx; i < end_idx; i++) {
    MakeKey(kbuf, sizeof(kbuf), i);
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);
    if (!tree->Insert(Slice(kbuf), Slice(vbuf))) {
      FAIL() << "Failed insert at iteration " << i;
    }

    /*
    int to_verify = start_idx + (rand() % (i - start_idx + 1));
    CHECK_LE(to_verify, i);
    VerifyEntry(tree, to_verify);
    */
  }
}

template<class T>
void VerifyGet(const CBTree<T> &tree,
               Slice key,
               Slice expected_val) {
  char vbuf[64];
  size_t len = sizeof(vbuf);
  ASSERT_EQ(CBTree<T>::GET_SUCCESS,
            tree.GetCopy(key, vbuf, &len))
    << "Failed on key " << HexDump(key);

  Slice got_val(vbuf, len);
  ASSERT_EQ(expected_val, got_val)
    << "Failure!\n"
    << "Expected: " << HexDump(expected_val)
    << "Got:      " << HexDump(got_val);
}

template<class T>
void VerifyRange(const CBTree<T> &tree,
                 int start_idx,
                 int end_idx) {
  char kbuf[64];
  char vbuf[64];
  for (int i = start_idx; i < end_idx; i++) {
    MakeKey(kbuf, sizeof(kbuf), i);
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);

    VerifyGet(tree, Slice(kbuf), Slice(vbuf));
  }
}


// Function which inserts a range of keys formatted key_<N>
// into the given tree, then verifies that they are all
// inserted properly
template<class T>
void InsertAndVerify(Barrier *go_barrier,
                     Barrier *done_barrier,
                     unique_ptr<CBTree<T>> *tree,
                     int start_idx,
                     int end_idx) {
  while (true) {
    go_barrier->Wait();

    if (tree->get() == nullptr) return;

    InsertRange(tree->get(), start_idx, end_idx);
    VerifyRange(*tree->get(), start_idx, end_idx);

    done_barrier->Wait();
  }
}


TEST_F(TestCBTree, TestInsertAndVerify) {
  CBTree<SmallFanoutTraits> t;
  char kbuf[64];
  char vbuf[64];

  int n_keys = 10000;

  for (int i = 0; i < n_keys; i++) {
    snprintf(kbuf, sizeof(kbuf), "key_%d", i);
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);
    if (!t.Insert(Slice(kbuf), Slice(vbuf))) {
      FAIL() << "Failed insert at iteration " << i;
    }
  }


  for (int i = 0; i < n_keys; i++) {
    snprintf(kbuf, sizeof(kbuf), "key_%d", i);

    // Try to insert with a different value, to ensure that on failure
    // it doesn't accidentally replace the old value anyway.
    snprintf(vbuf, sizeof(vbuf), "xxx_%d", i);
    if (t.Insert(Slice(kbuf), Slice(vbuf))) {
      FAIL() << "Allowed duplicate insert at iteration " << i;
    }

    // Do a Get() and check that the real value is still accessible.
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);
    VerifyGet(t, Slice(kbuf), Slice(vbuf));
  }
}

template<class TREE, class COLLECTION>
static void InsertRandomKeys(TREE *t, int n_keys,
                             COLLECTION *inserted) {
  char kbuf[64];
  char vbuf[64];
  int i = 0;
  while (inserted->size() < n_keys) {
    int key = rand();
    memcpy(kbuf, &key, sizeof(key));
    snprintf(vbuf, sizeof(vbuf), "val_%d", i);
    t->Insert(Slice(kbuf, sizeof(key)), Slice(vbuf));
    inserted->insert(key);
    i++;
  }
}

// Similar to above, but inserts in random order
TEST_F(TestCBTree, TestInsertAndVerifyRandom) {
  CBTree<SmallFanoutTraits> t;
  char kbuf[64];
  char vbuf_out[64];

  int n_keys = 1000;
  if (AllowSlowTests()) {
    n_keys = 100000;
  }

  unordered_set<int> inserted(n_keys);

  InsertRandomKeys(&t, n_keys, &inserted);


  for (int key : inserted) {
    memcpy(kbuf, &key, sizeof(key));

    // Do a Get() and check that the real value is still accessible.
    size_t len = sizeof(vbuf_out);
    ASSERT_EQ(CBTree<SmallFanoutTraits>::GET_SUCCESS,
              t.GetCopy(Slice(kbuf, sizeof(key)), vbuf_out, &len));
  }
}

// Thread which cycles through doing the following:
// - lock the node
// - either mark it splitting or inserting (alternatingly)
// - unlock it
void LockCycleThread(AtomicVersion *v, int count_split, int count_insert) {
  debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
  int i = 0;
  while (count_split > 0 || count_insert > 0) {
    i++;
    VersionField::Lock(v);
    if (i % 2 && count_split > 0) {
      VersionField::SetSplitting(v);
      count_split--;
    } else {
      VersionField::SetInserting(v);
      count_insert--;
    }
    VersionField::Unlock(v);
  }
}

// Single-threaded test case which verifies the correct behavior of
// VersionField.
TEST_F(TestCBTree, TestVersionLockSimple) {
  AtomicVersion v = 0;
  VersionField::Lock(&v);
  ASSERT_EQ(1L << 63, v);
  VersionField::Unlock(&v);
  ASSERT_EQ(0, v);

  VersionField::Lock(&v);
  VersionField::SetSplitting(&v);
  VersionField::Unlock(&v);

  ASSERT_EQ(0, VersionField::GetVInsert(v));
  ASSERT_EQ(1, VersionField::GetVSplit(v));

  VersionField::Lock(&v);
  VersionField::SetInserting(&v);
  VersionField::Unlock(&v);
  ASSERT_EQ(1, VersionField::GetVInsert(v));
  ASSERT_EQ(1, VersionField::GetVSplit(v));

}

// Multi-threaded test case which spawns several threads, each of which
// locks and unlocks a version field a predetermined number of times.
// Verifies that the counters are correct at the end.
TEST_F(TestCBTree, TestVersionLockConcurrent) {
  vector<thread> threads;
  int num_threads = 4;
  int split_per_thread = 2348;
  int insert_per_thread = 8327;

  AtomicVersion v = 0;

  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(LockCycleThread, &v, split_per_thread, insert_per_thread);
  }

  for (thread& thr : threads) {
    thr.join();
  }


  ASSERT_EQ(split_per_thread * num_threads,
            VersionField::GetVSplit(v));
  ASSERT_EQ(insert_per_thread * num_threads,
            VersionField::GetVInsert(v));
}

// Test that the tree holds up properly under a concurrent insert workload.
// Each thread inserts a number of elements and then verifies that it can
// read them back.
TEST_F(TestCBTree, TestConcurrentInsert) {
  DoTestConcurrentInsert<SmallFanoutTraits>();
}

// Same, but with a tree that tries to provoke race conditions.
TEST_F(TestCBTree, TestRacyConcurrentInsert) {
  DoTestConcurrentInsert<RacyTraits>();
}

template<class TraitsClass>
void TestCBTree::DoTestConcurrentInsert() {
  unique_ptr<CBTree<TraitsClass>> tree;

  int num_threads = 16;
  int ins_per_thread = 30;
#ifdef NDEBUG
  int n_trials = 600;
#else
  int n_trials = 30;
#endif

  vector<thread> threads;
  Barrier go_barrier(num_threads + 1);
  Barrier done_barrier(num_threads + 1);


  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(InsertAndVerify<TraitsClass>,
                         &go_barrier,
                         &done_barrier,
                         &tree,
                         ins_per_thread * i,
                         ins_per_thread * (i + 1));
  }


  // Rather than running one long trial, better to run
  // a bunch of short trials, so that the threads contend a lot
  // more on a smaller tree. As the tree gets larger, contention
  // on areas of the key space diminishes.

  for (int trial = 0; trial < n_trials; trial++) {
    tree.reset(new CBTree<TraitsClass>());
    go_barrier.Wait();

    done_barrier.Wait();

    if (::testing::Test::HasFatalFailure()) {
      tree->DebugPrint();
      break;
    }
  }

  tree.reset(nullptr);
  go_barrier.Wait();

  for (thread& thr : threads) {
    thr.join();
  }
}

TEST_F(TestCBTree, TestIterator) {
  CBTree<SmallFanoutTraits> t;

  int n_keys = 100000;
  unordered_set<int> inserted(n_keys);
  InsertRandomKeys(&t, n_keys, &inserted);

  // now iterate through, making sure we saw all
  // the keys that were inserted
  LOG_TIMING(INFO, "Iterating") {
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(
      t.NewIterator());
    bool exact;
    ASSERT_TRUE(iter->SeekAtOrAfter(Slice(""), &exact));
    int count = 0;
    while (iter->IsValid()) {
      Slice k, v;
      iter->GetCurrentEntry(&k, &v);

      int k_int;
      CHECK_EQ(sizeof(k_int), k.size());
      memcpy(&k_int, k.data(), k.size());

      bool removed = inserted.erase(k_int);
      if (!removed) {
        FAIL() << "Iterator saw entry " << k_int << " but not inserted";
      }
      count++;
      iter->Next();
    }

    ASSERT_EQ(n_keys, count);
    ASSERT_EQ(0, inserted.size()) << "Some entries were not seen by iterator";
  }
}

// Test the limited "Rewind" functionality within a given leaf node.
TEST_F(TestCBTree, TestIteratorRewind) {
  CBTree<SmallFanoutTraits> t;

  ASSERT_TRUE(t.Insert(Slice("key1"), Slice("val")));
  ASSERT_TRUE(t.Insert(Slice("key2"), Slice("val")));
  ASSERT_TRUE(t.Insert(Slice("key3"), Slice("val")));

  unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(t.NewIterator());
  bool exact;
  ASSERT_TRUE(iter->SeekAtOrAfter(Slice(""), &exact));

  Slice k, v;
  iter->GetCurrentEntry(&k, &v);
  ASSERT_EQ("key1", k.ToString());
  ASSERT_EQ(0, iter->index_in_leaf());
  ASSERT_EQ(3, iter->remaining_in_leaf());
  ASSERT_TRUE(iter->Next());

  iter->GetCurrentEntry(&k, &v);
  ASSERT_EQ("key2", k.ToString());
  ASSERT_EQ(1, iter->index_in_leaf());
  ASSERT_EQ(2, iter->remaining_in_leaf());
  ASSERT_TRUE(iter->Next());

  iter->GetCurrentEntry(&k, &v);
  ASSERT_EQ("key3", k.ToString());
  ASSERT_EQ(2, iter->index_in_leaf());
  ASSERT_EQ(1, iter->remaining_in_leaf());

  // Rewind to beginning of leaf.
  iter->RewindToIndexInLeaf(0);
  iter->GetCurrentEntry(&k, &v);
  ASSERT_EQ("key1", k.ToString());
  ASSERT_EQ(0, iter->index_in_leaf());
  ASSERT_EQ(3, iter->remaining_in_leaf());
  ASSERT_TRUE(iter->Next());

  iter->GetCurrentEntry(&k, &v);
  ASSERT_EQ("key2", k.ToString());
  ASSERT_EQ(1, iter->index_in_leaf());
  ASSERT_EQ(2, iter->remaining_in_leaf());
  ASSERT_TRUE(iter->Next());
}

TEST_F(TestCBTree, TestIteratorSeekOnEmptyTree) {
  CBTree<SmallFanoutTraits> t;

  unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(
    t.NewIterator());
  bool exact = true;
  ASSERT_FALSE(iter->SeekAtOrAfter(Slice(""), &exact));
  ASSERT_FALSE(exact);
  ASSERT_FALSE(iter->IsValid());
}

// Test seeking to exactly the first and last key, as well
// as the boundary conditions (before first and after last)
TEST_F(TestCBTree, TestIteratorSeekConditions) {
  CBTree<SmallFanoutTraits> t;

  ASSERT_TRUE(t.Insert(Slice("key1"), Slice("val")));
  ASSERT_TRUE(t.Insert(Slice("key2"), Slice("val")));
  ASSERT_TRUE(t.Insert(Slice("key3"), Slice("val")));

  // Seek to before first key should successfully reach first key
  {
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(
      t.NewIterator());

    bool exact;
    ASSERT_TRUE(iter->SeekAtOrAfter(Slice("key0"), &exact));
    ASSERT_FALSE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ("key1", k.ToString());
  }

  // Seek to exactly first key should successfully reach first key
  // and set exact = true
  {
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(
      t.NewIterator());

    bool exact;
    ASSERT_TRUE(iter->SeekAtOrAfter(Slice("key1"), &exact));
    ASSERT_TRUE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ("key1", k.ToString());
  }

  // Seek to exactly last key should successfully reach last key
  // and set exact = true
  {
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(
      t.NewIterator());

    bool exact;
    ASSERT_TRUE(iter->SeekAtOrAfter(Slice("key3"), &exact));
    ASSERT_TRUE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ("key3", k.ToString());
    ASSERT_FALSE(iter->Next());
  }

  // Seek to after last key should fail.
  {
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(
      t.NewIterator());

    bool exact;
    ASSERT_FALSE(iter->SeekAtOrAfter(Slice("key4"), &exact));
    ASSERT_FALSE(exact);
    ASSERT_FALSE(iter->IsValid());
  }
}

// Thread which scans through the entirety of the tree verifying
// that results are returned in-order. The scan is performed in a loop
// until tree->get() == NULL.
//   go_barrier: waits on this barrier to start running
//   done_barrier: waits on this barrier once finished.
template<class T>
static void ScanThread(Barrier *go_barrier,
                       Barrier *done_barrier,
                       unique_ptr<CBTree<T>> *tree) {
  while (true) {
    go_barrier->Wait();
    if (tree->get() == nullptr) return;

    int prev_count = 0;
    int count = 0;
    do {
      prev_count = count;
      count = 0;

      faststring prev_key;

      unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter((*tree)->NewIterator());
      bool exact;
      iter->SeekAtOrAfter(Slice(""), &exact);
      while (iter->IsValid()) {
        count++;
        Slice k, v;
        iter->GetCurrentEntry(&k, &v);

        if (k <= Slice(prev_key)) {
          FAIL() << "prev key " << Slice(prev_key).ToString() <<
            " wasn't less than cur key " << k.ToString();
        }
        prev_key.assign_copy(k.data(), k.size());

        iter->Next();
      }
      ASSERT_GE(count, prev_count);
    } while (count != prev_count || count == 0);

    done_barrier->Wait();
  }
}

// Thread which starts a number of threads to insert data while
// other threads repeatedly scan and verify that the results come back
// in order.
TEST_F(TestCBTree, TestConcurrentIterateAndInsert) {
  unique_ptr<CBTree<SmallFanoutTraits>> tree;

  int num_ins_threads = 4;
  int num_scan_threads = 4;
  int num_threads = num_ins_threads + num_scan_threads;
  int ins_per_thread = 1000;
  int trials = 2;

  if (AllowSlowTests()) {
    ins_per_thread = 30000;
  }

  vector<thread> threads;
  Barrier go_barrier(num_threads + 1);
  Barrier done_barrier(num_threads + 1);

  for (int i = 0; i < num_ins_threads; i++) {
    threads.emplace_back(InsertAndVerify<SmallFanoutTraits>,
                         &go_barrier,
                         &done_barrier,
                         &tree,
                         ins_per_thread * i,
                         ins_per_thread * (i + 1));
  }
  for (int i = 0; i < num_scan_threads; i++) {
    threads.emplace_back(ScanThread<SmallFanoutTraits>,
                         &go_barrier,
                         &done_barrier,
                         &tree);
  }


  // Rather than running one long trial, better to run
  // a bunch of short trials, so that the threads contend a lot
  // more on a smaller tree. As the tree gets larger, contention
  // on areas of the key space diminishes.
  for (int trial = 0; trial < trials; trial++) {
    tree.reset(new CBTree<SmallFanoutTraits>());
    go_barrier.Wait();

    done_barrier.Wait();

    if (::testing::Test::HasFatalFailure()) {
      tree->DebugPrint();
      break;
    }
  }

  tree.reset(nullptr);
  go_barrier.Wait();

  for (thread& thr : threads) {
    thr.join();
  }
}

template<bool VAL_ONLY>
void DoScan(CBTree<BTreeTraits>* tree, int64_t* count, int64_t* total_len) {
  unique_ptr<CBTreeIterator<BTreeTraits>> iter(
      tree->NewIterator());
  bool exact;
  iter->SeekAtOrAfter(Slice(""), &exact);
  while (iter->IsValid()) {
    (*count)++;
    Slice v;
    if (VAL_ONLY) {
      v = iter->GetCurrentValue();
    } else {
      Slice dummy;
      iter->GetCurrentEntry(&dummy, &v);
    }

    *total_len += v.size();
    iter->Next();
  }
}

// Check the performance of scanning through a large tree.
TEST_F(TestCBTree, TestScanPerformance) {
  CBTree<BTreeTraits> tree;
#ifndef NDEBUG
  int n_keys = 10000;
#else
  int n_keys = 1000000;
#endif
  if (AllowSlowTests()) {
    n_keys = 4000000;
  }
  LOG_TIMING(INFO, StringPrintf("Insert %d keys", n_keys)) {
    InsertRange(&tree, 0, n_keys);
  }

  for (int freeze = 0; freeze <= 1; freeze++) {
    if (freeze) {
      tree.Freeze();
    }
    int scan_trials = 10;
    LOG_TIMING(INFO, StringPrintf("Scan %d keys %d times (%s)",
                                  n_keys, scan_trials,
                                  freeze ? "frozen" : "not frozen")) {
      for (int i = 0; i < 10; i++)  {
        int64_t count = 0;
        int64_t total_len = 0;
        DoScan<false>(&tree, &count, &total_len);
        ASSERT_EQ(count, n_keys);
        ASSERT_GT(total_len, 0);
      }
    }

    LOG_TIMING(INFO, StringPrintf("Scan %d keys %d times (%s, val-only)",
                                  n_keys, scan_trials,
                                  freeze ? "frozen" : "not frozen")) {
      for (int i = 0; i < 10; i++)  {
        int64_t count = 0;
        int64_t total_len = 0;
        DoScan<true>(&tree, &count, &total_len);
        ASSERT_EQ(count, n_keys);
        ASSERT_GT(total_len, 0);
      }
    }
  }
}

// Test the seek AT_OR_BEFORE mode.
TEST_F(TestCBTree, TestIteratorSeekAtOrBefore) {
  int key_num = 1000;
  CBTree<SmallFanoutTraits> t;

  // Insert entry in CBTree with key1000 key1002 key1004 ... key2000.
  // Key1001 key1003 key1005 ... key1999 are omitted.
  for (int i = 500; i <= key_num; ++i) {
    string key = Substitute("key$0", i * 2);
    ASSERT_TRUE(t.Insert(Slice(key), Slice("val")));
  }

  // Seek to existing key should successfully reach key
  // and set exact = true;
  for (int i = 500; i <= key_num; ++i) {
    string key = Substitute("key$0", i * 2);
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(t.NewIterator());
    bool exact;
    ASSERT_TRUE(iter->SeekAtOrBefore(Slice(key), &exact));
    ASSERT_TRUE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ(key, k.ToString());
  }

  // Seek to before first key should fail.
  {
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(t.NewIterator());

    bool exact;
    ASSERT_FALSE(iter->SeekAtOrBefore(Slice("key0000"), &exact));
    ASSERT_FALSE(exact);
    ASSERT_FALSE(iter->IsValid());
  }

  // Seek to non-existent key in current CBTree key range should
  // successfully reach the first entry with key < given key
  // and set exact = false.
  for (int i = 500; i <= key_num - 1; ++i) {
    string key = Substitute("key$0", i * 2 + 1);
    string expect_key = Substitute("key$0", i * 2);
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(t.NewIterator());
    bool exact;
    ASSERT_TRUE(iter->SeekAtOrBefore(Slice(key), &exact));
    ASSERT_FALSE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ(expect_key, k.ToString());
  }

  // Seek to after last key should successfully reach last key
  // and set exact = false;
  {
    unique_ptr<CBTreeIterator<SmallFanoutTraits>> iter(t.NewIterator());

    bool exact;
    ASSERT_TRUE(iter->SeekAtOrBefore(Slice("key2001"), &exact));
    ASSERT_FALSE(exact);

    ASSERT_TRUE(iter->IsValid());
    Slice k, v;
    iter->GetCurrentEntry(&k, &v);
    ASSERT_EQ("key2000", k.ToString());
  }
}

// All applications of CBTree use a threadsafe arena with default node sizes.
struct ProdTreeTraits : public btree::BTreeTraits {
  typedef ThreadSafeMemoryTrackingArena ArenaType;
};

// We benchmark two scenarios:
// 1. Writing on multiple threads.
// 2. Reading on multiple threads while there are also active writes.
//
// If read threads wait for values to be inserted, it defeats the purpose of benchmarking.
// Therefore, we should first populate a tree with values for the read threads. The read threads
// will then read values that are already in the tree, while the write threads continue to insert
// new values.
//
// Setting up the tree for the second scenario essentially involves performing the first scenario.
// This is why both scenarios are combined into a single test.
TEST_F(TestCBTree, ConcurrentReadWriteBenchmark) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  constexpr int kTrials = 10;
  // Short names to make some formulas readable
  const int num_writer_threads = FLAGS_concurrent_rw_benchmark_num_writer_threads;
  const int num_reader_threads = FLAGS_concurrent_rw_benchmark_num_reader_threads;
  const int num_inserts = FLAGS_concurrent_rw_benchmark_num_inserts;
  const int reader_boost = FLAGS_concurrent_rw_benchmark_reader_boost;

  const int num_threads = num_writer_threads + num_reader_threads;
  // Number of nodes we write in the 1st phase, and read back in the 2nd, while there are still
  // concurrent writes going on.
  const int num_inserts_first_phase = num_inserts / 2;

  // We apply a (deterministic) mapping for i that feels random enough (for the current purpose).
  auto generate_shuffled_kv = [](std::array<char, 32>& kbuf, std::array<char, 32>& vbuf, int i) {
    // any prime number satisfying p % 4 == 3 and at least 1 order of magnitude larger than number
    // of threads is good. (p < num_inserts is ok).
    constexpr int p = 10007; // Just picked the first above 10000
    auto random_shuffle = [](int x) {
      int32_t r = static_cast<int32_t>((static_cast<uint64_t>(x) * x) % p);
      if (x <= p / 2)
        return r;
      else
        return p - r;
    };
    snprintf(kbuf.data(), kbuf.size(), "key_%d_%d", random_shuffle(i % p),  i); // max 23 bytes used
    snprintf(vbuf.data(), vbuf.size(), "val_%d", i);
  };
  unique_ptr<CBTree<ProdTreeTraits>> tree;
  vector<thread> threads;
  // We need 2 internal barriers to know when to stop the first and start the
  // second LOG_TIMING(...)
  Barrier start_write_barrier(num_writer_threads + 1);
  Barrier finish_write_barrier(num_writer_threads + 1);
  Barrier start_rw_barrier(num_threads + 1);
  Barrier finish_rw_barrier(num_threads + 1);

  // Writer threads insert keys from [0, num_inserts_first_phase), then wait for the internal
  // barriers. Then insert keys from [num_inserts_first_phase, num_inserts_overall). We want to
  // insert randomly distributed values without any significant performance penalty.
  // generate_shuffled_kv will apply some smart shuffling.
  for (int tidx = 0; tidx < num_writer_threads; tidx++) {
    threads.emplace_back([&, tidx]() {
      std::array<char, 32> kbuf;
      std::array<char, 32> vbuf;
      while (true) {
        start_write_barrier.Wait();
        if (!tree) {
          start_rw_barrier.Wait();  // Allow readers to wake up too.
          return;
        }
        // To prevent the existence of a one-to-one mapping between reader and writer threads even
        // if num_writer_threads == num_reader_threads, in the first phase a writing thread writes
        // a continuous section of the keys, while reader threads distribute keys in a round-robin
        // fashion.
        int interval_length =
            (num_inserts_first_phase + num_writer_threads - 1) / num_writer_threads;
        int start = interval_length * tidx;
        int until = std::min(interval_length * (tidx + 1), num_inserts_first_phase);
        for (int i = start; i < until; ++i) {
          generate_shuffled_kv(kbuf, vbuf, i);
          if (!tree->Insert(Slice(kbuf.data()), Slice(vbuf.data()))) {
            ADD_FAILURE() << "Failed insert at iteration " << i;
            break;
          }
        }
        finish_write_barrier.Wait();
        start_rw_barrier.Wait();
        if (!tree) {
          return;
        }
        for (int i = num_inserts_first_phase + tidx; i < num_inserts;
             i += num_writer_threads) {
          generate_shuffled_kv(kbuf, vbuf, i);
          if (!tree->Insert(Slice(kbuf.data()), Slice(vbuf.data()))) {
            ADD_FAILURE() << "Failed insert at iteration " << i;
            break;
          }
        }
        finish_rw_barrier.Wait();
      }
    });
  }

  // We want to read values while writes are also happening. However, waiting with ASSERT_EVENTUALLY
  // would completely screw performance measuring. So we will read values that are already
  // guaranteed to be in the tree.
  for (int tidx = 0; tidx < num_reader_threads; tidx++) {
    threads.emplace_back([&, tidx]() {
      std::array<char, 32> kbuf;
      std::array<char, 32> vbuf;
      while (true) {
        // At this point, the 1st phase is done, and the first half of the keys are already in the
        // tree.
        start_rw_barrier.Wait();
        if (!tree) {
          return;
        }
        for (int64_t i = tidx;
             i < static_cast<int64_t>(num_inserts_first_phase) * reader_boost;
             i += num_reader_threads) {
          generate_shuffled_kv(kbuf, vbuf, static_cast<int32_t>(i % num_inserts_first_phase));
          VerifyGet(*tree, Slice(kbuf.data()), Slice(vbuf.data()));
        }
        finish_rw_barrier.Wait();
      }
    });
  }

  std::shared_ptr<MemoryTrackingBufferAllocator> mtbf;
  std::shared_ptr<ThreadSafeMemoryTrackingArena> arena_ptr;

  bool skip_normal_shutdown = false;

  for (int trial = 0; trial < kTrials; trial++) {
    // shared_ptrs are passed on the interfaces, so at first glance one would think it is safe to
    // reset the ptrs in any order. But it is not.
    tree.reset();
    arena_ptr.reset();
    mtbf = std::make_shared<MemoryTrackingBufferAllocator>(HeapBufferAllocator::Get(),
                                                           MemTracker::GetRootTracker());
    arena_ptr = std::make_shared<ThreadSafeMemoryTrackingArena>(16, mtbf);
    tree.reset(new CBTree<ProdTreeTraits>(arena_ptr));

    LOG_TIMING(
        INFO,
        Substitute(
            "Writing $0 values on $1 threads", num_inserts_first_phase, num_writer_threads)) {
      start_write_barrier.Wait();
      finish_write_barrier.Wait();
    }
    if (::testing::Test::HasFatalFailure()) {
      tree.reset(nullptr);
      start_rw_barrier.Wait();
      skip_normal_shutdown = true;
      break;
    }
    LOG_TIMING(INFO,
               Substitute("Writing $0 values on $1 threads and reading $2 values on $3 threads",
                          num_inserts - num_inserts_first_phase,
                          num_writer_threads,
                          static_cast<uint64_t>(num_inserts_first_phase)
                            * reader_boost,
                          num_reader_threads)) {
      start_rw_barrier.Wait();
      finish_rw_barrier.Wait();
    }
    if (::testing::Test::HasFatalFailure()) {
      // Normal shutdown is fine. Threads are already waiting for the next start.
      break;
    }
  }

  if (!skip_normal_shutdown) {
    tree.reset(nullptr);
    start_write_barrier.Wait();
    start_rw_barrier.Wait();
  }

  for (thread& thr : threads) {
    thr.join();
  }
}

} // namespace btree
} // namespace tablet
} // namespace kudu
