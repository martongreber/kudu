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
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/common/schema.h"
#include "kudu/gutil/port.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
template <typename T> class RepeatedPtrField;
}  // namespace protobuf
}  // namespace google

namespace kudu {

class Arena;
class ConstContiguousRow;
class KuduPartialRow;
class PartitionPB;
class PartitionSchemaPB;
class PartitionSchemaPB_HashBucketSchemaPB;
template <typename Buffer> class KeyEncoder;

// A Partition describes the set of rows that a Tablet is responsible for
// serving. Each tablet is assigned a single Partition.
//
// Partitions consist primarily of a start and end partition key. Every row with
// a partition key that falls in a Tablet's Partition will be served by that
// tablet.
//
// In addition to the start and end partition keys, a Partition holds metadata
// to determine if a scan can prune, or skip, a partition based on the scan's
// start and end primary keys, and predicates.
class Partition {
 public:
  const std::vector<int32_t>& hash_buckets() const {
    return hash_buckets_;
  }

  Slice range_key_start() const;

  Slice range_key_end() const;

  const std::string& partition_key_start() const {
    return partition_key_start_;
  }

  const std::string& partition_key_end() const {
    return partition_key_end_;
  }

  // Returns true iff the given partition 'rhs' is equivalent to this one.
  bool operator==(const Partition& rhs) const;
  bool operator!=(const Partition& rhs) const {
    return !(*this == rhs);
  }

  // Serializes a partition into a protobuf message.
  void ToPB(PartitionPB* pb) const;

  // Deserializes a protobuf message into a partition.
  //
  // The protobuf message is not validated, since partitions are only expected
  // to be created by the master process.
  static void FromPB(const PartitionPB& pb, Partition* partition);

 private:
  friend class PartitionSchema;

  // Helper function for accessing the range key portion of a partition key.
  Slice range_key(const std::string& partition_key) const;

  std::vector<int32_t> hash_buckets_;

  std::string partition_key_start_;
  std::string partition_key_end_;
};

// A partition schema describes how the rows of a table are distributed among
// tablets.
//
// Primarily, a table's partition schema is responsible for translating the
// primary key column values of a row into a partition key that can be used to
// determine the tablet containing the key.
//
// The partition schema is made up of zero or more hash bucket components,
// followed by a single range component. In addition, the partition schema can
// contain multiple ranges and their per-range custom bucket schemas.
//
// Each hash bucket component includes one or more columns from the primary key
// column set, with the restriction that an individual primary key column may
// only be included in a single hash component.
//
// To determine the hash bucket of an individual row, the values of the columns
// of the hash component are encoded into bytes (in PK or lexicographic
// preserving encoding), then hashed into a u64, then modded into an i32. When
// constructing a partition key from a row, the buckets of the row are simply
// encoded into the partition key in order (again in PK or lexicographic
// preserving encoding).
//
// The range component contains a (possibly full or empty) subset of the primary
// key columns. When encoding the partition key, the columns of the partition
// component are encoded in order.
//
// The above is true of the relationship between rows and partition keys. It
// gets trickier with partitions (tablet partition key boundaries), because the
// boundaries of tablets do not necessarily align to rows. For instance,
// currently the absolute-start and absolute-end primary keys of a table
// represented as an empty key, but do not have a corresponding row. Partitions
// are similar, but instead of having just one absolute-start and absolute-end,
// each component of a partition schema has an absolute-start and absolute-end.
// When creating the initial set of partitions during table creation, we deal
// with this by "carrying through" absolute-start or absolute-ends into lower
// significance components.
//
// Notes on redaction:
//
// For the purposes of redaction, Kudu considers partitions and partition
// schemas to be metadata - not sensitive data which needs to be redacted from
// log files. However, the partition keys of individual rows _are_ considered
// sensitive, so we redact them from log messages and error messages. Thus,
// methods which format partitions and partition schemas will never redact, but
// the methods which format individual partition keys do redact.
class PartitionSchema {
 public:
  struct RangeSchema {
    std::vector<ColumnId> column_ids;
  };

  struct HashBucketSchema {
    std::vector<ColumnId> column_ids;
    int32_t num_buckets;
    uint32_t seed;

    bool operator==(const HashBucketSchema& rhs) const {
      if (this == &rhs) {
        return true;
      }
      if (seed != rhs.seed) {
        return false;
      }
      if (num_buckets != rhs.num_buckets) {
        return false;
      }
      if (column_ids != rhs.column_ids) {
        return false;
      }
      return true;
    }

    bool operator!=(const HashBucketSchema& rhs) const {
      return !(*this == rhs);
    }
  };

  typedef std::vector<HashBucketSchema> HashBucketSchemas;
  // Holds each bound's HashBucketSchemas.
  typedef std::vector<HashBucketSchemas> PerRangeHashBucketSchemas;

  struct RangeWithHashSchemas {
    std::string lower;
    std::string upper;
    HashBucketSchemas hash_schemas;
  };
  typedef std::vector<RangeWithHashSchemas> RangesWithHashSchemas;

  // Extracts HashBucketSchemas from a protobuf repeated field of hash buckets.
  static Status ExtractHashBucketSchemasFromPB(
      const Schema& schema,
      const google::protobuf::RepeatedPtrField<PartitionSchemaPB_HashBucketSchemaPB>&
          hash_buckets_pb,
      HashBucketSchemas* hash_bucket_schemas);

  // Deserializes a protobuf message into a partition schema.
  static Status FromPB(const PartitionSchemaPB& pb,
                       const Schema& schema,
                       PartitionSchema* partition_schema) WARN_UNUSED_RESULT;

  // Serializes a partition schema into a protobuf message.
  // Requires a schema to encode the range bounds.
  Status ToPB(const Schema& schema, PartitionSchemaPB* pb) const;

  // Appends the row's encoded partition key into the provided buffer.
  // On failure, the buffer may have data partially appended.
  std::string EncodeKey(const KuduPartialRow& row) const;
  std::string EncodeKey(const ConstContiguousRow& row) const;

  // Creates the set of table partitions for a partition schema and collection
  // of split rows and split bounds.
  //
  // Split bounds define disjoint ranges for which tablets will be created. If
  // empty, then Kudu assumes a single unbounded range. Each split key must fall
  // into one of the ranges, and results in the range being split.  The number
  // of resulting partitions is the product of the number of hash buckets for
  // each hash bucket component, multiplied by
  // (split_rows.size() + max(1, range_bounds.size())).
  // 'range_hash_schemas' contains each range's HashBucketSchemas,
  // its order corresponds to the bounds in 'range_bounds'.
  // If 'range_hash_schemas' is empty, the table wide hash schema is used per range.
  // Size of 'range_hash_schemas' and 'range_bounds' are equal if 'range_hash_schema' isn't empty.
  Status CreatePartitions(
      const std::vector<KuduPartialRow>& split_rows,
      const std::vector<std::pair<KuduPartialRow, KuduPartialRow>>& range_bounds,
      const PerRangeHashBucketSchemas& range_hash_schemas,
      const Schema& schema,
      std::vector<Partition>* partitions) const WARN_UNUSED_RESULT;

  // Check if the given partition contains the specified row. The row must have
  // all the columns participating in the table's partitioning schema
  // set to particular values.
  bool PartitionContainsRow(const Partition& partition,
                            const KuduPartialRow& row) const;
  bool PartitionContainsRow(const Partition& partition,
                            const ConstContiguousRow& row) const;

  // Check if the specified row is probably in the given partition.
  // The collection of columns set to particular values in the row can be a
  // subset of all the columns participating in the table's partitioning schema.
  // This method can be used to optimize the collection of values for IN list
  // predicates. As of now, this method is effectively implemented only for
  // single-column hash and single-column range partitioning schemas, meaning
  // that it can return false positives in case of other than single-row range
  // and hash partitioning schemas.
  //
  // NOTE: this method returns false positives in some cases (see above)
  //
  // TODO(aserbin): implement this for multi-row range schemas as well,
  //                substituting non-specified columns in the row with values
  //                from the partition's start key and return logically inverted
  //                result of calling PartitionContainsRow() with the
  //                artificially constructed row
  bool PartitionMayContainRow(const Partition& partition,
                              const KuduPartialRow& row) const;

  // Returns a text description of the partition suitable for debug printing.
  //
  // Partitions are considered metadata, so no redaction will happen on the hash
  // and range bound values.
  std::string PartitionDebugString(const Partition& partition, const Schema& schema) const;

  // Returns a text description of a partition key suitable for debug printing.
  std::string PartitionKeyDebugString(Slice key, const Schema& schema) const;
  std::string PartitionKeyDebugString(const KuduPartialRow& row) const;
  std::string PartitionKeyDebugString(const ConstContiguousRow& row) const;

  // Returns a text description of the range partition with the provided
  // inclusive lower bound and exclusive upper bound.
  //
  // Range partitions are considered metadata, so no redaction will happen on
  // the row values.
  std::string RangePartitionDebugString(const KuduPartialRow& lower_bound,
                                        const KuduPartialRow& upper_bound) const;
  std::string RangePartitionDebugString(Slice lower_bound,
                                        Slice upper_bound,
                                        const Schema& schema) const;

  // Returns a text description of this partition schema suitable for debug printing.
  //
  // The partition schema is considered metadata, so partition bound information
  // is not redacted from the returned string.
  std::string DebugString(const Schema& schema) const;

  // Returns a text description of this partition schema suitable for display in the web UI.
  // The format of this string is not guaranteed to be identical cross-version.
  //
  // 'range_partitions' should include the set of range partitions in the table,
  // as formatted by 'RangePartitionDebugString'.
  std::string DisplayString(const Schema& schema,
                            const std::vector<std::string>& range_partitions) const;

  // Returns header and entry HTML cells for the partition schema for the master
  // table web UI. This is an abstraction leak, but it's better than leaking the
  // internals of partitions to the master path handlers.
  //
  // Partitions are considered metadata, so no redaction will be done.
  std::string PartitionTableHeader(const Schema& schema) const;
  std::string PartitionTableEntry(const Schema& schema, const Partition& partition) const;

  // Returns 'true' iff the partition schema 'rhs' is equivalent to this one.
  bool operator==(const PartitionSchema& rhs) const;

  // Returns 'true' iff the partition schema 'rhs' is not equivalent to this one.
  bool operator!=(const PartitionSchema& rhs) const {
    return !(*this == rhs);
  }

  // Transforms an exclusive lower bound range partition key into an inclusive
  // lower bound range partition key.
  //
  // The provided partial row is considered metadata, so error messages may
  // contain unredacted row data.
  Status MakeLowerBoundRangePartitionKeyInclusive(KuduPartialRow* row) const;

  // Transforms an inclusive upper bound range partition key into an exclusive
  // upper bound range partition key.
  //
  // The provided partial row is considered metadata, so error messages may
  // contain unredacted row data.
  Status MakeUpperBoundRangePartitionKeyExclusive(KuduPartialRow* row) const;

  // Decodes a range partition key into a partial row, with variable-length
  // fields stored in the arena.
  Status DecodeRangeKey(Slice* encoded_key,
                        KuduPartialRow* partial_row,
                        Arena* arena) const;

  const RangeSchema& range_partition_schema() const {
    return range_schema_;
  }

  const HashBucketSchemas& hash_partition_schemas() const {
    return hash_bucket_schemas_;
  }

  const RangesWithHashSchemas& ranges_with_hash_schemas() const {
    return ranges_with_hash_schemas_;
  }

  // Given the specified table schema, populate the 'range_column_indexes'
  // container with column indexes of the range partition keys.
  // If any of the columns is not in the key range columns then an
  // InvalidArgument status is returned.
  Status GetRangeSchemaColumnIndexes(
      const Schema& schema,
      std::vector<int>* range_column_indexes) const;

 private:
  friend class PartitionPruner;
  FRIEND_TEST(PartitionTest, TestIncrementRangePartitionBounds);
  FRIEND_TEST(PartitionTest, TestIncrementRangePartitionStringBounds);
  FRIEND_TEST(PartitionTest, TestVarcharRangePartitions);

  // Tests if the hash partition contains the row with given hash_idx.
  bool HashPartitionContainsRow(const Partition& partition,
                                const KuduPartialRow& row,
                                int hash_idx) const;

  // Tests if the range partition contains the row.
  bool RangePartitionContainsRow(const Partition& partition,
                                 const KuduPartialRow& row) const;

  // Returns a text description of the encoded range key suitable for debug printing.
  std::string RangeKeyDebugString(Slice range_key, const Schema& schema) const;
  std::string RangeKeyDebugString(const KuduPartialRow& key) const;
  std::string RangeKeyDebugString(const ConstContiguousRow& key) const;

  // Encodes the specified columns of a row into lexicographic sort-order
  // preserving format.
  static void EncodeColumns(const KuduPartialRow& row,
                            const std::vector<ColumnId>& column_ids,
                            std::string* buf);

  // Encodes the specified columns of a row into lexicographic sort-order
  // preserving format.
  static void EncodeColumns(const ConstContiguousRow& row,
                            const std::vector<ColumnId>& column_ids,
                            std::string* buf);

  // Returns the hash bucket of the encoded hash column. The encoded columns must match the
  // columns of the hash bucket schema.
  static int32_t BucketForEncodedColumns(const std::string& encoded_hash_columns,
                                         const HashBucketSchema& hash_bucket_schema);

  // Helper function that validates the hash bucket schemas.
  static Status ValidateHashBucketSchemas(const Schema& schema,
                                          const HashBucketSchemas& hash_schemas);

  // Generates hash partitions for each combination of hash buckets in hash_schemas.
  static std::vector<Partition> GenerateHashPartitions(
      const HashBucketSchemas& hash_schemas,
      const KeyEncoder<std::string>& hash_encoder);

  // Assigns the row to a hash bucket according to the hash schema.
  template<typename Row>
  static int32_t BucketForRow(const Row& row,
                              const HashBucketSchema& hash_bucket_schema);

  // PartitionKeyDebugString implementation for row types.
  template<typename Row>
  std::string PartitionKeyDebugStringImpl(const Row& row) const;

  // Private templated helper for PartitionContainsRow.
  template<typename Row>
  bool PartitionContainsRowImpl(const Partition& partition,
                                const Row& row) const;

  // Private templated helper for HashPartitionContainsRow.
  template<typename Row>
  bool HashPartitionContainsRowImpl(
      const Partition& partition,
      const Row& row,
      const HashBucketSchemas& hash_bucket_schemas,
      int hash_idx) const;

  // Private templated helper for RangePartitionContainsRow.
  template<typename Row>
  bool RangePartitionContainsRowImpl(const Partition& partition,
                                     const Row& row) const;

  // Private templated helper for EncodeKey.
  template<typename Row>
  void EncodeKeyImpl(const Row& row, std::string* buf) const;

  // Returns true if all of the columns in the range partition key are unset in
  // the row.
  bool IsRangePartitionKeyEmpty(const KuduPartialRow& row) const;

  // Appends the stringified range partition components of a partial row to a
  // vector.
  //
  // If any columns of the range partition do not exist in the partial row, the
  // logical minimum value for that column will be used instead.
  void AppendRangeDebugStringComponentsOrMin(const KuduPartialRow& row,
                                             std::vector<std::string>* components) const;

  // Returns the stringified hash and range schema components of the partition
  // schema.
  //
  // Partition schemas are considered metadata, so no redaction will happen on
  // the hash and range bound values.
  std::vector<std::string> DebugStringComponents(const Schema& schema) const;

  // Encode the provided row into a range key. The row must not include values
  // for any columns not in the range key. Missing range values will be filled
  // with the logical minimum value for the column. A row without any values
  // will encode to an empty string.
  //
  // This method is useful used for encoding splits and bounds.
  Status EncodeRangeKey(const KuduPartialRow& row, const Schema& schema, std::string* key) const;

  // Decodes the hash bucket component of a partition key into its buckets.
  //
  // This should only be called with partition keys created from a row, not with
  // partition keys from a partition.
  Status DecodeHashBuckets(Slice* encoded_key, std::vector<int32_t>* buckets) const;

  // Clears the state of this partition schema.
  void Clear();

  // Validates that this partition schema is valid. Returns OK, or an
  // appropriate error code for an invalid partition schema.
  Status Validate(const Schema& schema) const;

  // Validates the split rows, converts them to partition key form, and inserts
  // them into splits in sorted order.
  Status EncodeRangeSplits(const std::vector<KuduPartialRow>& split_rows,
                           const Schema& schema,
                           std::vector<std::string>* splits) const;

  // Validates the range bounds, converts them to partition key form, and
  // inserts them into 'bounds_with_hash_schemas' in sorted order. The hash schemas
  // per range are stored within 'range_hash_schemas'. If 'range_hash_schemas' is empty,
  // it indicates that the table wide hash schema will be used per range.
  Status EncodeRangeBounds(
      const std::vector<std::pair<KuduPartialRow, KuduPartialRow>>& range_bounds,
      const PerRangeHashBucketSchemas& range_hash_schemas,
      const Schema& schema,
      RangesWithHashSchemas* bounds_with_hash_schemas) const;

  // Splits the encoded range bounds by the split points. The splits and bounds within
  // 'bounds_with_hash_schemas' must be sorted. If `bounds_with_hash_schemas` is empty,
  // then a single unbounded range is assumed. If any of the splits falls outside
  // of the bounds, then an InvalidArgument status is returned.
  Status SplitRangeBounds(const Schema& schema,
                          const std::vector<std::string>& splits,
                          RangesWithHashSchemas* bounds_with_hash_schemas) const;

  // Increments a range partition key, setting 'increment' to true if the
  // increment succeeds, or false if all range partition columns are already the
  // maximum value. Unset columns will be incremented to increment(min_value).
  Status IncrementRangePartitionKey(KuduPartialRow* row, bool* increment) const;

  // Find hash bucket schemas for the given encoded range key. Depending
  // on the partitioning schema and the key, it might be either table-wide
  // or a custom hash bucket schema for a particular range.
  const HashBucketSchemas& GetHashBucketSchemasForRange(
      const std::string& range_key) const;

  HashBucketSchemas hash_bucket_schemas_;
  RangeSchema range_schema_;
  RangesWithHashSchemas ranges_with_hash_schemas_;

  // Encoded start of the range --> index of the hash bucket schemas for the
  // range in the 'ranges_with_hash_schemas_' array container.
  // NOTE: the contents of this map and 'ranges_with_hash_schemas_' are tightly
  //       coupled -- it's necessary to clear/set this map along with
  //       'ranges_with_hash_schemas_'.
  typedef std::map<std::string, size_t> HashSchemasByEncodedLowerRange;
  HashSchemasByEncodedLowerRange hash_schema_idx_by_encoded_range_start_;
};

} // namespace kudu
