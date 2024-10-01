// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A WriteBatchWithIndex with a binary searchable index built for all the keys
// inserted.
#pragma once

#include <gflags/gflags_declare.h>

#include <memory>
#include <string>
#include <vector>
#include <db/memtable.h>

#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_batch_base.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class Comparator;
class DB;
class ReadCallback;
struct ReadOptions;
struct DBOptions;
class MergeContext;

enum WriteType {
  kPutRecord,
  kMergeRecord,
  kDeleteRecord,
  kSingleDeleteRecord,
  kDeleteRangeRecord,
  kLogDataRecord,
  kXIDRecord,
  kPutEntityRecord,
  kUnknownRecord,
};

const std::map<WriteType, ValueType> WriteTypeToValueTypeMap = {
  {kPutRecord, kTypeValue},
  {kMergeRecord, kTypeMerge},
  {kDeleteRecord, kTypeDeletion},
  {kSingleDeleteRecord, kTypeSingleDeletion},
  {kDeleteRangeRecord, kTypeRangeDeletion},
  // {kLogDataRecord, kTypeLogData},
  // {kXIDRecord, kTypeBeginPrepareXID},
  {kPutEntityRecord, kTypeWideColumnEntity},
  // {kUnknownRecord, kTypeNoop}
};

// An entry for Put, PutEntity, Merge, Delete, or SingleDelete for write
// batches. Used in WBWIIterator.
struct WriteEntry {
  WriteType type = kUnknownRecord;
  Slice key;
  Slice value;
};

// Iterator of one column family out of a WriteBatchWithIndex.
class WBWIIterator {
 public:
  virtual ~WBWIIterator() {}

  virtual bool Valid() const = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual void Seek(const Slice& key) = 0;

  virtual void SeekForPrev(const Slice& key) = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  // the return WriteEntry is only valid until the next mutation of
  // WriteBatchWithIndex
  virtual WriteEntry Entry() const = 0;

  virtual Status status() const = 0;
};

// A WriteBatchWithIndex with a binary searchable index built for all the keys
// inserted. In Put(), PutEntity(), Merge(), Delete(), or SingleDelete(), the
// corresponding function of the wrapped WriteBatch will be called. At the same
// time, indexes will be built. By calling GetWriteBatch(), a user will get the
// WriteBatch for the data they inserted, which can be used for DB::Write(). A
// user can call NewIterator() to create an iterator.
class WriteBatchWithIndex : public WriteBatchBase {
 public:
  // backup_index_comparator: the backup comparator used to compare keys
  // within the same column family, if column family is not given in the
  // interface, or we can't find a column family from the column family handle
  // passed in, backup_index_comparator will be used for the column family.
  // reserved_bytes: reserved bytes in underlying WriteBatch
  // max_bytes: maximum size of underlying WriteBatch in bytes
  // overwrite_key: if true, overwrite the key in the index when inserting
  //                the same key as previously, so iterator will never
  //                show two entries with the same key.
  explicit WriteBatchWithIndex(
      const Comparator* backup_index_comparator = BytewiseComparator(),
      size_t reserved_bytes = 0, bool overwrite_key = false,
      size_t max_bytes = 0, size_t protection_bytes_per_key = 0);

  ~WriteBatchWithIndex() override;
  WriteBatchWithIndex(WriteBatchWithIndex&&);
  WriteBatchWithIndex& operator=(WriteBatchWithIndex&&);

  using WriteBatchBase::Put;
  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value) override;

  Status Put(const Slice& key, const Slice& value) override;

  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& ts, const Slice& value) override;

  using WriteBatchBase::TimedPut;
  Status TimedPut(ColumnFamilyHandle* /* column_family */,
                  const Slice& /* key */, const Slice& /* value */,
                  uint64_t /* write_unix_time */) override {
    return Status::NotSupported(
        "TimedPut not supported by WriteBatchWithIndex");
  }

  Status PutEntity(ColumnFamilyHandle* column_family, const Slice& /* key */,
                   const WideColumns& /* columns */) override;

  Status PutEntity(const Slice& /* key */,
                   const AttributeGroups& attribute_groups) override {
    if (attribute_groups.empty()) {
      return Status::InvalidArgument(
          "Cannot call this method without attribute groups");
    }
    return Status::NotSupported(
        "PutEntity with AttributeGroups not supported by WriteBatchWithIndex");
  }

  using WriteBatchBase::Merge;
  Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
               const Slice& value) override;

  Status Merge(const Slice& key, const Slice& value) override;
  Status Merge(ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
               const Slice& /*ts*/, const Slice& /*value*/) override {
    return Status::NotSupported(
        "Merge does not support user-defined timestamp");
  }

  using WriteBatchBase::Delete;
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key) override;
  Status Delete(const Slice& key) override;
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                const Slice& ts) override;

  using WriteBatchBase::SingleDelete;
  Status SingleDelete(ColumnFamilyHandle* column_family,
                      const Slice& key) override;
  Status SingleDelete(const Slice& key) override;
  Status SingleDelete(ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts) override;

  using WriteBatchBase::DeleteRange;
  Status DeleteRange(ColumnFamilyHandle* /* column_family */,
                     const Slice& /* begin_key */,
                     const Slice& /* end_key */) override {
    return Status::NotSupported(
        "DeleteRange unsupported in WriteBatchWithIndex");
  }
  Status DeleteRange(const Slice& /* begin_key */,
                     const Slice& /* end_key */) override {
    return Status::NotSupported(
        "DeleteRange unsupported in WriteBatchWithIndex");
  }
  Status DeleteRange(ColumnFamilyHandle* /*column_family*/,
                     const Slice& /*begin_key*/, const Slice& /*end_key*/,
                     const Slice& /*ts*/) override {
    return Status::NotSupported(
        "DeleteRange unsupported in WriteBatchWithIndex");
  }

  using WriteBatchBase::PutLogData;
  Status PutLogData(const Slice& blob) override;

  using WriteBatchBase::Clear;
  void Clear() override;

  using WriteBatchBase::GetWriteBatch;
  WriteBatch* GetWriteBatch() override;

  // Create an iterator of a column family. User can call iterator.Seek() to
  // search to the next entry of or after a key. Keys will be iterated in the
  // order given by index_comparator. For multiple updates on the same key,
  // each update will be returned as a separate entry, in the order of update
  // time.
  //
  // The returned iterator should be deleted by the caller.
  WBWIIterator* NewIterator(ColumnFamilyHandle* column_family);
  WBWIIterator* NewIterator(uint32_t cf_id) const;
  // Create an iterator of the default column family.
  WBWIIterator* NewIterator();

  // Will create a new Iterator that will use WBWIIterator as a delta and
  // base_iterator as base.
  //
  // The returned iterator should be deleted by the caller.
  // The base_iterator is now 'owned' by the returned iterator. Deleting the
  // returned iterator will also delete the base_iterator.
  //
  // Updating write batch with the current key of the iterator is not safe.
  // We strongly recommend users not to do it. It will invalidate the current
  // key() and value() of the iterator. This invalidation happens even before
  // the write batch update finishes. The state may recover after Next() is
  // called.
  Iterator* NewIteratorWithBase(ColumnFamilyHandle* column_family,
                                Iterator* base_iterator,
                                const ReadOptions* opts = nullptr);
  // default column family
  Iterator* NewIteratorWithBase(Iterator* base_iterator);

  // Similar to DB::Get() but will only read the key from this batch.
  // If the batch does not have enough data to resolve Merge operations,
  // MergeInProgress status may be returned.
  Status GetFromBatch(ColumnFamilyHandle* column_family,
                      const DBOptions& options, const Slice& key,
                      std::string* value);

  // Similar to previous function but does not require a column_family.
  // Note:  An InvalidArgument status will be returned if there are any Merge
  // operators for this key.  Use previous method instead.
  Status GetFromBatch(const DBOptions& options, const Slice& key,
                      std::string* value) {
    return GetFromBatch(nullptr, options, key, value);
  }

  // If the batch contains an entry for "key" in "column_family", return it as a
  // wide-column entity in "*columns". If the entry is a wide-column entity,
  // return it as-is; if it is a plain key-value, return it as an entity with a
  // single anonymous column (see kDefaultWideColumnName) which contains the
  // value.
  //
  // Returns OK on success, NotFound if the there is no mapping for "key",
  // MergeInProgress if the key has merge operands but the base value cannot be
  // resolved based on the batch, or some error status (e.g. Corruption
  // or InvalidArgument) on failure.
  Status GetEntityFromBatch(ColumnFamilyHandle* column_family, const Slice& key,
                            PinnableWideColumns* columns);

  // Similar to DB::Get() but will also read writes from this batch.
  //
  // This function will query both this batch and the DB and then merge
  // the results using the DB's merge operator (if the batch contains any
  // merge requests).
  //
  // Setting read_options.snapshot will affect what is read from the DB
  // but will NOT change which keys are read from the batch (the keys in
  // this batch do not yet belong to any snapshot and will be fetched
  // regardless).
  Status GetFromBatchAndDB(DB* db, const ReadOptions& read_options,
                           const Slice& key, std::string* value);

  // An overload of the above method that receives a PinnableSlice
  Status GetFromBatchAndDB(DB* db, const ReadOptions& read_options,
                           const Slice& key, PinnableSlice* value);

  Status GetFromBatchAndDB(DB* db, const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value);

  // An overload of the above method that receives a PinnableSlice
  Status GetFromBatchAndDB(DB* db, const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* value);

  // Similar to DB::GetEntity() but also reads writes from this batch.
  //
  // This method queries the batch for the key and if the result can be
  // determined based on the batch alone, it is returned (assuming the key is
  // found, in the form of a wide-column entity). If the batch does not contain
  // enough information to determine the result (the key is not present in the
  // batch at all or a merge is in progress), the DB is queried and the result
  // is merged with the entries from the batch if necessary.
  //
  // Setting read_options.snapshot will affect what is read from the DB
  // but will NOT change which keys are read from the batch (the keys in
  // this batch do not yet belong to any snapshot and will be fetched
  // regardless).
  Status GetEntityFromBatchAndDB(DB* db, const ReadOptions& read_options,
                                 ColumnFamilyHandle* column_family,
                                 const Slice& key,
                                 PinnableWideColumns* columns) {
    constexpr ReadCallback* callback = nullptr;

    return GetEntityFromBatchAndDB(db, read_options, column_family, key,
                                   columns, callback);
  }

  void MultiGetFromBatchAndDB(DB* db, const ReadOptions& read_options,
                              ColumnFamilyHandle* column_family,
                              const size_t num_keys, const Slice* keys,
                              PinnableSlice* values, Status* statuses,
                              bool sorted_input);

  // Similar to DB::MultiGetEntity() but also reads writes from this batch.
  //
  // For each key, this method queries the batch and if the result can be
  // determined based on the batch alone, it is returned in the appropriate
  // PinnableWideColumns object (assuming the key is found). For all keys for
  // which the batch does not contain enough information to determine the result
  // (the key is not present in the batch at all or a merge is in progress), the
  // DB is queried and the result is merged with the entries from the batch if
  // necessary.
  //
  // Setting read_options.snapshot will affect what is read from the DB
  // but will NOT change which keys are read from the batch (the keys in
  // this batch do not yet belong to any snapshot and will be fetched
  // regardless).
  void MultiGetEntityFromBatchAndDB(DB* db, const ReadOptions& read_options,
                                    ColumnFamilyHandle* column_family,
                                    size_t num_keys, const Slice* keys,
                                    PinnableWideColumns* results,
                                    Status* statuses, bool sorted_input) {
    constexpr ReadCallback* callback = nullptr;

    MultiGetEntityFromBatchAndDB(db, read_options, column_family, num_keys,
                                 keys, results, statuses, sorted_input,
                                 callback);
  }

  // Records the state of the batch for future calls to RollbackToSavePoint().
  // May be called multiple times to set multiple save points.
  // TODO: how does CF work with save point?
  void SetSavePoint() override;

  // Remove all entries in this batch (Put, PutEntity, Merge, Delete,
  // SingleDelete, PutLogData) since the most recent call to SetSavePoint() and
  // removes the most recent save point. If there is no previous call to
  // SetSavePoint(), behaves the same as Clear().
  //
  // Calling RollbackToSavePoint invalidates any open iterators on this batch.
  //
  // Returns Status::OK() on success,
  //         Status::NotFound() if no previous call to SetSavePoint(),
  //         or other Status on corruption.
  Status RollbackToSavePoint() override;

  // Pop the most recent save point.
  // If there is no previous call to SetSavePoint(), Status::NotFound()
  // will be returned.
  // Otherwise returns Status::OK().
  Status PopSavePoint() override;

  void SetMaxBytes(size_t max_bytes) override;
  size_t GetDataSize() const;

  const std::unordered_map<uint32_t, uint32_t>& GetColumnFamilyIDs() const;

  bool HasDuplicateKeys() const;

 private:
  friend class PessimisticTransactionDB;
  friend class WritePreparedTxn;
  friend class WriteUnpreparedTxn;
  friend class WriteBatchWithIndex_SubBatchCnt_Test;
  friend class WriteBatchWithIndexInternal;
  // Returns the number of sub-batches inside the write batch. A sub-batch
  // starts right before inserting a key that is a duplicate of a key in the
  // last sub-batch.
  size_t SubBatchCnt();

  void MergeAcrossBatchAndDBImpl(ColumnFamilyHandle* column_family,
                                 const Slice& key,
                                 const PinnableWideColumns& existing,
                                 const MergeContext& merge_context,
                                 std::string* value,
                                 PinnableWideColumns* columns, Status* status);
  void MergeAcrossBatchAndDB(ColumnFamilyHandle* column_family,
                             const Slice& key,
                             const PinnableWideColumns& existing,
                             const MergeContext& merge_context,
                             PinnableSlice* value, Status* status);
  void MergeAcrossBatchAndDB(ColumnFamilyHandle* column_family,
                             const Slice& key,
                             const PinnableWideColumns& existing,
                             const MergeContext& merge_context,
                             PinnableWideColumns* columns, Status* status);

  Status GetFromBatchAndDB(DB* db, const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* value, ReadCallback* callback);
  void MultiGetFromBatchAndDB(DB* db, const ReadOptions& read_options,
                              ColumnFamilyHandle* column_family,
                              const size_t num_keys, const Slice* keys,
                              PinnableSlice* values, Status* statuses,
                              bool sorted_input, ReadCallback* callback);
  Status GetEntityFromBatchAndDB(DB* db, const ReadOptions& read_options,
                                 ColumnFamilyHandle* column_family,
                                 const Slice& key, PinnableWideColumns* columns,
                                 ReadCallback* callback);
  void MultiGetEntityFromBatchAndDB(DB* db, const ReadOptions& read_options,
                                    ColumnFamilyHandle* column_family,
                                    size_t num_keys, const Slice* keys,
                                    PinnableWideColumns* results,
                                    Status* statuses, bool sorted_input,
                                    ReadCallback* callback);

  struct Rep;
  std::unique_ptr<Rep> rep;
};

class FlushableWriteBatchWithIndexIterator : public InternalIterator {
  public:
  FlushableWriteBatchWithIndexIterator(std::shared_ptr<WBWIIterator>it, SequenceNumber seqno,
    const Comparator* comparator) : it_(it), global_seqno_(seqno), comparator_(comparator) {
  }
  bool Valid() const override {
    return it_->Valid();
  }
  void SeekToFirst() override {
    it_->SeekToFirst();
    UpdateKey();
  }
  void SeekToLast() override {
    it_->SeekToLast();
    UpdateKey();
  }
  void Seek(const Slice& target) override {
    it_->Seek(ExtractUserKey(target));
    if (it_->Valid()) {
      // compare seqno
      SequenceNumber seqno = GetInternalKeySeqno(target);
      if (seqno < global_seqno_ && comparator_->Compare(it_->Entry().key, target) == 0) {
        it_->Next();
      }
    }
    UpdateKey();
  }
  void SeekForPrev(const Slice& target) override {
    it_->SeekForPrev(ExtractUserKey(target));
    if (it_->Valid()) {
      SequenceNumber seqno = GetInternalKeySeqno(target);
      if (seqno > global_seqno_ && comparator_->Compare(it_->Entry().key, target) == 0) {
        it_->Prev();
      }
    }
    UpdateKey();
  }
  void Next() override {
    assert(Valid());
    it_->Next();
    UpdateKey();
  }
  bool NextAndGetResult(IterateResult* result) override {
    assert(Valid());
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
    }
    return is_valid;
  };
  void Prev() override {
    assert(Valid());
    it_->Prev();
    UpdateKey();
  }
  Slice key() const override {
    assert(Valid());
    return key_;
  }
  Slice value() const override {
    assert(Valid());
    return it_->Entry().value;
  }
  Status status() const override {
    return it_->status();
  }
  private:
  void UpdateKey() {
    if (!Valid()) {
      return;
    }
    key_buf_.Clear();
    assert(WriteTypeToValueTypeMap.find(it_->Entry().type) != WriteTypeToValueTypeMap.end());
    ValueType t = WriteTypeToValueTypeMap.at(it_->Entry().type);
    key_buf_.SetInternalKey(it_->Entry().key, global_seqno_, t);
    key_ = key_buf_.GetInternalKey();
  }
  std::shared_ptr<WBWIIterator> it_;
  SequenceNumber global_seqno_;
  const Comparator* comparator_;
  IterKey key_buf_;
  Slice key_;
};

class FlushableWriteBatchWithIndex : public Flushable {
  friend class WriteBatchWithIndex;
public:
  // TODO: assign seqno
  // TODO: initialize necessary memtable fields in constructor by iterating though wbwi
  FlushableWriteBatchWithIndex(std::shared_ptr<WriteBatchWithIndex> wbwi, const Comparator* cmp, uint32_t cf_id, const ImmutableOptions* immutable_options, const MutableCFOptions* cf_options):
    wbwi_(wbwi), comparator_(cmp), key_comparator_(comparator_),
  cf_id_(cf_id), moptions_(*immutable_options, *cf_options),
  clock_(immutable_options->clock),
  refs_(0) {
    atomic_flush_seqno_ = kMaxSequenceNumber;
  }
  ~FlushableWriteBatchWithIndex() override {
    fprintf(stdout, "Destruct refs = %d\n", refs_);
  }
  uint64_t GetMinLogContainingPrepSection() override {
    assert(min_prep_log_referenced_ != 0);
    return min_prep_log_referenced_;
  }
  MemTableStats ApproximateStats(const Slice& , const Slice& ) override {
    return {};
  }
  uint64_t ApproximateOldestKeyTime() const override {
    return kUnknownOldestAncesterTime;
  }
  std::unique_ptr<FlushJobInfo> ReleaseFlushJobInfo() override {
    return nullptr;
  }
  const InternalKeyComparator& GetInternalKeyComparator() const override {
    return key_comparator_;
  }
  size_t ApproximateMemoryUsage() override ;
  void Ref() override {
    ++refs_;
    fprintf(stdout, "Ref %d\n", refs_);
  };
  void MultiGet(const ReadOptions& , MultiGetRange* , ReadCallback* , bool ) override {};
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true) override ;
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true) override {
    SequenceNumber seq;
    return Get(key, value, columns, timestamp, s, merge_context,
               max_covering_tombstone_seq, &seq, read_opts, immutable_memtable,
               callback, is_blob_index, do_merge);
  };
  bool IsFragmentedRangeTombstonesConstructed() const override {
    return true;
  }
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(const ReadOptions& , SequenceNumber , bool ) override {
    return nullptr;
  }
  InternalIterator* NewIterator(const ReadOptions& , UnownedPtr<const SeqnoToTimeMapping> , Arena* ) override {
    assert(global_seqno_ != kMaxSequenceNumber);
    std::shared_ptr<WBWIIterator>it{wbwi_->NewIterator(cf_id_)};
    return new FlushableWriteBatchWithIndexIterator(it, global_seqno_, comparator_);
  }
  uint64_t num_entries() const override {
    uint64_t n = wbwi_->GetColumnFamilyIDs().at(cf_id_);
    return n;
  }
  uint64_t num_deletes() const override {
    return 0;
  }
  SequenceNumber GetEarliestSequenceNumber() override {
    return global_seqno_;
  }
  SequenceNumber GetFirstSequenceNumber() override {
    return global_seqno_;
  }
  void MarkFlushed() override {};
  size_t MemoryAllocatedBytes() const override {
    return 0;
  }
  uint64_t GetID() const override {
    return id_;
  }
  void SetNextLogNumber(uint64_t num) override {
    next_log_number_ = num;
  }
  uint64_t GetNextLogNumber() override {
    assert(next_log_number_ != 0);
    return next_log_number_;
  }
  bool UnrefFlushable() override {
    --refs_;
    fprintf(stdout, "Unref %d\n", refs_);
    assert(refs_ >= 0);
    return refs_ == 0;
  }
  void MarkImmutable() override {
    // ok
  }
  bool SetGlobalSequenceNumber (SequenceNumber global_seqno) override {
    global_seqno_ = global_seqno;
    return true;
  }
  void SetID(uint64_t id) override {
    id_ = id;
  }
  void SetMinPrepLog(uint64_t id) {
    min_prep_log_referenced_ = id;
  }
private:
  std::shared_ptr<WriteBatchWithIndex> wbwi_;
  const Comparator* comparator_;
  InternalKeyComparator key_comparator_;
  uint32_t cf_id_;
  SequenceNumber global_seqno_ = kMaxSequenceNumber;
  const ImmutableMemTableOptions moptions_;
  SystemClock* clock_;
  int refs_;
  uint64_t id_{0};
  uint64_t min_prep_log_referenced_{0};
  uint64_t next_log_number_{0};
};

}  // namespace ROCKSDB_NAMESPACE
