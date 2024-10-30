//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "db/merge_helper.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace ROCKSDB_NAMESPACE {

class WBWIMemtableIterator final : public InternalIterator {
 public:
  WBWIMemtableIterator(std::shared_ptr<WBWIIterator> it, SequenceNumber seqno,
                       const Comparator* comparator)
      : it_(it), global_seqno_(seqno), comparator_(comparator) {
    assert(seqno != kMaxSequenceNumber);
  }
  bool Valid() const override { return valid_; }
  void SeekToFirst() override {
    it_->SeekToFirst();
    UpdateKey();
  }
  void SeekToLast() override {
    it_->SeekToLast();
    UpdateKey();
  }
  // TODO: simply by ordering by descending offset in WBWI
  // TODO: remove duplicate key support for first version
  // Move the iterator to the next user key
  void NextKey() {
    if (it_->Valid()) {
      Slice cur_user_key = it_->Entry().key;
      it_->Next();
      while (it_->Valid() &&
             comparator_->Compare(cur_user_key, it_->Entry().key) == 0) {
        it_->Next();
      }
    }
  }

  void MoveToLastCurrentKey() {
    if (it_->Valid()) {
      NextKey();
      if (it_->Valid()) {
        it_->Prev();
      } else {
        it_->SeekToLast();
      }
    }
  }

  void Seek(const Slice& target) override {
    it_->Seek(ExtractUserKey(target));
    if (it_->Valid()) {
      // compare seqno
      SequenceNumber seqno = GetInternalKeySeqno(target);
      if (seqno < global_seqno_ &&
          comparator_->Compare(it_->Entry().key, ExtractUserKey(target)) == 0) {
        NextKey();
        // TODO: go to next key
        // it_->Next();
      }
    }
    // Move to last occurrence
    MoveToLastCurrentKey();
    // TODO: move to the last occurrence of this key
    UpdateKey();
  }
  void SeekForPrev(const Slice& target) override {
    it_->SeekForPrev(ExtractUserKey(target));
    if (it_->Valid()) {
      SequenceNumber seqno = GetInternalKeySeqno(target);
      if (seqno > global_seqno_ &&
          comparator_->Compare(it_->Entry().key, target) == 0) {
        it_->Prev();
      }
    }
    UpdateKey();
  }
  void Next() override {
    // TODO: check that if we should move backward to get the next internal key
    // By move back, and compare key
    assert(Valid());
    Slice cur_user_key = it_->Entry().key;
    // First try go backwards
    {
      it_->Prev();
      if (it_->Valid()) {
        if (comparator_->Compare(it_->Entry().key, cur_user_key) == 0) {
          UpdateKey();
          return;
        } else {
          // restore position
          it_->Next();
        }
      } else {
        it_->SeekToFirst();
      }
    }

    // it_->Next();
    // Move to the first entry of the next user key
    NextKey();
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
    assert(it_->status().ok());
    return s_;
  }

 private:
  void UpdateKey() {
    valid_ = it_->Valid();
    if (!Valid()) {
      return;
    }
    key_buf_.Clear();
    assert(WriteTypeToValueTypeMap.find(it_->Entry().type) !=
           WriteTypeToValueTypeMap.end());
    auto t = WriteTypeToValueTypeMap.find(it_->Entry().type);
    assert(t != WriteTypeToValueTypeMap.end());
    if (t == WriteTypeToValueTypeMap.end()) {
      valid_ = false;
      s_ = Status::Corruption("Unexpected write_batch_with_index entry type " + std::to_string(t->second));
      return;
    }
    key_buf_.SetInternalKey(it_->Entry().key, global_seqno_, t->second);
    key_ = key_buf_.GetInternalKey();
  }
  std::shared_ptr<WBWIIterator> it_;
  SequenceNumber global_seqno_;
  const Comparator* comparator_;
  IterKey key_buf_;
  Slice key_;
  std::vector<WriteEntry> current_entries_;
  Status s_;
  bool valid_ = false;
};

// Current limitations/TODO:
// - Operations not supported yet: DeleteRange/WideColumn
// TODO: not support Merge
// - Need to provide memtable metadata like data_size().
// - Does not support UDT.
class WBWIMemtable final : public ReadOnlyMemtable {
  friend class WriteBatchWithIndex;
 public:
  WBWIMemtable(std::shared_ptr<WriteBatchWithIndex> wbwi, const Comparator* cmp,
               uint32_t cf_id, const ImmutableOptions* immutable_options,
               const MutableCFOptions* cf_options)
      : wbwi_(wbwi),
        comparator_(cmp),
        key_comparator_(comparator_),
        cf_id_(cf_id),
        moptions_(*immutable_options, *cf_options),
        clock_(immutable_options->clock),
        refs_(0) {
  }
  ~WBWIMemtable() override { fprintf(stdout, "Destruct refs = %d\n", refs_); }

  const char* Name() const override { return "WBWIMemtable"; }

  using ReadOnlyMemtable::Get;
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true) override;

  void MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                        ReadCallback* callback, bool immutable_memtable) override;

  uint64_t GetMinLogContainingPrepSection() override {
    assert(min_prep_log_referenced_ != 0);
    return min_prep_log_referenced_;
  }
  MemTableStats ApproximateStats(const Slice&, const Slice&) override {
    // FIXME: Do the actual walk through since this is not big and occurs infrequently?
    return {};
  }
  uint64_t ApproximateOldestKeyTime() const override {
    // FIXME: Can use table creation time
    return kUnknownOldestAncesterTime;
  }
  const InternalKeyComparator& GetInternalKeyComparator() const override {
    return key_comparator_;
  }
  size_t ApproximateMemoryUsage() override {
    // FIXME: either calculate for each CF during initialization,
    //  or just divide evenly among CFs
    return 0;
  }

  bool IsFragmentedRangeTombstonesConstructed() const override { return true; }
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions&, SequenceNumber, bool) override {
    return nullptr;
  }
  // TODO: does read options matter?, what snapshot to use
  // TODO: seqno to time mapping?
  InternalIterator* NewIterator(const ReadOptions&,
                                UnownedPtr<const SeqnoToTimeMapping>,
                                Arena* arena) override {
    assert(global_seqno_ != kMaxSequenceNumber);
    std::shared_ptr<WBWIIterator> it{wbwi_->NewIterator(cf_id_)};
    assert(arena);
      auto mem = arena->AllocateAligned(sizeof(WBWIMemtableIterator));
      return new (mem) WBWIMemtableIterator(it, global_seqno_, comparator_);
  }

  InternalIterator* NewIterator() {
    assert(global_seqno_ != kMaxSequenceNumber);
    std::shared_ptr<WBWIIterator> it{wbwi_->NewIterator(cf_id_)};
    return new WBWIMemtableIterator(it, global_seqno_, comparator_);
  }

  uint64_t num_entries() const override {
    uint64_t n = wbwi_->GetColumnFamilyIDs().at(cf_id_);
    return n;
  }
  uint64_t num_deletes() const override {
    // FIXME: this is used for stats and event logging
    return 0;
  }
  SequenceNumber GetEarliestSequenceNumber() override { return global_seqno_; }
  SequenceNumber GetFirstSequenceNumber() override { return global_seqno_; }
  void MarkFlushed() override{};
  size_t MemoryAllocatedBytes() const override {
    // FIXME: similar to ApproximateMemoryUsage(). Used in MemTableList to drop memtable history.
    return 0;
  }
  void MarkImmutable() override {}
  void SetGlobalSequenceNumber(SequenceNumber global_seqno) {
    global_seqno_ = global_seqno;
  }
  void SetMinPrepLog(uint64_t id) { min_prep_log_referenced_ = id; }
  void UniqueRandomSample(
      const uint64_t& /* target_sample_size */,
      std::unordered_set<const char*>* /* entries */) override {
    // FIXME: used for mempurge
    assert(false);
  }
  uint64_t GetDataSize() const override {
    // FIXME: used in event logging in flush_job
    return 0;
  }
  uint64_t NumRangeDeletion() const override { return 0; }
  const Slice& GetNewestUDT() const override {
    // FIXME: UDT support
    assert(false);
    return slice_;
  }

 private:
  Slice slice_;
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
};
}  // namespace ROCKSDB_NAMESPACE
