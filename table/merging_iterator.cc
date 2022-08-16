//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merging_iterator.h"

#include "db/arena_wrapped_db_iter.h"
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "memory/arena.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "table/iter_heap.h"
#include "table/iterator_wrapper.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"
#include "util/heap.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {
// For merging iterator to process range deletions, we treat the start and end
// key of a range deletion as point keys and put them into the minHeap/maxHeap
// used in merging iterator. Take minHeap for example, we are able to keep track
// of currently active range tombstones (the ones where start keys are popped
// but end keys are still in the heap). This `active_` set of range tombstones
// is then used to quickly determine whether the point key at heap top is
// deleted (by heap property, the point key at heap top must be within internal
// key range of active range tombstones).
//
// The HeapItem structure represents the aforementioned 3 types of elements in
// the minHeap/maxHeap.
struct HeapItem {
  HeapItem() = default;

  enum Type { ITERATOR, DELETE_RANGE_START, DELETE_RANGE_END };
  IteratorWrapper iter;
  size_t level = 0;
  std::string pinned_key;
  // Will be overwritten, initialize here so compiler does not complain.
  Type type = ITERATOR;

  explicit HeapItem(size_t _level, InternalIteratorBase<Slice>* _iter)
      : level(_level), type(Type::ITERATOR) {
    iter.Set(_iter);
  }

  void SetTombstoneEndKey(ParsedInternalKey&& pik) {
    pinned_key.clear();
    // Range tombstone end key is exclusive. If a point internal key has same
    // user key and sequence number as the start or end key, the order will be
    // start < end key < < internal key.
    //
    // Note: The op_type change of end key may not be necessary, but kept so
    // that keys are non-decreasing in the case of a range tombstone and a user
    // key straddles two SST files.
    ParsedInternalKey p(pik.user_key, pik.sequence, kValueTypeForSeek);
    AppendInternalKey(&pinned_key, p);
  }

  Slice key() const {
    if (type == Type::ITERATOR) {
      return iter.key();
    }
    return pinned_key;
  }

  bool IsDeleteRangeSentinelKey() const {
    if (type == Type::ITERATOR) {
      return iter.IsDeleteRangeSentinelKey();
    }
    return false;
  }
};

class MinHeapItemComparator {
 public:
  MinHeapItemComparator(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}
  bool operator()(HeapItem* a, HeapItem* b) const {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }

 private:
  const InternalKeyComparator* comparator_;
};

class MaxHeapItemComparator {
 public:
  MaxHeapItemComparator(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}
  bool operator()(HeapItem* a, HeapItem* b) const {
    return comparator_->Compare(a->key(), b->key()) < 0;
  }

 private:
  const InternalKeyComparator* comparator_;
};

using MergerMinIterHeap = BinaryHeap<HeapItem*, MinHeapItemComparator>;
using MergerMaxIterHeap = BinaryHeap<HeapItem*, MaxHeapItemComparator>;
}  // namespace

class MergingIterator : public InternalIterator {
 public:
  MergingIterator(const InternalKeyComparator* comparator,
                  InternalIterator** children, int n, bool is_arena_mode,
                  bool prefix_seek_mode)
      : is_arena_mode_(is_arena_mode),
        prefix_seek_mode_(prefix_seek_mode),
        direction_(kForward),
        comparator_(comparator),
        current_(nullptr),
        minHeap_(comparator_),
        pinned_iters_mgr_(nullptr) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].level = i;
      children_[i].iter.Set(children[i]);
    }
  }

  void considerStatus(Status s) {
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  virtual void AddIterator(InternalIterator* iter) {
    children_.emplace_back(children_.size(), iter);
    if (pinned_iters_mgr_) {
      iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
    // Invalidate to ensure `Seek*()` is called to construct the heaps before
    // use.
    current_ = nullptr;
  }

  // Merging iterator can optionally process range deletions: if a key is
  // covered by a range deletion, the merging iterator will not output it but
  // skip it.
  //
  // Add next range tombstone iterator to this merging iterator.
  // There must be either no range tombstone iterator, or same number of
  // range tombstone iterators as point iterators after all range tombstone
  // iters are added. The i-th added range tombstone iterator and the i-th point
  // iterator must point to the same sorted run.
  // Merging iterator takes ownership of the range tombstone iterator and
  // is responsible for freeing it. Note that during Iterator::Refresh()
  // and when a level iterator moves to a different SST file, the range
  // tombstone iterator could be updated. In that case, the merging iterator
  // is only responsible to freeing the new range tombstone iterator
  // that it has pointers to in range_tombstone_iters_.
  void AddRangeTombstoneIterator(TruncatedRangeDelIterator* iter) {
    range_tombstone_iters_.emplace_back(iter);
  }

  // Called by MergingIteratorBuilder when all point iterators and range
  // tombstone iterators are added. Initializes HeapItems for range tombstone
  // iterators.
  void Finish() {
    if (!range_tombstone_iters_.empty()) {
      pinned_heap_item_.resize(range_tombstone_iters_.size() * 2);
      for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
        pinned_heap_item_[2 * i].type = HeapItem::DELETE_RANGE_START;
        pinned_heap_item_[2 * i].level = i;
        pinned_heap_item_[2 * i + 1].type = HeapItem::DELETE_RANGE_END;
        pinned_heap_item_[2 * i + 1].level = i;
      }
    }
  }

  ~MergingIterator() override {
    for (auto child : range_tombstone_iters_) {
      delete child;
    }

    for (auto& child : children_) {
      child.iter.DeleteIter(is_arena_mode_);
    }
    status_.PermitUncheckedError();
  }

  bool Valid() const override { return current_ != nullptr && status_.ok(); }

  Status status() const override { return status_; }

  // Add current range tombstone from range_tombstone_iters_[level] into min
  // heap.
  void InsertRangeTombstoneToMinHeap(size_t level) {
    assert(!range_tombstone_iters_.empty() &&
           range_tombstone_iters_[level]->Valid());
    pinned_heap_item_[level * 2].SetTombstoneEndKey(
        range_tombstone_iters_[level]->start_key());
    pinned_heap_item_[level * 2 + 1].SetTombstoneEndKey(
        range_tombstone_iters_[level]->end_key());
    minHeap_.push(&pinned_heap_item_[level * 2]);
    minHeap_.push(&pinned_heap_item_[level * 2 + 1]);
  }

  // Add current range tombstone from range_tombstone_iters_[level] into max
  // heap.
  void InsertRangeTombstoneToMaxHeap(size_t level) {
    assert(!range_tombstone_iters_.empty() &&
           range_tombstone_iters_[level]->Valid());
    pinned_heap_item_[level * 2].SetTombstoneEndKey(
        range_tombstone_iters_[level]->start_key());
    pinned_heap_item_[level * 2 + 1].SetTombstoneEndKey(
        range_tombstone_iters_[level]->end_key());
    maxHeap_->push(&pinned_heap_item_[level * 2]);
    maxHeap_->push(&pinned_heap_item_[level * 2 + 1]);
  }

  // Remove HeapItems from top of minHeap_ that are of type DELETE_RANGE_START
  // until minHeap_ is empty or the top of the minHeap_ is not of type
  // DELETE_RANGE_START. Each such item means a range tombstone becomes active,
  // so `active_` is updated accordingly.
  void PopDeleteRangeStart() {
    while (!minHeap_.empty() &&
           minHeap_.top()->type == HeapItem::DELETE_RANGE_START) {
      active_.insert(minHeap_.top()->level);
      minHeap_.pop();
    }
  }

  // Remove HeapItems from top of maxHeap_ that are of type DELETE_RANGE_START
  // until minHeap_ is empty or the top of the maxHeap_ is not of type
  // DELETE_RANGE_START. Each such item means a range tombstone becomes active,
  // so `active_` is updated accordingly.
  void PopDeleteRangeEnd() {
    while (!maxHeap_->empty() &&
           maxHeap_->top()->type == HeapItem::DELETE_RANGE_END) {
      active_.insert(maxHeap_->top()->level);
      maxHeap_->pop();
    }
  }

  void SeekToFirst() override {
    ClearHeaps();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.iter.SeekToFirst();
      AddToMinHeapOrCheckStatus(&child);
    }

    for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
      if (range_tombstone_iters_[i]) {
        range_tombstone_iters_[i]->SeekToFirst();
        if (range_tombstone_iters_[i]->Valid()) {
          // It is possible to be invalid due to snapshots.
          InsertRangeTombstoneToMinHeap(i);
        }
      }
    }
    FindNextVisibleEntry();
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void SeekToLast() override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.iter.SeekToLast();
      AddToMaxHeapOrCheckStatus(&child);
    }

    for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
      if (range_tombstone_iters_[i]) {
        range_tombstone_iters_[i]->SeekToLast();
        if (range_tombstone_iters_[i]->Valid()) {
          // It is possible to be invalid due to snapshots.
          InsertRangeTombstoneToMaxHeap(i);
        }
      }
    }
    FindPrevVisibleEntry();
    direction_ = kReverse;
    current_ = CurrentReverse();
  }

  // Position this merging iterator at the first key >= target (internal key).
  // If range tombstones are present, keys covered by range tombstones are
  // skipped, and this merging iter points to the first non-range-deleted key >=
  // target after Seek(). If !Valid() and status().ok() then end of the iterator
  // is reached.
  //
  // Internally, this involves positioning all child iterators at the first key
  // >= target. If range tombstones are present, we apply a similar
  // optimization, cascading seek, as in Pebble
  // (https://github.com/cockroachdb/pebble). Specifically, if there is a range
  // tombstone [start, end) that covers the target user key at level L, then
  // this range tombstone must cover the range [target key, end) in all levels >
  // L. So for all levels > L, we can pretend the target key is `end`. This
  // optimization is applied at each level and hence the name "cascading seek".
  // After a round of (cascading) seeks, the top of the heap is checked to see
  // if it is covered by a range tombstone (see FindNextVisibleEntry() for more
  // detail), and advanced if so. The process is repeated until a visible key is
  // at the top of the heap.
  //
  // As mentioned in comments above HeapItem, to make the checking of whether
  // top of the heap is covered by some range tombstone efficient, we treat each
  // range deletion [start, end) as two point keys and insert them into the same
  // min/maxHeap_ where point iterators are. The set `active_` tracks levels
  // that have active range tombstones. If level L is in `active_`, then the
  // point key at top of the heap is within internal key range of the range
  // tombstone that range_tombstone_iters_[L] currently points to. For
  // correctness reasoning, one invariant that Seek() guarantees is that,
  // suppose current_ is from level L. Then for each range tombstone iterators
  // at level <= L, it is at or before the first range tombstone with end key >
  // current_.key(). This ensures that when a point iterator reaches top of the
  // heap, `active_` is calculated correctly, since no range tombstone iterators
  // are skipped beyond that point iterator doing Seek().
  void Seek(const Slice& target) override {
    assert(range_tombstone_iters_.empty() ||
           range_tombstone_iters_.size() == children_.size());
    SeekImpl(target);
    FindNextVisibleEntry();

    direction_ = kForward;

    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      current_ = CurrentForward();
    }
  }

  void SeekForPrev(const Slice& target) override {
    assert(range_tombstone_iters_.empty() ||
           range_tombstone_iters_.size() == children_.size());
    SeekForPrevImpl(target);
    FindPrevVisibleEntry();

    direction_ = kReverse;
    {
      PERF_TIMER_GUARD(seek_max_heap_time);
      current_ = CurrentReverse();
    }
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current children since current_ is
    // the smallest child and key() == current_->key().
    if (direction_ != kForward) {
      // The loop advanced all non-current children to be > key() so current_
      // should still be strictly the smallest key.
      SwitchToForward();
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentForward());

    // as the current points to the current record. move the iterator forward.
    current_->Next();
    if (current_->Valid()) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      minHeap_.replace_top(minHeap_.top());
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      minHeap_.pop();
    }
    // Heap changed: so we should pop range tombstone starts to bring active_
    // current
    FindNextVisibleEntry();
    current_ = CurrentForward();
  }

  bool NextAndGetResult(IterateResult* result) override {
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->bound_check_result = UpperBoundCheckResult();
      result->value_prepared = current_->IsValuePrepared();
    }
    return is_valid;
  }

  void Prev() override {
    assert(Valid());
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current children since current_ is
    // the largest child and key() == current_->key().
    if (direction_ != kReverse) {
      // Otherwise, retreat the non-current children.  We retreat current_
      // just after the if-block.
      SwitchToBackward();
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentReverse());
    current_->Prev();
    if (current_->Valid()) {
      // current is still valid after the Prev() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      maxHeap_->replace_top(maxHeap_->top());
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      maxHeap_->pop();
    }
    FindPrevVisibleEntry();
    current_ = CurrentReverse();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  bool PrepareValue() override {
    assert(Valid());
    if (current_->PrepareValue()) {
      return true;
    }

    considerStatus(current_->status());
    assert(!status_.ok());
    return false;
  }

  // Here we simply relay MayBeOutOfLowerBound/MayBeOutOfUpperBound result
  // from current child iterator. Potentially as long as one of child iterator
  // report out of bound is not possible, we know current key is within bound.

  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return current_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return current_->UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.iter.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsValuePinned();
  }

 private:
  friend class MergeIteratorBuilder;
  // Clears heaps for both directions, used when changing direction or seeking
  void ClearHeaps();
  // Ensures that maxHeap_ is initialized when starting to go in the reverse
  // direction
  void InitMaxHeap();

  // Advance this merging iterator until the current key (top of min heap) is
  // not covered by any range tombstone or that there is no more keys (heap is
  // empty). After this call, if Valid(), current_ points to the next key that
  // is not covered by any range tombstone.
  void FindNextVisibleEntry();
  void FindPrevVisibleEntry();

  void SeekImpl(const Slice& target, size_t starting_level = 0,
                bool range_tombstone_reseek = false);

  // Seek to fist key <= target key (internal key) for
  // children_[starting_level:].
  void SeekForPrevImpl(const Slice& target, size_t starting_level = 0,
                       bool range_tombstone_reseek = false);

  bool is_arena_mode_;
  bool prefix_seek_mode_;
  // Which direction is the iterator moving?
  enum Direction : uint8_t { kForward, kReverse };
  Direction direction_;
  const InternalKeyComparator* comparator_;
  // We could also use an autovector with a larger reserved size.
  std::vector<HeapItem> children_;
  // HeapItem for range tombstone start and end keys. Each range tombstone
  // iterator will have at most 1 start key and 1 end key HeapItem in the heap
  // at the same time, so this vector will be of size 2 * children_.size();
  // pinned_heap_item_[2*i] and pinned_heap_item_[2*i+1] corresponds to the
  // start key and end key HeapItem from range_tombstone_iters_[i].
  std::vector<HeapItem> pinned_heap_item_;
  // range_tombstone_iters_[i] contains range tombstones in sorted run that
  // corresponds to children_[i]. range_tombstone_iters_.empty() means not
  // handling range tombstones in merging iterator. range_tombstone_iters_[i] ==
  // nullptr means the sorted run of children_[i] does not have range
  // tombstones.
  std::vector<TruncatedRangeDelIterator*> range_tombstone_iters_;

  // Levels (indices into range_tombstone_iters_/children_ ) that currently have
  // active range tombstones. See comments above Seek() for meaning of `active`.
  std::set<size_t> active_;
  // Checks if top of the heap (current key) is covered by a range tombstone by
  // It current key is covered by some range tombstone, its iter is advanced and
  // heap property is maintained. Returns whether top of heap is deleted.
  bool IsNextDeleted();
  bool IsPrevDeleted();

  // Cached pointer to child iterator with the current key, or nullptr if no
  // child iterators are valid.  This is the top of minHeap_ or maxHeap_
  // depending on the direction.
  IteratorWrapper* current_;
  // If any of the children have non-ok status, this is one of them.
  Status status_;
  MergerMinIterHeap minHeap_;

  // Max heap is used for reverse iteration, which is way less common than
  // forward.  Lazily initialize it to save memory.
  std::unique_ptr<MergerMaxIterHeap> maxHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  // In forward direction, process a child that is not in the min heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMinHeapOrCheckStatus(HeapItem*);

  // In backward direction, process a child that is not in the max heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMaxHeapOrCheckStatus(HeapItem*);

  void SwitchToForward();

  // Switch the direction from forward to backward without changing the
  // position. Iterator should still be valid.
  void SwitchToBackward();

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    assert(minHeap_.empty() || minHeap_.top()->type == HeapItem::ITERATOR);
    return !minHeap_.empty() ? &minHeap_.top()->iter : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_);
    assert(maxHeap_->empty() || maxHeap_->top()->type == HeapItem::ITERATOR);
    return !maxHeap_->empty() ? &maxHeap_->top()->iter : nullptr;
  }
};

// Seek to fist key >= target key (internal key) for children_[starting_level:].
// Cascading seek optimizations are applied if range tombstones are present (see
// comment above Seek() for more).
//
// @param range_tombstone_reseek Whether this Seek is to some range tombstone
// end, i.e., a part of a "cascading seek". This is used only for recoding
// relevant perf_context.
void MergingIterator::SeekImpl(const Slice& target, size_t starting_level,
                               bool range_tombstone_reseek) {
  ClearHeaps();
  status_ = Status::OK();
  IterKey current_search_key;
  current_search_key.SetInternalKey(target, false /* copy */);
  // Seek target might change to some range tombstone end key, so
  // we need to remember them for async requests.
  // (level, target) pairs
  autovector<std::pair<size_t, std::string>> prefetched_target;

  for (auto level = starting_level; level < children_.size(); ++level) {
    {
      PERF_TIMER_GUARD(seek_child_seek_time);
      children_[level].iter.Seek(current_search_key.GetInternalKey());
    }

    PERF_COUNTER_ADD(seek_child_seek_count, 1);

    if (!range_tombstone_iters_.empty()) {
      if (range_tombstone_reseek) {
        // This seek is to some range tombstone end key.
        // Should only happen when there are range tombstones.
        PERF_COUNTER_ADD(internal_range_del_reseek_count, 1);
      }
      if (children_[level].iter.status().IsTryAgain()) {
        prefetched_target.emplace_back(
            level, current_search_key.GetInternalKey().ToString());
      }
      auto range_tombstone_iter = range_tombstone_iters_[level];
      if (range_tombstone_iter) {
        range_tombstone_iter->Seek(current_search_key.GetUserKey());
        if (range_tombstone_iter->Valid()) {
          InsertRangeTombstoneToMinHeap(level);
          // current_search_key < end_key guaranteed by the Seek() and Valid()
          // calls above. Only interested in user key coverage since older
          // sorted runs must have smaller sequence numbers than this tombstone.
          //
          // TODO: range_tombstone_iter->Seek() finds the max covering
          //  sequence number, can make it cheaper by not looking for max.
          if (comparator_->user_comparator()->Compare(
                  range_tombstone_iter->start_key().user_key,
                  current_search_key.GetUserKey()) <= 0) {
            // Since range_tombstone_iter->Valid(), seqno should be valid, so
            // there is no need to check it.
            range_tombstone_reseek = true;
            // Current target user key is covered by this range tombstone.
            // All older sorted runs will seek to range tombstone end key.
            // Note that for prefix seek case, it is possible that the prefix
            // is not the same as the original target, it should not affect
            // correctness. Besides, in most cases, range tombstone start and
            // end key should have the same prefix?
            current_search_key.SetInternalKey(
                range_tombstone_iter->end_key().user_key, kMaxSequenceNumber);
          }
        }
      }
    }
    // child.iter.status() is set to Status::TryAgain indicating asynchronous
    // request for retrieval of data blocks has been submitted. So it should
    // return at this point and Seek should be called again to retrieve the
    // requested block and add the child to min heap.
    if (children_[level].iter.status().IsTryAgain()) {
      continue;
    }
    {
      // Strictly, we timed slightly more than min heap operation,
      // but these operations are very cheap.
      PERF_TIMER_GUARD(seek_min_heap_time);
      AddToMinHeapOrCheckStatus(&children_[level]);
    }
  }
  // TODO: perhaps we could save some upheap cost by add all child iters first
  //  and then do a single heapify.
  for (size_t level = 0; level < starting_level; ++level) {
    PERF_TIMER_GUARD(seek_min_heap_time);
    AddToMinHeapOrCheckStatus(&children_[level]);
  }

  if (range_tombstone_iters_.empty()) {
    for (auto& child : children_) {
      if (child.iter.status().IsTryAgain()) {
        child.iter.Seek(target);
        {
          PERF_TIMER_GUARD(seek_min_heap_time);
          AddToMinHeapOrCheckStatus(&child);
        }
        PERF_COUNTER_ADD(number_async_seek, 1);
      }
    }
  } else {
    for (auto& prefetch : prefetched_target) {
      // (level, target) pairs
      children_[prefetch.first].iter.Seek(prefetch.second);
      {
        PERF_TIMER_GUARD(seek_min_heap_time);
        AddToMinHeapOrCheckStatus(&children_[prefetch.first]);
      }
      PERF_COUNTER_ADD(number_async_seek, 1);
    }
    for (size_t level = 0; level < starting_level; ++level) {
      if (range_tombstone_iters_[level] &&
          range_tombstone_iters_[level]->Valid()) {
        InsertRangeTombstoneToMinHeap(level);
      }
    }
  }
}

// Returns true iff the current key (min heap top) should not be returned
// to user (of the merging iterator). This can be because the current key
// is deleted by some range tombstone, the current key is some fake file
// boundary sentinel key, or the current key is an end point of a range
// tombstone. Advance the iterator at heap top if so. Heap order is restored
// and `active_` is updated if needed.
// See FindNextVisibleEntry() for more detail on internal implementation
// of advancing child iters.
//
// REQUIRES:
// - min heap is currently not empty, and iter is in kForward direction.
// - minHeap_ top is not DELETE_RANGE_START (so that active_ is `active_` is
// current).
bool MergingIterator::IsNextDeleted() {
  // 3 types of keys:
  // - point key
  // - file boundary sentinel keys
  // - range deletion end key
  auto current = minHeap_.top();
  if (current->type == HeapItem::DELETE_RANGE_END) {
    minHeap_.pop();
    active_.erase(current->level);
    assert(range_tombstone_iters_[current->level] &&
           range_tombstone_iters_[current->level]->Valid());
    range_tombstone_iters_[current->level]->Next();
    if (range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMinHeap(current->level);
    }
    return true /* current key deleted */;
  }
  if (current->iter.IsDeleteRangeSentinelKey()) {
    // If the file boundary is defined by a range deletion, the range
    // tombstone's end key must come before this sentinel key (see op_type in
    // SetTombstoneEndKey()).
    assert(ExtractValueType(current->iter.key()) != kTypeRangeDeletion ||
           active_.count(current->level) == 0);
    // LevelIterator enters a new SST file
    current->iter.Next();
    if (current->iter.Valid()) {
      assert(current->iter.status().ok());
      minHeap_.replace_top(current);
    } else {
      minHeap_.pop();
    }
    // Remove last SST file's range tombstone end key if there is one.
    // This means file boundary is before range tombstone end key,
    // which could happen when a range tombstone and a user key
    // straddle two SST files. Note that in TruncatedRangeDelIterator,
    // parsed_largest.sequence is decremented 1 in this case.
    if (!minHeap_.empty() && minHeap_.top()->level == current->level &&
        minHeap_.top()->type == HeapItem::DELETE_RANGE_END) {
      minHeap_.pop();
      active_.erase(current->level);
    }
    if (range_tombstone_iters_[current->level] &&
        range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMinHeap(current->level);
    }
    return true /* current key deleted */;
  }
  assert(current->type == HeapItem::ITERATOR);
  // Point key case: check active_ for range tombstone coverage.
  ParsedInternalKey pik;
  ParseInternalKey(current->iter.key(), &pik, false).PermitUncheckedError();
  for (auto& i : active_) {
    if (i < current->level) {
      // range tombstone is from a newer level, definitely covers
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      std::string target;
      AppendInternalKey(&target, range_tombstone_iters_[i]->end_key());
      SeekImpl(target, current->level, true);
      // SeekImpl takes care of PopDeleteRangeStart()
      return true /* current key deleted */;
    } else if (i == current->level) {
      // range tombstone is from the same level as current, check sequence
      // number. By `active_` we know current key is between start key and end
      // key.
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      if (pik.sequence < range_tombstone_iters_[current->level]->seq()) {
        // covered by range tombstone
        current->iter.Next();
        if (current->iter.Valid()) {
          minHeap_.replace_top(current);
        } else {
          minHeap_.pop();
        }
        return true /* current key deleted */;
      } else {
        return false /* current key not deleted */;
      }
    } else {
      return false /* current key not deleted */;
      // range tombstone from an older sorted run with current key < end key.
      // current key is not deleted and the older sorted run will have its range
      // tombstone updated when the range tombstone's end key are popped from
      // minHeap_.
    }
  }
  // we can reach here only if active is empty
  assert(active_.empty());
  assert(minHeap_.top()->type == HeapItem::ITERATOR);
  return false /* current key not deleted */;
}

void MergingIterator::SeekForPrevImpl(const Slice& target,
                                      size_t starting_level,
                                      bool range_tombstone_reseek) {
  ClearHeaps();
  InitMaxHeap();
  status_ = Status::OK();
  IterKey current_search_key;
  current_search_key.SetInternalKey(target, false /* copy */);
  // Seek target might change to some range tombstone end key, so
  // we need to remember them for async requests.
  // (level, target) pairs
  autovector<std::pair<size_t, std::string>> prefetched_target;

  for (auto level = starting_level; level < children_.size(); ++level) {
    {
      PERF_TIMER_GUARD(seek_child_seek_time);
      children_[level].iter.SeekForPrev(current_search_key.GetInternalKey());
    }

    PERF_COUNTER_ADD(seek_child_seek_count, 1);

    if (!range_tombstone_iters_.empty()) {
      if (range_tombstone_reseek) {
        // This seek is to some range tombstone end key.
        // Should only happen when there are range tombstones.
        PERF_COUNTER_ADD(internal_range_del_reseek_count, 1);
      }
      if (children_[level].iter.status().IsTryAgain()) {
        prefetched_target.emplace_back(
            level, current_search_key.GetInternalKey().ToString());
      }
      auto range_tombstone_iter = range_tombstone_iters_[level];
      if (range_tombstone_iter) {
        range_tombstone_iter->SeekForPrev(current_search_key.GetUserKey());
        if (range_tombstone_iter->Valid()) {
          InsertRangeTombstoneToMaxHeap(level);
          // start key <= current_search_key guaranteed by the Seek() call above
          // Only interested in user key coverage since older sorted runs must
          // have smaller sequence numbers than this tombstone.
          if (comparator_->user_comparator()->Compare(
                  current_search_key.GetUserKey(),
                  range_tombstone_iter->end_key().user_key) < 0) {
            range_tombstone_reseek = true;
            // covered by this range tombstone
            current_search_key.SetInternalKey(
                range_tombstone_iter->start_key().user_key, kMaxSequenceNumber,
                kValueTypeForSeekForPrev);
          }
        }
      }
    }
    // child.iter.status() is set to Status::TryAgain indicating asynchronous
    // request for retrieval of data blocks has been submitted. So it should
    // return at this point and Seek should be called again to retrieve the
    // requested block and add the child to min heap.
    if (children_[level].iter.status().IsTryAgain()) {
      continue;
    }
    {
      // Strictly, we timed slightly more than min heap operation,
      // but these operations are very cheap.
      PERF_TIMER_GUARD(seek_max_heap_time);
      AddToMaxHeapOrCheckStatus(&children_[level]);
    }
  }
  for (size_t level = 0; level < starting_level; ++level) {
    PERF_TIMER_GUARD(seek_max_heap_time);
    AddToMaxHeapOrCheckStatus(&children_[level]);
  }

  if (range_tombstone_iters_.empty()) {
    for (auto& child : children_) {
      if (child.iter.status().IsTryAgain()) {
        child.iter.SeekForPrev(target);
        {
          PERF_TIMER_GUARD(seek_min_heap_time);
          AddToMaxHeapOrCheckStatus(&child);
        }
        PERF_COUNTER_ADD(number_async_seek, 1);
      }
    }
  } else {
    for (auto& prefetch : prefetched_target) {
      // (level, target) pairs
      children_[prefetch.first].iter.SeekForPrev(prefetch.second);
      {
        PERF_TIMER_GUARD(seek_max_heap_time);
        AddToMaxHeapOrCheckStatus(&children_[prefetch.first]);
      }
      PERF_COUNTER_ADD(number_async_seek, 1);
    }
    for (size_t level = 0; level < starting_level; ++level) {
      PERF_TIMER_GUARD(seek_max_heap_time);
      if (range_tombstone_iters_[level] &&
          range_tombstone_iters_[level]->Valid()) {
        InsertRangeTombstoneToMaxHeap(level);
      }
    }
  }
}

// See IsNextDeleted().
// REQUIRES:
// - max heap is currently not empty, and iter is in kReverse direction.
// - maxHeap_ top is not DELETE_RANGE_END (so that active_ is `active_` is
// current).
bool MergingIterator::IsPrevDeleted() {
  // 3 types of keys:
  // - point key
  // - file boundary sentinel keys
  // - range deletion end key
  auto current = maxHeap_->top();
  if (current->type == HeapItem::DELETE_RANGE_START) {
    maxHeap_->pop();
    active_.erase(current->level);
    assert(range_tombstone_iters_[current->level] &&
           range_tombstone_iters_[current->level]->Valid());
    range_tombstone_iters_[current->level]->Prev();
    if (range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMaxHeap(current->level);
    }
    return true /* current key deleted */;
  }
  if (current->iter.IsDeleteRangeSentinelKey()) {
    // Different from IsNextDeleted(), range tombstone start key is before file
    // boundary due to op_type set in SetTombstoneEndKey().
    assert(ExtractValueType(current->iter.key()) != kTypeRangeDeletion ||
           active_.count(current->level));
    // LevelIterator enters a new SST file
    current->iter.Prev();
    if (current->iter.Valid()) {
      assert(current->iter.status().ok());
      maxHeap_->replace_top(current);
    } else {
      maxHeap_->pop();
    }
    if (!maxHeap_->empty() && maxHeap_->top()->level == current->level &&
        maxHeap_->top()->type == HeapItem::DELETE_RANGE_START) {
      maxHeap_->pop();
      active_.erase(current->level);
    }
    if (range_tombstone_iters_[current->level] &&
        range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMaxHeap(current->level);
    }
    return true /* current key deleted */;
  }
  assert(current->type == HeapItem::ITERATOR);
  // Point key case: check active_ for range tombstone coverage.
  ParsedInternalKey pik;
  ParseInternalKey(current->iter.key(), &pik, false).PermitUncheckedError();
  for (auto& i : active_) {
    if (i < current->level) {
      // range tombstone is from a newer level, definitely covers
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      std::string target;
      AppendInternalKey(&target, range_tombstone_iters_[i]->start_key());
      // This is different from IsDeleted() which does reseek at sorted runs >=
      // level (instead of i+1 here). With min heap, if level L is at top of the
      // heap, then levels <L all have internal keys > level L's current
      // internal key, which means levels <L are already at a different user
      // key. With max heap, if level L is at top of the heap, then levels <L
      // all have internal keys smaller than level L's current internal key,
      // which might still be the same user key.
      SeekForPrevImpl(target, i + 1, true);
      // SeekImpl takes care of PopDeleteRangeStart()
      return true /* current key deleted */;
    } else if (i == current->level) {
      // By `active_` we know current key is between start key and end key.
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      if (pik.sequence < range_tombstone_iters_[current->level]->seq()) {
        current->iter.Prev();
        if (current->iter.Valid()) {
          maxHeap_->replace_top(current);
        } else {
          maxHeap_->pop();
        }
        return true /* current key deleted */;
      } else {
        return false /* current key not deleted */;
      }
    } else {
      return false /* current key not deleted */;
    }
  }
  assert(active_.empty());
  assert(maxHeap_->top()->type == HeapItem::ITERATOR);
  return false /* current key not deleted */;
}

void MergingIterator::AddToMinHeapOrCheckStatus(HeapItem* child) {
  if (child->iter.Valid()) {
    assert(child->iter.status().ok());
    minHeap_.push(child);
  } else {
    considerStatus(child->iter.status());
  }
}

void MergingIterator::AddToMaxHeapOrCheckStatus(HeapItem* child) {
  if (child->iter.Valid()) {
    assert(child->iter.status().ok());
    maxHeap_->push(child);
  } else {
    considerStatus(child->iter.status());
  }
}

// Advance all non current_ child to > current_.key().
// We advance current_ after the this function call as it does not require
// Seek().
//
// Advance all range tombstones iters, including the one corresponding to
// current_, to the first tombstone with end_key > current_.key() (user key).
// TODO: potentially do cascading seek here too
void MergingIterator::SwitchToForward() {
  ClearHeaps();
  Slice target = key();
  for (auto& child : children_) {
    if (&child.iter != current_) {
      child.iter.Seek(target);
      // child.iter.status() is set to Status::TryAgain indicating asynchronous
      // request for retrieval of data blocks has been submitted. So it should
      // return at this point and Seek should be called again to retrieve the
      // requested block and add the child to min heap.
      if (child.iter.status() == Status::TryAgain()) {
        continue;
      }
      if (child.iter.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.iter.status().ok());
        child.iter.Next();
      }
    }
    AddToMinHeapOrCheckStatus(&child);
  }

  for (auto& child : children_) {
    if (child.iter.status() == Status::TryAgain()) {
      child.iter.Seek(target);
      if (child.iter.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.iter.status().ok());
        child.iter.Next();
      }
      AddToMinHeapOrCheckStatus(&child);
    }
  }

  // Current range tombstone iter also needs to seek for the following case:
  //
  // Previous direction is backward, so range tombstone iter may point to a
  // tombstone before current_. If there is no such tombstone, then the range
  // tombstone is !Valid(). Need to reseek here to make it valid again.
  Slice target_user_key = ExtractUserKey(target);
  for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
    if (range_tombstone_iters_[i]) {
      range_tombstone_iters_[i]->Seek(target_user_key);
      if (range_tombstone_iters_[i]->Valid()) {
        InsertRangeTombstoneToMinHeap(i);
      }
    }
  }

  PopDeleteRangeStart();
  direction_ = kForward;
  assert(current_ == CurrentForward());
}

void MergingIterator::SwitchToBackward() {
  ClearHeaps();
  InitMaxHeap();
  Slice target = key();
  for (auto& child : children_) {
    if (&child.iter != current_) {
      child.iter.SeekForPrev(target);
      TEST_SYNC_POINT_CALLBACK("MergeIterator::Prev:BeforePrev", &child);
      if (child.iter.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.iter.status().ok());
        child.iter.Prev();
      }
    }
    AddToMaxHeapOrCheckStatus(&child);
  }

  ParsedInternalKey pik;
  ParseInternalKey(target, &pik, false /* log_err_key */)
      .PermitUncheckedError();
  for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
    if (range_tombstone_iters_[i]) {
      range_tombstone_iters_[i]->SeekForPrev(pik.user_key);
      if (range_tombstone_iters_[i]->Valid()) {
        // Since the SeekForPrev above is only for user key,
        // we may end up with some range deletions with start key having the
        // same user key at current_, but with a smaller sequence number. This
        // makes current_ not at maxHeap_ top for the CurrentReverse() call
        // below.  n If there is a range tombstone start key with the same user
        // key and the same sequence number as current_, it will be fine as in
        // InsertRangeTombstoneToMaxHeap() we change op_type to be the smallest.
        if (comparator_->Compare(pik, range_tombstone_iters_[i]->start_key()) <
            0) {
          range_tombstone_iters_[i]->Prev();
          if (!range_tombstone_iters_[i]->Valid()) {
            continue;
          }
        }
        InsertRangeTombstoneToMaxHeap(i);
      }
    }
  }
  PopDeleteRangeEnd();
  direction_ = kReverse;
  if (!prefix_seek_mode_) {
    // Note that we don't do assert(current_ == CurrentReverse()) here
    // because it is possible to have some keys larger than the seek-key
    // inserted between Seek() and SeekToLast(), which makes current_ not
    // equal to CurrentReverse().
    current_ = CurrentReverse();
  }
  assert(current_ == CurrentReverse());
  assert(!current_ || maxHeap_->top()->type == HeapItem::ITERATOR);
}

void MergingIterator::ClearHeaps() {
  minHeap_.clear();
  if (maxHeap_) {
    maxHeap_->clear();
  }
  active_.clear();
}

void MergingIterator::InitMaxHeap() {
  if (!maxHeap_) {
    maxHeap_ = std::make_unique<MergerMaxIterHeap>(comparator_);
  }
}

// Repeatedly check and remove heap top key if it is not a point key
// that is not covered by range tombstones. SeekImpl() is called to apply
// cascading if the heap top is a point key covered by some range tombstone from
// a newer sorted run. If the covering tombstone is from current key's level,
// then the current child iterator is simply advanced to its next key without
// reseeking.
inline void MergingIterator::FindNextVisibleEntry() {
  // When active_ is empty, we know heap top cannot be range deletion end key.
  // It cannot be start key per method requirement.
  PopDeleteRangeStart();
  while (!minHeap_.empty() &&
         (!active_.empty() || minHeap_.top()->IsDeleteRangeSentinelKey()) &&
         IsNextDeleted()) {
    PopDeleteRangeStart();
  }
}

inline void MergingIterator::FindPrevVisibleEntry() {
  PopDeleteRangeEnd();
  while (!maxHeap_->empty() &&
         (!active_.empty() || maxHeap_->top()->IsDeleteRangeSentinelKey()) &&
         IsPrevDeleted()) {
    PopDeleteRangeEnd();
  }
}

InternalIterator* NewMergingIterator(const InternalKeyComparator* cmp,
                                     InternalIterator** list, int n,
                                     Arena* arena, bool prefix_seek_mode) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyInternalIterator<Slice>(arena);
  } else if (n == 1) {
    return list[0];
  } else {
    if (arena == nullptr) {
      return new MergingIterator(cmp, list, n, false, prefix_seek_mode);
    } else {
      auto mem = arena->AllocateAligned(sizeof(MergingIterator));
      return new (mem) MergingIterator(cmp, list, n, true, prefix_seek_mode);
    }
  }
}

MergeIteratorBuilder::MergeIteratorBuilder(
    const InternalKeyComparator* comparator, Arena* a, bool prefix_seek_mode)
    : first_iter(nullptr), use_merging_iter(false), arena(a) {
  auto mem = arena->AllocateAligned(sizeof(MergingIterator));
  merge_iter =
      new (mem) MergingIterator(comparator, nullptr, 0, true, prefix_seek_mode);
}

MergeIteratorBuilder::~MergeIteratorBuilder() {
  if (first_iter != nullptr) {
    first_iter->~InternalIterator();
  }
  if (merge_iter != nullptr) {
    merge_iter->~MergingIterator();
  }
}

void MergeIteratorBuilder::AddIterator(InternalIterator* iter) {
  if (!use_merging_iter && first_iter != nullptr) {
    merge_iter->AddIterator(first_iter);
    use_merging_iter = true;
    first_iter = nullptr;
  }
  if (use_merging_iter) {
    merge_iter->AddIterator(iter);
  } else {
    first_iter = iter;
  }
}

void MergeIteratorBuilder::AddRangeTombstoneIterator(
    TruncatedRangeDelIterator* iter, TruncatedRangeDelIterator*** iter_ptr) {
  if (!use_merging_iter) {
    use_merging_iter = true;
    if (first_iter) {
      merge_iter->AddIterator(first_iter);
      first_iter = nullptr;
    }
  }
  merge_iter->AddRangeTombstoneIterator(iter);
  if (iter_ptr) {
    // This is needed instead of set to &range_tombstone_iters_[i] directly
    // here since memory address of range_tombstone_iters_[i] might change
    // during vector resizing.
    range_del_iter_ptrs_.emplace_back(
        merge_iter->range_tombstone_iters_.size() - 1, iter_ptr);
  }
}

InternalIterator* MergeIteratorBuilder::Finish(ArenaWrappedDBIter* db_iter) {
  InternalIterator* ret = nullptr;
  if (!use_merging_iter) {
    ret = first_iter;
    first_iter = nullptr;
  } else {
    for (auto& p : range_del_iter_ptrs_) {
      *(p.second) = &(merge_iter->range_tombstone_iters_[p.first]);
    }
    if (db_iter) {
      assert(!merge_iter->range_tombstone_iters_.empty());
      // memtable is always the first level
      db_iter->SetMemtableRangetombstoneIter(
          &merge_iter->range_tombstone_iters_.front());
    }
    merge_iter->Finish();
    ret = merge_iter;
    merge_iter = nullptr;
  }
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
