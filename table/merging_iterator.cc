//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merging_iterator.h"

#include <iostream>
#include <vector>

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
using MergerMaxIterHeap = BinaryHeap<IteratorWrapper*, MaxIteratorComparator>;
using MergerMinIterHeap = BinaryHeap<IteratorWrapper*, MinIteratorComparator>;
}  // namespace

const size_t kNumIterReserve = 4;

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
      children_[i].Set(children[i]);
    }
  }

  void considerStatus(Status s) {
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  virtual void AddIterator(InternalIterator* iter) {
    children_.emplace_back(iter);
    if (pinned_iters_mgr_) {
      iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
    // Invalidate to ensure `Seek*()` is called to construct the heaps before
    // use.
    current_ = nullptr;
    std::cout << "Add iter. Num range del iter: "
              << children_range_tombstones_.size()
              << ", num iter: " << children_.size() << std::endl;
  }

  /*
   * Add next range tombstone iterator to this merging iterator.
   * There must be either no range tombstone iterator, or same number of
   * range tombstone iterators as point iterators after all range tombstone iters
   * are added. The i-th added range tombstone iterator and the i-th point
   * iterator must point to the same sorted run.
   *
   * Returns FragmentedRangeTombstoneIterator** which points to where
   * this merging iter stores the range tombstone iterator.
   * This is used by level iterators to update range tombtone iterator
   * when reading a different SST partition in the same level.
   */
//  FragmentedRangeTombstoneIterator** AddRangeTombstoneIterator(
//      FragmentedRangeTombstoneIterator* iter) {
//    children_range_tombstones_.emplace_back(iter);
//    std::cout << "Add range del iter. Num range del iter: "
//              << children_range_tombstones_.size()
//              << ", num iter: " << children_.size() << std::endl;
//    return &children_range_tombstones_.back();
//  }

  TruncatedRangeDelIterator** AddRangeTombstoneIterator(
      TruncatedRangeDelIterator* iter) {
    children_range_tombstones_.emplace_back(iter);
    std::cout << "Add Truncated range del iter. Num range del iter: "
              << children_range_tombstones_.size()
              << ", num iter: " << children_.size() << std::endl;
    return &children_range_tombstones_.back();
  }

  ~MergingIterator() override {
    // TODO: a better place for this check
    assert(children_range_tombstones_.empty() ||
           children_range_tombstones_.size() == children_.size());
    // TODO: consider unique_ptr
    for (auto child : children_range_tombstones_) {
      delete child;
    }

    for (auto& child : children_) {
      child.DeleteIter(is_arena_mode_);
    }
    status_.PermitUncheckedError();
  }

  bool Valid() const override { return current_ != nullptr && status_.ok(); }

  Status status() const override { return status_; }

  void SeekToFirst() override {
    ClearHeaps();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.SeekToFirst();
      AddToMinHeapOrCheckStatus(&child);
    }
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void SeekToLast() override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.SeekToLast();
      AddToMaxHeapOrCheckStatus(&child);
    }
    direction_ = kReverse;
    current_ = CurrentReverse();
  }

  /*
   * Position this merging iterator at the first key >= target.
   * Internally, this involves positioned all child iterators at
   * the first key >= target.
   *
   * If range tombstones are provided to this merging iter,
   * we apply a similar technique of cascading seek
   * as in Pebble (https://github.com/cockroachdb/pebble). Specifically, if
   * there is a range tombstone that covers the target key at level L, then we
   * know this range tombstone covers the range [target, range tombstone end)
   * for all levels > L. So for all levels > L, we do seek on the range
   * tombstone end key instead of target. This optimization is applied at each
   * level and hence the name "cascading seek". When Seek() returns, if
   * iter->Valid(), then the merging iter is guaranteed to point to the first
   * key >= target that is not covered by any tombstone. Internall, this is
   * achieved by repeatly checking if the top of the heap is coverd by a range
   * tombstone (see FindNextUserEntry() for more detail). If the top of the heap
   * (current entry) is coverd by some range tombstone, a similar optimized seek
   * is done to range tombstone's end key.
   *
   * TODO: sanity check correctness with snapshot
   * TODO: early stop at upper/lower bound (mergingIter and childIters)
   * TODO: file boundary magic for range tombstones
   * TODO: check range tombestone-free path to minimize regression, say readoptions.ignore_range_tombstone or all empty
   */
  void Seek(const Slice& target) override {
    SeekImpl(target);
    if (!children_range_tombstones_.empty()) {
      // Skip range tombstone covered keys
      FindNextUserEntry();
    }

    direction_ = kForward;

    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      current_ = CurrentForward();
    }
  }

  void SeekForPrev(const Slice& target) override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();

    for (auto& child : children_) {
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.SeekForPrev(target);
      }
      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      {
        PERF_TIMER_GUARD(seek_max_heap_time);
        AddToMaxHeapOrCheckStatus(&child);
      }
    }
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
      SwitchToForward();
      // The loop advanced all non-current children to be > key() so current_
      // should still be strictly the smallest key.
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
      minHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      minHeap_.pop();
    }

    if (!children_range_tombstones_.empty()) {
      FindNextUserEntry();
    }
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
      maxHeap_->replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      maxHeap_->pop();
    }
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
      child.SetPinnedItersMgr(pinned_iters_mgr);
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
  // Clears heaps for both directions, used when changing direction or seeking
  void ClearHeaps();
  // Ensures that maxHeap_ is initialized when starting to go in the reverse
  // direction
  void InitMaxHeap();

  // Skip all entries that are covered by range deletions following merging iter
  // direction. After this call, current_ points to the next user entry if
  // Valid().
  // TODO: currently assuming forward direction and uses minHeap
  void FindNextUserEntry();

  void SeekImpl(const Slice& target, size_t starting_level = 0,
                bool range_tombstone_reseek = false);

  bool is_arena_mode_;
  bool prefix_seek_mode_;
  // Which direction is the iterator moving?
  enum Direction : uint8_t { kForward, kReverse };
  Direction direction_;
  const InternalKeyComparator* comparator_;
  autovector<IteratorWrapper, kNumIterReserve> children_;
  // children_range_tombstones_.empty() means not handling range tombstones in
  // merging iter element == nullptr means a sorted run does not have range
  // deletions
//  autovector<FragmentedRangeTombstoneIterator*> children_range_tombstones_;
  autovector<TruncatedRangeDelIterator*> children_range_tombstones_;
  // Checks if top of the heap (current key) is covered by a range tombstone by
  // restoring invariant up to current key.
  // It current key is covered by some range tombstone,
  // a reseek is done on all levels at or older than current key.
  // Return whether top of heap is deleted.
  bool IsDeleted();

  /*
   * Return the index of child in chidlren_.
   * Assumes child is a member of children_.
   */
  size_t GetChildIndex(IteratorWrapper* child);

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
  void AddToMinHeapOrCheckStatus(IteratorWrapper*);

  // In backward direction, process a child that is not in the max heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMaxHeapOrCheckStatus(IteratorWrapper*);

  void SwitchToForward();

  // Switch the direction from forward to backward without changing the
  // position. Iterator should still be valid.
  void SwitchToBackward();

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_);
    return !maxHeap_->empty() ? maxHeap_->top() : nullptr;
  }
};

// Seek children[starting_lvel:] to >= target
// TODO: in Next(target), potentially return whether seek is done
// to help with seek or next decision in db iter
// TODO: think about if we can disable prefetch?
// TODO: verify correctness for default SeekImpl(target) case against Seek code pre-PR
/*
 * Position children[starting_level:] at first key >= target.
 * See documentation for Seek() for more detail on how the positining works.
 *
 * find_next_entry: whether we need to skip entries covered range tombstones
 * after re-positioning child iters. This is set to false when SeekImpl() is
 * called from FindNextUserEntry() to prevent unnecessary recursion.
 */
void MergingIterator::SeekImpl(const Slice& target, size_t starting_level,
                               bool range_tombstone_reseek) {
  ClearHeaps();
  status_ = Status::OK();
  // auto current_search_key = target;
  IterKey current_search_key;
  current_search_key.SetInternalKey(target, false /* copy */);
  // (level, target) pairs
  autovector<std::pair<size_t, std::string>> pinned_prefetched_target;

  // !find_next_entry means SeekImpl() is called from FindNextUserEntry().
  // So we are doing seek because of a range tombstone covers keys
  // at levels >= starting_level until target key, which is the range tombstone
  // end key. This is used to update
  // perf_context.internal_range_del_reseek_count

  // TODO: consider use size_t for level
  for (auto level = starting_level; level < children_.size(); ++level) {
    {
      // this call is made due to an entry at level L covered by some range
      // tombstone at level < L
      PERF_TIMER_GUARD(seek_child_seek_time);
      std::cerr << "SeekImpl(starting_level=" << starting_level << ") level=" << level << " seek: " << current_search_key.GetInternalKey().ToString() << std::endl;
      children_[level].Seek(current_search_key.GetInternalKey());
    }

    if (range_tombstone_reseek) {
      PERF_COUNTER_ADD(internal_range_del_reseek_count, 1);
    }

    PERF_COUNTER_ADD(seek_child_seek_count, 1);

    if (!children_range_tombstones_.empty() &&
        children_range_tombstones_[level] != nullptr) {
      // Only need seqno > 0 since we are only interested in covering older
      // levels. This MaxCoveringTombstoneSeqnum call also does the seek of the
      // tombstone, i.e. advance the range tombstone iterator to relevant
      // tombstone.
      // TODO: since we only care about seqno > 0, add a cheaper method that
      // does not compute MAX seqno
      children_range_tombstones_[level]->Seek(current_search_key.GetUserKey());

//      if (children_range_tombstones_[level]->MaxCoveringTombstoneSeqnum(
//              current_search_key.GetInternalKey())) {
      if (children_range_tombstones_[level]->Valid() &&
        comparator_->user_comparator()->Compare(children_range_tombstones_[level]->start_key(), current_search_key.GetUserKey()) <= 0 &&
        children_range_tombstones_[level]->seq())
        range_tombstone_reseek = true;
        // covered by this range tombstone
        std::cerr << "In SeekImpl(" << starting_level << ") " << level
                  << "-th child range tombstone iter covers current target: "
                     "Skipping key "
                  << current_search_key.GetUserKey().ToString()
                  << " covered by ["
                  << children_range_tombstones_[level]->start_key().ToString()
                  << ", "
                  << children_range_tombstones_[level]->end_key().ToString()
                  << ") until end of tombstone for seeks into newer levels" << std::endl;
        // TODO: potentially refactor pinned_prefetched_target caching
        if (children_[level].status().IsTryAgain()) {
          pinned_prefetched_target.emplace_back(
              level, current_search_key.GetInternalKey().ToString());
        }
        // all lowers levels are covered by this tombstone, so it is okay to
        //        update seek target to tombstone end key
        current_search_key.SetInternalKey(
            children_range_tombstones_[level]->end_key(), kMaxSequenceNumber);
      }
    }
    // child.status() is set to Status::TryAgain indicating asynchronous
    // request for retrieval of data blocks has been submitted. So it should
    // return at this point and Seek should be called again to retrieve the
    // requested block and add the child to min heap.
    if (children_[level].status().IsTryAgain()) {
      continue;
    }
    {
      // Strictly, we timed slightly more than min heap operation,
      // but these operations are very cheap.
      PERF_TIMER_GUARD(seek_min_heap_time);
      AddToMinHeapOrCheckStatus(&children_[level]);
    }
  }
  for (size_t level = 0; level < starting_level; ++level) {
    PERF_TIMER_GUARD(seek_min_heap_time);
    AddToMinHeapOrCheckStatus(&children_[level]);
  }

  for (auto& prefetch : pinned_prefetched_target /* (level, target) pair */) {
    children_[prefetch.first].Seek(prefetch.second);
    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      AddToMinHeapOrCheckStatus(&children_[prefetch.first]);
    }
    PERF_COUNTER_ADD(number_async_seek, 1);
  }
}

// TODO: heapify -> O(n) compared to O(nlogn)
// Probably not, too few elements
// actually, this can be done for *current* iterator too?

// TODO: sanity check range deletion free path to avoid regression
/*
 * Returns iff the current key (heap top) is deleted.
 * Advance the iter at heap top if so. Heap order is restored.
 * See FindNextUserEntry() for more detail on internal implementation
 * of advancing child iters.
 * 
 * Assumes heap is current not empty.
 */
bool MergingIterator::IsDeleted() {
  // Since we always check Valid() before adding an iter to heap,
  // so we do not need to check current->Valid() here.
  auto current = minHeap_.top();
  auto level = GetChildIndex(current);
  ParsedInternalKey pik;
  // TODO: error handling
  ParseInternalKey(current->key(), &pik, false /* log_error_key */)
      .PermitUncheckedError();

  // For all previous levels:
  //  we can advance their range tombstone iter to after current user key,
  //  since current key is at top of the heap, which means all previous
  //  iters must pointer to a key after current ser key.
  for (size_t i = 0; i <= level; ++i) {
    // current level has no range tombstone left
    if (children_range_tombstones_[i] == nullptr ||
        !children_range_tombstones_[i]->Valid()) {
      continue;
    }

    // TODO: consider timestamp
    // range tombstone end_key <= current user_key
    if (comparator_->user_comparator()->Compare(
      children_range_tombstones_[i]->end_key(),
      // ExtractUserKey(current->key()),
      pik.user_key) <= 0) {
      // advance this iterator
      children_range_tombstones_[i]->SeekToCoveringTombstone(pik.user_key);
    }
    // The above if statements guarantees that current user_key < tombstone end_key
    if (!children_range_tombstones_[i]->Valid()) {
      // Exhausted all range tombstones at i-th level
      continue;
    }

    // current user_key < range tombstone start key
    if (comparator_->user_comparator()->Compare(
      pik.user_key,
      children_range_tombstones_[i]->start_key()) < 0) {
      continue;
    }

    // Now we know start_key <= current user_key < end_key
    // Check sequence number if this is at current level
    if (i == level) {
      children_range_tombstones_[i]->SeekToCoveringSequenceNumber();
    }
    // else: range tombstone from a previous level covers the current key
    // no need to check sequence number
    // TODO: think about some file boundary magic here

    if (!children_range_tombstones_[i]->Valid()) {
      continue;
    }

    if (i < level) {
      std::cerr << "FindNextUserEntry() current heap top: " << current->key().ToString() << " deleted." << std::endl;
      // tombstone->Valid() means there is a valid sequence number
      std::cerr << "Reseeking iterator at level " << level
                << " since its current key " << current->key().ToString()
                << " is covered by tombstone from level " << i << " ["
                << children_range_tombstones_[i]->start_key().ToString()
                << ", "
                << children_range_tombstones_[i]->end_key().ToString()
                << "] at seqno " << children_range_tombstones_[i]->seq()
                << std::endl;
      IterKey seekKey;
      seekKey.SetInternalKey(children_range_tombstones_[i]->end_key(),
                              kMaxSequenceNumber, kValueTypeForSeek);

      SeekImpl(seekKey.GetInternalKey(), level, true /* tombstone_reseek */);
      return true /* entry deleted */;
    } else if (children_range_tombstones_[i]->seq() > pik.sequence) {
      std::cerr << "FindNextUserEntry() current heap top: " << current->key().ToString() << " deleted." << std::endl;

      // TODO: timestamp?
      // TODO: do a fast path iterating here on the current iter?
      // i.e. not to do any heap operation, but simply iterate till end of
      // current range tombstone
      // TODO: optimize this iteration optimization by switching to seek after
      // a certain number of iterations
      std::cerr << "Advancing iterator at level " << level
                << " since its current key " << current->key().ToString()
                << " is covered by tombstone from level " << i << " ["
                << children_range_tombstones_[i]->start_key().ToString()
                << ", " << children_range_tombstones_[i]->end_key().ToString()
                << "] at seqno " << children_range_tombstones_[i]->seq()
                << std::endl;
      current->Next();
      if (current->Valid()) {
        minHeap_.replace_top(current);
      } else {
        considerStatus(current->status());
        minHeap_.pop();
      }
      return true /* entry deleted */;
    }
    // Current level's range tombstone does not cover current user_key,
    // should not have equal sequence number
    assert(children_range_tombstones_[i]->seq() < pik.sequence);
  }
  return false /* not deleted */;
}


size_t MergingIterator::GetChildIndex(IteratorWrapper* child) {
  return child - &children_[0];
}

void MergingIterator::AddToMinHeapOrCheckStatus(IteratorWrapper* child) {
  if (child->Valid()) {
    assert(child->status().ok());
    minHeap_.push(child);
  } else {
    considerStatus(child->status());
  }
}

void MergingIterator::AddToMaxHeapOrCheckStatus(IteratorWrapper* child) {
  if (child->Valid()) {
    assert(child->status().ok());
    maxHeap_->push(child);
  } else {
    considerStatus(child->status());
  }
}

void MergingIterator::SwitchToForward() {
  // Otherwise, advance the non-current children.  We advance current_
  // just after the if-block.
  ClearHeaps();
  Slice target = key();
  for (auto& child : children_) {
    if (&child != current_) {
      child.Seek(target);
      // child.status() is set to Status::TryAgain indicating asynchronous
      // request for retrieval of data blocks has been submitted. So it should
      // return at this point and Seek should be called again to retrieve the
      // requested block and add the child to min heap.
      if (child.status() == Status::TryAgain()) {
        continue;
      }
      if (child.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.status().ok());
        child.Next();
      }
    }
    AddToMinHeapOrCheckStatus(&child);
  }

  for (auto& child : children_) {
    if (child.status() == Status::TryAgain()) {
      child.Seek(target);
      if (child.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.status().ok());
        child.Next();
      }
      AddToMinHeapOrCheckStatus(&child);
    }
  }

  direction_ = kForward;
}

void MergingIterator::SwitchToBackward() {
  ClearHeaps();
  InitMaxHeap();
  Slice target = key();
  for (auto& child : children_) {
    if (&child != current_) {
      child.SeekForPrev(target);
      TEST_SYNC_POINT_CALLBACK("MergeIterator::Prev:BeforePrev", &child);
      if (child.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.status().ok());
        child.Prev();
      }
    }
    AddToMaxHeapOrCheckStatus(&child);
  }
  direction_ = kReverse;
  if (!prefix_seek_mode_) {
    // Note that we don't do assert(current_ == CurrentReverse()) here
    // because it is possible to have some keys larger than the seek-key
    // inserted between Seek() and SeekToLast(), which makes current_ not
    // equal to CurrentReverse().
    current_ = CurrentReverse();
  }
  assert(current_ == CurrentReverse());
}

void MergingIterator::ClearHeaps() {
  minHeap_.clear();
  if (maxHeap_) {
    maxHeap_->clear();
  }
}

void MergingIterator::InitMaxHeap() {
  if (!maxHeap_) {
    maxHeap_.reset(new MergerMaxIterHeap(comparator_));
  }
}

/*
 * Advances this merging iter until the current key (top of the heap) is not covered
 * by any range tombtone or that there is no such key (heap is empty).
 * 
 * Internally, the following process is repeated.
 * For the current key (heap top), range tombstones at levels [0, curent key level] are examined
 * in this order. If a covering tombstone is found from level befre current key,
 * SeekImpl() is called to apply cascading seek from current key's level. If the covering
 * tombstone is from current key's level, then the current child iterator is simply advanved to
 * its next key without calling SeekImpl(). 
 */
void MergingIterator::FindNextUserEntry() {
  // TODO: prefix checking? or just return a invalid one and let db iter decide
  // TODO: upper and lower bound checking
  while (!minHeap_.empty() && IsDeleted()) {
    // move to next entry
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

//FragmentedRangeTombstoneIterator**
//MergeIteratorBuilder::AddRangeTombstoneIterator(
//    FragmentedRangeTombstoneIterator* iter,
//    InternalKey* /* smallest */,
//    InternalKey* /* largest */) {
//  if (!use_merging_iter) {
//    merge_iter->AddIterator(first_iter);
//    use_merging_iter = true;
//    first_iter = nullptr;
//  }
//  return merge_iter->AddRangeTombstoneIterator(iter);
//}

TruncatedRangeDelIterator** MergeIteratorBuilder::AddRangeTombstoneIterator(TruncatedRangeDelIterator* iter) {
  if (!use_merging_iter) {
    merge_iter->AddIterator(first_iter);
    use_merging_iter = true;
    first_iter = nullptr;
  }
  return merge_iter->AddRangeTombstoneIterator(iter);
}

InternalIterator* MergeIteratorBuilder::Finish() {
  InternalIterator* ret = nullptr;
  if (!use_merging_iter) {
    ret = first_iter;
    first_iter = nullptr;
  } else {
    ret = merge_iter;
    merge_iter = nullptr;
  }
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE

//    SeekImpl(target, 0 /* starting_level */, true /* find_next_entry */);
////    for (auto &child_struct: children_structs_) {
////      std::cerr << "Level: " << child_struct.level << std::endl
////        << "Child Iter: " << child_struct.iter.iter() << std::endl
////        << "RangeTombstone: " << child_struct.range_tombstone_iter <<
/// std::endl;
////
////      std::cerr << "children_[i]: " << children_[i].iter() << std::endl
////        << "children_range_tombstones_[i]: " <<
/// children_range_tombstones_[i] << std::endl << std::endl; /    }
//    ClearHeaps();
//    status_ = Status::OK();
//    IterKey current_search_key;
//    current_search_key.SetInternalKey(target, false /* copy */);
//    bool tombstone_reseek = false;
//
//    autovector<std::string> prefetched_target;
//
////    auto range_tombstone = children_range_tombstones_.begin();
////    for (auto& child : children_) {
////    IteratorWrapper &child;
//    for (size_t level = 0; level < children_.size(); ++level) {
//      {
//        PERF_TIMER_GUARD(seek_child_seek_time);
//        children_[level].Seek(current_search_key.GetInternalKey());
//      }
//
//      PERF_COUNTER_ADD(seek_child_seek_count, 1);
//      if (tombstone_reseek) {
//        std::cerr << "In Seek: child " << level << " skipping to end of
//        tombstone" << std::endl;
//        PERF_COUNTER_ADD(internal_range_del_reseek_count, 1);
//      }
//
//      // If current level's tombstone covers target user key, then can seek
//      // until tombstone end key for all older levels
//      // TODO: consider putting child and tombstone iter into a struct
//      if (!children_range_tombstones_.empty() &&
//      children_range_tombstones_[level] != nullptr) {
//        // TODO: more efficient version of MaxCoveringTombstoneSeqnum, since
//        we
//        // do not need sequence number
//
//        // Currently, MaxCoveringTombstoneSeqnum call does the seek
//        auto maxs = children_range_tombstones_[level]
//                ->MaxCoveringTombstoneSeqnum(current_search_key.GetUserKey());
//        if (maxs) {
//          tombstone_reseek = true;
//          // covered by this range tombstone
//          std::cerr << level
//                    << "-th child iter:"
//                       "Skipping key "
//                    << current_search_key.GetUserKey().ToString() << " covered
//                    by ["
//                    <<
//                    children_range_tombstones_[level]->start_key().ToString()
//                    << ", "
//                    << children_range_tombstones_[level]->end_key().ToString()
//                    << ")"
//                    << std::endl;
//          // TODO: potentially refactor this
//          if (children_[level].status().IsTryAgain()) {
//            prefetched_target.emplace_back(current_search_key.GetInternalKey().ToString());
//          }
//          // all lowers levels are covered by this tombstone, so it is okay to
//          update seek target to tombstone end key
//          current_search_key.SetInternalKey(children_range_tombstones_[level]->end_key(),
//          kMaxSequenceNumber);
//        }
//      }
////      range_tombstone++;
////      i++;
//      // child.status() is set to Status::TryAgain indicating asynchronous
//      // request for retrieval of data blocks has been submitted. So it should
//      // return at this point and Seek should be called again to retrieve the
//      // requested block and add the child to min heap.
//      if (children_[level].status() == Status::TryAgain()) {
//        continue;
//      }
//      {
//        // Strictly, we timed slightly more than min heap operation,
//        // but these operations are very cheap.
//        PERF_TIMER_GUARD(seek_min_heap_time);
//        AddToMinHeapOrCheckStatus(&children_[level]);
//      }
//    }
//
//    auto t = prefetched_target.begin();
//    for (auto& child : children_) {
//      if (child.status() == Status::TryAgain()) {
//        child.Seek(*t);
//        t++;
//        {
//          PERF_TIMER_GUARD(seek_min_heap_time);
//          AddToMinHeapOrCheckStatus(&child);
//        }
//        PERF_COUNTER_ADD(number_async_seek, 1);
//      }
//    }
//
//    direction_ = kForward;
//
//    // Skip range tombstone covered keys
//    FindNextUserEntry();
//
//    // TODO: the following block can be deleted
//    {
//      PERF_TIMER_GUARD(seek_min_heap_time);
//      current_ = CurrentForward();
//    }