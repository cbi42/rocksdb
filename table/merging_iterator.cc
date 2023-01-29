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

namespace ROCKSDB_NAMESPACE {
// MergingIterator uses a min/max heap to combine data from point iterators.
// Range tombstones can be added and keys covered by range tombstones will be
// skipped.
//
// The following are implementation details and can be ignored by user.
// For merging iterator to process range tombstones, it treats the start and end
// keys of a range tombstone as two keys and put them into minHeap_ or maxHeap_
// together with regular point keys. Each range tombstone is active only within
// its internal key range [start_key, end_key). An `active_` set is used to
// track levels that have an active range tombstone. Take forward scanning
// for example. Level j is in active_ if its current range tombstone has its
// start_key popped from minHeap_ and its end_key in minHeap_. If the top of
// minHeap_ is a point key from level L, we can determine if the point key is
// covered by any range tombstone by checking if there is an l <= L in active_.
// The case of l == L also involves checking range tombstone's sequence number.
//
// The following (non-exhaustive) list of invariants are maintained by
// MergingIterator during forward scanning. After each InternalIterator API,
// i.e., Seek*() and Next(), for all levels i, if minHeap_ is not empty:
// 1) current_ = minHeap_.top()
// 2) minHeap_.top().type == ITERATOR
// 3) minHeap_.top()->key() is not covered by any range tombstone.
//
// After each call to SeekImpl() and FindNextVisibleKey() besides public
// InternalIterator APIs:
// 4) children_[i] is in minHeap_ iff children_[i].iter.Valid(), and at most
// one children_[i] is in minHeap_.
// 5) pinned_heap_item_[i] is in minHeap_ iff
// range_tombstone_iters_[i]->Valid() and at most one pinned_heap_item_[i] is
// in minHeap_.
// When referring to range tombstone start/end keys, assume it is the value set
// to HeapItem::tombstone_pik. This value has op_type = kMaxValid, which makes
// range tombstone keys have distinct values from point keys.
// 6) If range_tombstone_iters_[i]->Valid(), pinned_heap_item_[i].tombstone_pik
// is equal to range_tombstone_iters_[i]->start_key() when type is
// DELETE_RANGE_START and range_tombstone_iters_[i]->end_key() when type is
// DELETE_RANGE_END (ignoring op_type which is kMaxValid for all
// pinned_heap_item_).
// 7) heap property is maintained: parent is always <= child in minHeap_.
// The value used for ordering is children_[i].iter.key() for each children_[i],
// and pinned_heap_item_[i].tombstone_pik) for each pinned_heap_item_[i].
//
// Define LevelNextVisible(i, k) to be the first key >= k in level i that is
// not covered by any range tombstone.
// 8) For all j <= i, range_tombstone_iters_[j] is at or before the first
// range tombstone from level j that has end_key() > children_[j].iter.key().
// 9) i is in active_ iff range_tombstone_iters_[i]->Valid() and
// pinned_heap_item_[i].type == DELETE_RANGE_END.
// 10) If i is in active_, range_tombstone_iters_[i]->start_key() <
// children_[j].iter.key() for j >= i.
class MergingIterator : public InternalIterator {
 public:
  MergingIterator(const InternalKeyComparator* comparator,
                  InternalIterator** children, int n, bool is_arena_mode,
                  bool prefix_seek_mode,
                  const Slice* iterate_upper_bound = nullptr)
      : is_arena_mode_(is_arena_mode),
        prefix_seek_mode_(prefix_seek_mode),
        direction_(kForward),
        comparator_(comparator),
        current_(nullptr),
        minHeap_(MinHeapItemComparator(comparator_)),
        pinned_iters_mgr_(nullptr),
        iterate_upper_bound_(iterate_upper_bound) {
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

  // There must be either no range tombstone iterator or the same number of
  // range tombstone iterators as point iterators after all iters are added.
  // The i-th added range tombstone iterator and the i-th point iterator
  // must point to the same LSM level.
  // Merging iterator takes ownership of `iter` and is responsible for freeing
  // it. One exception to this is when a LevelIterator moves to a different SST
  // file or when Iterator::Refresh() is called, the range tombstone iterator
  // could be updated. In that case, this merging iterator is only responsible
  // for freeing the new range tombstone iterator that it has pointers to in
  // range_tombstone_iters_.
  void AddRangeTombstoneIterator(TruncatedRangeDelIterator* iter) {
    range_tombstone_iters_.emplace_back(iter);
  }

  // Called by MergingIteratorBuilder when all point iterators and range
  // tombstone iterators are added. Initializes HeapItems for range tombstone
  // iterators.
  void Finish() {
    if (!range_tombstone_iters_.empty()) {
      assert(range_tombstone_iters_.size() == children_.size());
      pinned_heap_item_.resize(range_tombstone_iters_.size());
      for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
        pinned_heap_item_[i].level = i;
        // Range tombstone end key is exclusive. If a point internal key has the
        // same user key and sequence number as the start or end key of a range
        // tombstone, the order will be start < end key < internal key with the
        // following op_type change. This is helpful to ensure keys popped from
        // heap are in expected order since range tombstone start/end keys will
        // be distinct from point internal keys. Strictly speaking, this is only
        // needed for tombstone end points that are truncated in
        // TruncatedRangeDelIterator since untruncated tombstone end points
        // always have kMaxSequenceNumber and kTypeRangeDeletion (see
        // TruncatedRangeDelIterator::start_key()/end_key()).
        pinned_heap_item_[i].tombstone_pik.type = kTypeMaxValid;
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

  // Add range_tombstone_iters_[level] into min heap.
  // Updates active_ if the end key of a range tombstone is inserted.
  // @param start_key specifies which end point of the range tombstone to add.
  void InsertRangeTombstoneToMinHeap(size_t level, bool start_key = true,
                                     bool replace_top = false) {
    assert(!range_tombstone_iters_.empty() &&
           range_tombstone_iters_[level]->Valid());
    if (start_key) {
      ParsedInternalKey pik = range_tombstone_iters_[level]->start_key();
      // iterate_upper_bound does not have timestamp
      if (iterate_upper_bound_ &&
          comparator_->user_comparator()->CompareWithoutTimestamp(
              pik.user_key, true /* a_has_ts */, *iterate_upper_bound_,
              false /* b_has_ts */) >= 0) {
        if (replace_top) {
          // replace_top implies this range tombstone iterator is still in
          // minHeap_ and at the top.
          minHeap_.pop();
        }
        return;
      }
      pinned_heap_item_[level].SetTombstoneKey(std::move(pik));
      pinned_heap_item_[level].type = HeapItem::Type::DELETE_RANGE_START;
      assert(active_.count(level) == 0);
    } else {
      // allow end key to go over upper bound (if present) since start key is
      // before upper bound and the range tombstone could still cover a
      // range before upper bound.
      pinned_heap_item_[level].SetTombstoneKey(
          range_tombstone_iters_[level]->end_key());
      pinned_heap_item_[level].type = HeapItem::Type::DELETE_RANGE_END;
      active_.insert(level);
    }
    if (replace_top) {
      minHeap_.replace_top(&pinned_heap_item_[level]);
    } else {
      minHeap_.push(&pinned_heap_item_[level]);
    }
  }

  // Add range_tombstone_iters_[level] into max heap.
  // Updates active_ if the start key of a range tombstone is inserted.
  // @param end_key specifies which end point of the range tombstone to add.
  void InsertRangeTombstoneToMaxHeap(size_t level, bool end_key = true,
                                     bool replace_top = false) {
    assert(!range_tombstone_iters_.empty() &&
           range_tombstone_iters_[level]->Valid());
    if (end_key) {
      pinned_heap_item_[level].SetTombstoneKey(
          range_tombstone_iters_[level]->end_key());
      pinned_heap_item_[level].type = HeapItem::Type::DELETE_RANGE_END;
      assert(active_.count(level) == 0);
    } else {
      pinned_heap_item_[level].SetTombstoneKey(
          range_tombstone_iters_[level]->start_key());
      pinned_heap_item_[level].type = HeapItem::Type::DELETE_RANGE_START;
      active_.insert(level);
    }
    if (replace_top) {
      maxHeap_->replace_top(&pinned_heap_item_[level]);
    } else {
      maxHeap_->push(&pinned_heap_item_[level]);
    }
  }

  // Remove HeapItems from top of minHeap_ that are of type DELETE_RANGE_START
  // until minHeap_ is empty or the top of the minHeap_ is not of type
  // DELETE_RANGE_START. Each such item means a range tombstone becomes active,
  // so `active_` is updated accordingly.
  void PopDeleteRangeStart() {
    while (!minHeap_.empty() &&
           minHeap_.top()->type == HeapItem::Type::DELETE_RANGE_START) {
      TEST_SYNC_POINT_CALLBACK("MergeIterator::PopDeleteRangeStart", nullptr);
      // insert end key of this range tombstone and updates active_
      InsertRangeTombstoneToMinHeap(
          minHeap_.top()->level, false /* start_key */, true /* replace_top */);
    }
  }

  // Remove HeapItems from top of maxHeap_ that are of type DELETE_RANGE_END
  // until maxHeap_ is empty or the top of the maxHeap_ is not of type
  // DELETE_RANGE_END. Each such item means a range tombstone becomes active,
  // so `active_` is updated accordingly.
  void PopDeleteRangeEnd() {
    while (!maxHeap_->empty() &&
           maxHeap_->top()->type == HeapItem::Type::DELETE_RANGE_END) {
      // insert start key of this range tombstone and updates active_
      InsertRangeTombstoneToMaxHeap(maxHeap_->top()->level, false /* end_key */,
                                    true /* replace_top */);
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
    FindNextVisibleKey();
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void SeekToLast() override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();
    for (auto &child : children_) {
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
    FindPrevVisibleKey();
    direction_ = kReverse;
    current_ = CurrentReverse();
  }

  // Position this merging iterator at the first key >= target (internal key).
  // If range tombstones are present, keys covered by range tombstones are
  // skipped, and this merging iter points to the first non-range-deleted key >=
  // target after Seek(). If !Valid() and status().ok() then this iterator
  // reaches the end.
  //
  // If range tombstones are present, cascading seeks may be called, an
  // optimization adapted from Pebble (https://github.com/cockroachdb/pebble).
  // Specifically, if there is a range tombstone [start, end) that covers the
  // target user key at level L, then this range tombstone must cover the range
  // [target key, end) in all levels > L. So for all levels > L, we can pretend
  // the target key is `end`. This optimization is applied at each level and
  // hence the name "cascading seek".
  void Seek(const Slice& target) override {
    SeekImpl(target);
    // After SeekImpl(target, 0), invariants (4)-(10) hold.
    // For all level i, target <= children_[i].iter.key() <= LevelNextVisible(i,
    // target). By the contract of FindNextVisibleKey(), Invariants (2)-(10)
    // hold after this call, and minHeap_.top().iter points to
    // NextVisible(target).
    FindNextVisibleKey();

    direction_ = kForward;
    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      // Invariant (1)
      current_ = CurrentForward();
    }
  }

  void SeekForPrev(const Slice &target) override {
    assert(range_tombstone_iters_.empty() ||
           range_tombstone_iters_.size() == children_.size());
    SeekForPrevImpl(target);
    FindPrevVisibleKey();

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
    // true for all the non-current children since current_ is
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
    // Invariants (4)-(10) hold when after advancing curren_,
    // Let k be the smallest key among children_[i].iter.key(),
    // k <= children_[i].iter.key() <= LevelNextVisible(i, k) holds for all
    // level i. After FindNextVisible(), Invariants (2)-(10) hold and
    // minHeap_.top()->key() is the first key >= k from any children_ that is
    // not covered by a range tombstone.
    FindNextVisibleKey();
    current_ = CurrentForward();
  }

  bool NextAndGetResult(IterateResult *result) override {
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
    // true for all the non-current children since current_ is
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
    FindPrevVisibleKey();
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

  void SetPinnedItersMgr(PinnedIteratorsManager *pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto &child : children_) {
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
  // Represents an element in the (min/max) heap used by MergingIterator. Each
  // HeapItem corresponds to a point iterator or a range tombstone iterator,
  // differentiated by HeapItem::Type::type.
  struct HeapItem {
    HeapItem() = default;

    // corresponding point iterator
    IteratorWrapper iter;
    size_t level = 0;
    // corresponding range tombstone iterator's start or end key value
    // depending on value of `type`.
    ParsedInternalKey tombstone_pik;
    // Will be overwritten before use, initialize here so compiler does not
    // complain.
    enum class Type { ITERATOR, DELETE_RANGE_START, DELETE_RANGE_END };
    Type type = Type::ITERATOR;

    explicit HeapItem(size_t _level, InternalIteratorBase<Slice>* _iter)
        : level(_level), type(Type::ITERATOR) {
      iter.Set(_iter);
    }

    void SetTombstoneKey(ParsedInternalKey &&pik) {
      // op_type is already initialized in MergingIterator::Finish().
      tombstone_pik.user_key = pik.user_key;
      tombstone_pik.sequence = pik.sequence;
    }
  };

  class MinHeapItemComparator {
   public:
    explicit MinHeapItemComparator(const InternalKeyComparator* comparator)
        : comparator_(comparator) {}

    bool operator()(HeapItem* a, HeapItem* b) const {
      if (LIKELY(a->type == HeapItem::Type::ITERATOR)) {
        if (LIKELY(b->type == HeapItem::Type::ITERATOR)) {
          return comparator_->Compare(a->iter.key(), b->iter.key()) > 0;
        } else {
          return comparator_->Compare(a->iter.key(), b->tombstone_pik) > 0;
        }
      } else {
        if (LIKELY(b->type == HeapItem::Type::ITERATOR)) {
          return comparator_->Compare(a->tombstone_pik, b->iter.key()) > 0;
        } else {
          return comparator_->Compare(a->tombstone_pik, b->tombstone_pik) > 0;
        }
      }
    }

   private:
    const InternalKeyComparator* comparator_;
  };

  class MaxHeapItemComparator {
   public:
    explicit MaxHeapItemComparator(const InternalKeyComparator* comparator)
        : comparator_(comparator) {}

    bool operator()(HeapItem* a, HeapItem* b) const {
      if (LIKELY(a->type == HeapItem::Type::ITERATOR)) {
        if (LIKELY(b->type == HeapItem::Type::ITERATOR)) {
          return comparator_->Compare(a->iter.key(), b->iter.key()) < 0;
        } else {
          return comparator_->Compare(a->iter.key(), b->tombstone_pik) < 0;
        }
      } else {
        if (LIKELY(b->type == HeapItem::Type::ITERATOR)) {
          return comparator_->Compare(a->tombstone_pik, b->iter.key()) < 0;
        } else {
          return comparator_->Compare(a->tombstone_pik, b->tombstone_pik) < 0;
        }
      }
    }

   private:
    const InternalKeyComparator* comparator_;
  };

  using MergerMinIterHeap = BinaryHeap<HeapItem*, MinHeapItemComparator>;
  using MergerMaxIterHeap = BinaryHeap<HeapItem*, MaxHeapItemComparator>;

  friend class MergeIteratorBuilder;

  // Clears heaps for both directions, used when changing direction or seeking
  void ClearHeaps(bool clear_active = true);

  // Ensures that maxHeap_ is initialized when starting to go in the reverse
  // direction
  void InitMaxHeap();

  // Advance this merging iterator until the current key (minHeap_.top()) is
  // from a point iterator and is not covered by any range tombstone,
  // or that there is no more keys (heap is empty). SeekImpl() may be called
  // to seek to the end of a range tombstone as an optimization.
  void FindNextVisibleKey();

  void FindPrevVisibleKey();

  // Advance this merging iterators to the first key >= `target` for all
  // components from levels >= starting_level. All iterators before
  // starting_level is untouched.
  //
  // @param range_tombstone_reseek Whether target is some range tombstone
  // end, i.e., whether this SeekImpl() call is a part of a "cascading seek".
  // This is used only for recoding relevant perf_context.
  void SeekImpl(const Slice &target, size_t starting_level = 0,
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
  // HeapItem for all child point iterators.
  std::vector<HeapItem> children_;
  // HeapItem for range tombstone start and end keys. Each range tombstone
  // iterator will have at most one side (start key or end key) in a heap
  // at the same time, so this vector will be of size children_.size();
  // pinned_heap_item_[i] corresponds to the start key and end key HeapItem
  // for range_tombstone_iters_[i].
  std::vector<HeapItem> pinned_heap_item_;
  // range_tombstone_iters_[i] contains range tombstones in the sorted run that
  // corresponds to children_[i]. range_tombstone_iters_.empty() means not
  // handling range tombstones in merging iterator. range_tombstone_iters_[i] ==
  // nullptr means the sorted run of children_[i] does not have range
  // tombstones.
  std::vector<TruncatedRangeDelIterator*> range_tombstone_iters_;

  // Levels (indices into range_tombstone_iters_/children_ ) that currently have
  // "active" range tombstones. See comments above Seek() for meaning of
  // "active".
  std::set<size_t> active_;

  bool SkipNextDeleted();

  bool SkipPrevDeleted();

  // Cached pointer to child iterator with the current key, or nullptr if no
  // child iterators are valid.  This is the top of minHeap_ or maxHeap_
  // depending on the direction.
  IteratorWrapper *current_;
  // If any of the children have non-ok status, this is one of them.
  Status status_;
  MergerMinIterHeap minHeap_;

  // Max heap is used for reverse iteration, which is way less common than
  // forward.  Lazily initialize it to save memory.
  std::unique_ptr<MergerMaxIterHeap> maxHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  // Used to bound range tombstones. For point keys, DBIter and SSTable iterator
  // take care of boundary checking.
  const Slice* iterate_upper_bound_;

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
    assert(minHeap_.empty() ||
           minHeap_.top()->type == HeapItem::Type::ITERATOR);
    return !minHeap_.empty() ? &minHeap_.top()->iter : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_);
    assert(maxHeap_->empty() ||
           maxHeap_->top()->type == HeapItem::Type::ITERATOR);
    return !maxHeap_->empty() ? &maxHeap_->top()->iter : nullptr;
  }
};

// Pre-condition:
// - Invariant (5), (6), (9), and (10) hold for i < starting_level
// - For i < starting_level, if i in active_, then
// range_tombstone_iters_[i].start_key() < `target`.
//
// Post-condition:
// - Invariants (4)-(7), (9), and (10) hold for all level i.
// - Invariant (8) holds for starting_level <= j <= i
// - (*) target <= children_[i].iter.key() <= LevelNextVisible(i, target)
// - (**) target < pinned_heap_item_[i].tombstone_pik if
// range_tombstone_iters_[i].Valid()
//
// Proof sketch:
// Invariant (4): minHeap_ is cleared. Each children_[i] is added to minHeap_
// through AddToMinHeapOrCheckStatus() which checks children_[i].iter.Valid().
//
// Invariant (5): minHeap_ is cleared. Each pinned_heap_item_[i] is added to
// minHeap_ after checking range_tombstone_iters_[i]->Valid().
//
// Invariant (6): For i < starting_level, since
// range_tombstone_iters_[i]->Valid(), by Invariant (9), i is in active_ iff
// pinned_heap_item_[i].type == DELETE_RANGE_END. If i in active_,
// pinned_heap_item_[i] is not updated and this follows from the pre-condition
// that (6) holds. If i not in active_, InsertRangeTombstoneToMinHeap(i) is
// called to set type and tombstone_pik. For i >= starting_level,
// InsertRangeTombstoneToMinHeap() is called which maintains (6).
//
// Invariant (7): after minHeap_ is cleared, item is added to heap_ through
// BinaryHeap API push().
//
// Invariant (8) holds for starting_level <= j <= i:
// children_[i].iter.Seek(k1) and range_tombstone_iters_[j]->SeekInternalKey(k2)
// are called in SeekImpl(). Seek(k1) positions children_[i] at the first key >=
// k1 from level i. SeekInternalKey(k2) positions range_tombstone_iters_[j] at
// the first range tombstone from level j with end_key() > k2. It suffices to
// show that k1 >= k2. If `current_search_key` is updated, it is updated to some
// range_tombstone_iter->end_key() after
// range_tombstone_iter->SeekInternalKey(current_search_key) was called. So
// range_tombstone_iter->end_key() > current_search_key. So current_search_key
// increases if updated. So k1 >= k2.
//
// Invariant (9) holds for all level i >= 0.
// For i < starting_level, active_ is checked to decide whether to insert
// DELETE_RANGE_END or DELETE_RANGE_START. So this follows from Pre-condition
// that (9) holds for i < starting_level when the method was called. For i >=
// starting_level, if range_tombstone_iters_[i]->Valid(),
// InsertRangeTombstoneToMinHeap() is called to insert a range tombstone, which
// maintains active_.
//
// Invariant (10) holds for all level >= 0.
// By Pre-condition Invariant (10) holds for i < starting_level.
// Since children_[j] for j < starting level is not changed,
// if i < starting_level and is in active_, then
// range_tombstone_iters_[i]->start_key() <= children_[j].iter.key() for i <= j
// < starting_level. By Pre-condition: for all i < starting_level, if i in
// active_, then range_tombstone_iters_[i].start_key() < target. For j >=
// starting_level, children_[j].iter.Seek(k) is called for k >= target. So
// children_[j].iter.key() >= target > range_tombstone_iters_[i]->start_key()
// for i < starting_level. So variant (10) holds for i < starting_level. For i
// >= starting_level, i is added to active_ only if
// - range_tombstone_iters_[i]->SeekInternalKey(k1) was called
// - range_tombstone_iters_[i]->start_key() <= k1
// children_[j].iter.Seek(k2) is called for some k2 >= k1 and for all j >= i
// So (10) also holds for i >= starting_level.
//
// Post-condition (*): target <= children_[i].iter.key() <= LevelNextVisible(i,
// target) for i >= starting_level.
// target <= children_[i].iter.key() follows from that Seek() is called on some
// key >= target. If `current_search_key` is updated from k1 to k2 when
// level = i, we show that the range [k1, k2) is not visible for children_[j]
// for any j >= i. When `current_search_key` is updated from k1 to k2,
//  - range_tombstone_iters_[i]->SeekInternalKey(k1) was called
//  - range_tombstone_iters_[i]->Valid()
//  - range_tombstone_iters_[i]->start_key().user_key <= k1.user_key
//  - k2 = range_tombstone_iters_[i]->end_key()
// We assume that range_tombstone_iters_[i]->start_key() has a higher sequence
// number compared to any key from levels > i that has the same user key. So no
// point key from levels > i in range [k1, k2) is visible. So
// children_[i].iter.key() <= LevelNextVisible(i, target).
//
// Post-condition (**) target < pinned_heap_item_[i].tombstone_pik for i >=
// starting_level if range_tombstone_iters_[i].Valid(). This follows from
// SeekInternalKey() being called for each range_tombstone_iters_ with some key
// >= `target`.
void MergingIterator::SeekImpl(const Slice& target, size_t starting_level,
                               bool range_tombstone_reseek) {
  // active range tombstones before `starting_level` remain active
  ClearHeaps(false /* clear_active */);
  ParsedInternalKey pik;
  if (!range_tombstone_iters_.empty()) {
    // pik is only used in InsertRangeTombstoneToMinHeap().
    ParseInternalKey(target, &pik, false).PermitUncheckedError();
  }

  // TODO: perhaps we could save some upheap cost by add all child iters first
  //  and then do a single heapify.
  for (size_t level = 0; level < starting_level; ++level) {
    PERF_TIMER_GUARD(seek_min_heap_time);
    AddToMinHeapOrCheckStatus(&children_[level]);
  }
  if (!range_tombstone_iters_.empty()) {
    // Add range tombstones from levels < starting_level. We can insert from
    // pinned_heap_item_ for the following reasons:
    // - pinned_heap_item_[level] is in minHeap_ iff
    // range_tombstone_iters[level]->Valid().
    // - If `level` is in active_, then range_tombstone_iters_[level]->Valid()
    // and pinned_heap_item_[level] is of type RANGE_DELETION_END.
    for (size_t level = 0; level < starting_level; ++level) {
      if (range_tombstone_iters_[level] &&
          range_tombstone_iters_[level]->Valid()) {
        // use an iterator on active_ if performance becomes an issue here
        if (active_.count(level) > 0) {
          assert(pinned_heap_item_[level].type ==
                 HeapItem::Type::DELETE_RANGE_END);
          // if it was active, then start key must be within upper_bound,
          // so we can add to minHeap_ directly.
          minHeap_.push(&pinned_heap_item_[level]);
        } else {
          assert(pinned_heap_item_[level].type ==
                 HeapItem::Type::DELETE_RANGE_START);
          // this takes care of checking iterate_upper_bound, but with an extra
          // key comparison if range_tombstone_iters_[level] was already out of
          // bound. Consider using a new HeapItem type or some flag to remember
          // boundary checking result.
          InsertRangeTombstoneToMinHeap(level);
        }
      } else {
        assert(!active_.count(level));
      }
    }
    // levels >= starting_level will be reseeked below, so clearing their active
    // state here.
    active_.erase(active_.lower_bound(starting_level), active_.end());
  }

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
          // If range tombstone starts after `current_search_key`,
          // we should insert start key to heap as the range tombstone is not
          // active yet.
          InsertRangeTombstoneToMinHeap(
              level, comparator_->Compare(range_tombstone_iter->start_key(),
                                          pik) > 0 /* start_key */);
          // current_search_key < end_key guaranteed by the SeekInternalKey()
          // and Valid() calls above. Here we only need to compare user_key
          // since if target.user_key ==
          // range_tombstone_iter->start_key().user_key and target <
          // range_tombstone_iter->start_key(), no older level would have any
          // key in range [target, range_tombstone_iter->start_key()], so no
          // keys in range [target, range_tombstone_iter->end_key()) from older
          // level would be visible. So it is safe to seek to
          // range_tombstone_iter->end_key().
          //
          // TODO: range_tombstone_iter->Seek() finds the max covering
          //  sequence number, can make it cheaper by not looking for max.
          if (comparator_->user_comparator()->Compare(
                  range_tombstone_iter->start_key().user_key,
                  current_search_key.GetUserKey()) <= 0) {
            range_tombstone_reseek = true;
            // Note that for prefix seek case, it is possible that the prefix
            // is not the same as the original target, it should not affect
            // correctness. Besides, in most cases, range tombstone start and
            // end key should have the same prefix?
            current_search_key.SetInternalKey(range_tombstone_iter->end_key());
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
  }
}

// Returns true iff the current key (min heap top) should not be returned
// to user (of the merging iterator). This can be because the current key
// is deleted by some range tombstone, the current key is some fake file
// boundary sentinel key, or the current key is an end point of a range
// tombstone. Advance the iterator at heap top if needed. Heap order is restored
// and `active_` is updated accordingly.
// See FindNextVisibleKey() for more detail on internal implementation
// of advancing child iters.
//
// REQUIRES:
// - min heap is currently not empty, and iter is in kForward direction.
// - minHeap_ top is not DELETE_RANGE_START (so that `active_` is current).
bool MergingIterator::SkipNextDeleted() {
  // 3 types of keys:
  // - point key
  // - file boundary sentinel keys
  // - range deletion end key
  auto current = minHeap_.top();
  if (current->type == HeapItem::Type::DELETE_RANGE_END) {
    active_.erase(current->level);
    assert(range_tombstone_iters_[current->level] &&
           range_tombstone_iters_[current->level]->Valid());
    range_tombstone_iters_[current->level]->Next();
    if (range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMinHeap(current->level, true /* start_key */,
                                    true /* replace_top */);
    } else {
      minHeap_.pop();
    }
    return true /* current key deleted */;
  }
  if (current->iter.IsDeleteRangeSentinelKey()) {
    // If the file boundary is defined by a range deletion, the range
    // tombstone's end key must come before this sentinel key (see op_type in
    // SetTombstoneKey()).
    assert(ExtractValueType(current->iter.key()) != kTypeRangeDeletion ||
           active_.count(current->level) == 0);
    // When entering a new file, old range tombstone iter is freed,
    // but the last key from that range tombstone iter may still be in the heap.
    // We need to ensure the data underlying its corresponding key Slice is
    // still alive. We do so by popping the range tombstone key from heap before
    // calling iter->Next(). Technically, this change is not needed: if there is
    // a range tombstone end key that is after file boundary sentinel key in
    // minHeap_, the range tombstone end key must have been truncated at file
    // boundary. The underlying data of the range tombstone end key Slice is the
    // SST file's largest internal key stored as file metadata in Version.
    // However, since there are too many implicit assumptions made, it is safer
    // to just ensure range tombstone iter is still alive.
    minHeap_.pop();
    // Remove last SST file's range tombstone end key if there is one.
    // This means file boundary is before range tombstone end key,
    // which could happen when a range tombstone and a user key
    // straddle two SST files. Note that in TruncatedRangeDelIterator
    // constructor, parsed_largest.sequence is decremented 1 in this case.
    if (range_tombstone_iters_[current->level] &&
        range_tombstone_iters_[current->level]->Valid()) {
      // range tombstone end key must immediately follow file boundary sentinel
      assert(!minHeap_.empty() && minHeap_.top()->level == current->level &&
             minHeap_.top()->type == HeapItem::Type::DELETE_RANGE_END);
      minHeap_.pop();
      active_.erase(current->level);
    }
    // LevelIterator enters a new SST file
    current->iter.Next();
    if (current->iter.Valid()) {
      assert(current->iter.status().ok());
      minHeap_.push(current);
    }
    if (range_tombstone_iters_[current->level] &&
        range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMinHeap(current->level);
    }
    return true /* current key deleted */;
  }
  assert(current->type == HeapItem::Type::ITERATOR);
  // Point key case: check active_ for range tombstone coverage.
  ParsedInternalKey pik;
  ParseInternalKey(current->iter.key(), &pik, false).PermitUncheckedError();
  if (!active_.empty()) {
    auto i = *active_.begin();
    if (i < current->level) {
      // range tombstone is from a newer level, definitely covers
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      std::string target;
      AppendInternalKey(&target, range_tombstone_iters_[i]->end_key());
      SeekImpl(target, current->level, true);
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
  // we can reach here only if active_ is empty
  assert(active_.empty());
  assert(minHeap_.top()->type == HeapItem::Type::ITERATOR);
  return false /* current key not deleted */;
}

void MergingIterator::SeekForPrevImpl(const Slice& target,
                                      size_t starting_level,
                                      bool range_tombstone_reseek) {
  // active range tombstones before `starting_level` remain active
  ClearHeaps(false /* clear_active */);
  InitMaxHeap();
  ParsedInternalKey pik;
  if (!range_tombstone_iters_.empty()) {
    ParseInternalKey(target, &pik, false).PermitUncheckedError();
  }
  for (size_t level = 0; level < starting_level; ++level) {
    PERF_TIMER_GUARD(seek_max_heap_time);
    AddToMaxHeapOrCheckStatus(&children_[level]);
  }
  if (!range_tombstone_iters_.empty()) {
    // Add range tombstones before starting_level.
    for (size_t level = 0; level < starting_level; ++level) {
      if (range_tombstone_iters_[level] &&
          range_tombstone_iters_[level]->Valid()) {
        assert(static_cast<bool>(active_.count(level)) ==
               (pinned_heap_item_[level].type ==
                HeapItem::Type::DELETE_RANGE_START));
        maxHeap_->push(&pinned_heap_item_[level]);
      } else {
        assert(!active_.count(level));
      }
    }
    // levels >= starting_level will be reseeked below,
    active_.erase(active_.lower_bound(starting_level), active_.end());
  }

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
          InsertRangeTombstoneToMaxHeap(
              level, comparator_->Compare(range_tombstone_iter->end_key(),
                                          pik) <= 0 /* end_key */);
          // start key <= current_search_key guaranteed by the Seek() call above
          // Only interested in user key coverage since older sorted runs must
          // have smaller sequence numbers than this tombstone.
          if (comparator_->user_comparator()->Compare(
                  current_search_key.GetUserKey(),
                  range_tombstone_iter->end_key().user_key) < 0) {
            range_tombstone_reseek = true;
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
  }
}

// See more in comments above SkipNextDeleted().
// REQUIRES:
// - max heap is currently not empty, and iter is in kReverse direction.
// - maxHeap_ top is not DELETE_RANGE_END (so that `active_` is current).
bool MergingIterator::SkipPrevDeleted() {
  // 3 types of keys:
  // - point key
  // - file boundary sentinel keys
  // - range deletion start key
  auto current = maxHeap_->top();
  if (current->type == HeapItem::Type::DELETE_RANGE_START) {
    active_.erase(current->level);
    assert(range_tombstone_iters_[current->level] &&
           range_tombstone_iters_[current->level]->Valid());
    range_tombstone_iters_[current->level]->Prev();
    if (range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMaxHeap(current->level, true /* end_key */,
                                    true /* replace_top */);
    } else {
      maxHeap_->pop();
    }
    return true /* current key deleted */;
  }
  if (current->iter.IsDeleteRangeSentinelKey()) {
    // LevelIterator enters a new SST file
    maxHeap_->pop();
    // Remove last SST file's range tombstone key if there is one.
    if (!maxHeap_->empty() && maxHeap_->top()->level == current->level &&
        maxHeap_->top()->type == HeapItem::Type::DELETE_RANGE_START) {
      maxHeap_->pop();
      active_.erase(current->level);
    }
    current->iter.Prev();
    if (current->iter.Valid()) {
      assert(current->iter.status().ok());
      maxHeap_->push(current);
    }

    if (range_tombstone_iters_[current->level] &&
        range_tombstone_iters_[current->level]->Valid()) {
      InsertRangeTombstoneToMaxHeap(current->level);
    }
    return true /* current key deleted */;
  }
  assert(current->type == HeapItem::Type::ITERATOR);
  // Point key case: check active_ for range tombstone coverage.
  ParsedInternalKey pik;
  ParseInternalKey(current->iter.key(), &pik, false).PermitUncheckedError();
  if (!active_.empty()) {
    auto i = *active_.begin();
    if (i < current->level) {
      // range tombstone is from a newer level, definitely covers
      assert(comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                  pik) <= 0);
      assert(comparator_->Compare(pik, range_tombstone_iters_[i]->end_key()) <
             0);
      std::string target;
      AppendInternalKey(&target, range_tombstone_iters_[i]->start_key());
      // This is different from SkipNextDeleted() which does reseek at sorted
      // runs >= level (instead of i+1 here). With min heap, if level L is at
      // top of the heap, then levels <L all have internal keys > level L's
      // current internal key, which means levels <L are already at a different
      // user key. With max heap, if level L is at top of the heap, then levels
      // <L all have internal keys smaller than level L's current internal key,
      // which might still be the same user key.
      SeekForPrevImpl(target, i + 1, true);
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
  assert(maxHeap_->top()->type == HeapItem::Type::ITERATOR);
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
// Advance all range tombstones iters, including the one corresponding to
// current_, to the first tombstone with end_key > current_.key().
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
      if (child.iter.Valid() && comparator_->Equal(target, child.iter.key())) {
        assert(child.iter.status().ok());
        child.iter.Next();
      }
    }
    AddToMinHeapOrCheckStatus(&child);
  }

  for (auto& child : children_) {
    if (child.iter.status() == Status::TryAgain()) {
      child.iter.Seek(target);
      if (child.iter.Valid() && comparator_->Equal(target, child.iter.key())) {
        assert(child.iter.status().ok());
        child.iter.Next();
      }
      AddToMinHeapOrCheckStatus(&child);
    }
  }

  // Current range tombstone iter also needs to seek for the following case:
  // Previous direction is backward, so range tombstone iter may point to a
  // tombstone before current_. If there is no such tombstone, then the range
  // tombstone iter is !Valid(). Need to reseek here to make it valid again.
  if (!range_tombstone_iters_.empty()) {
    ParsedInternalKey pik;
    ParseInternalKey(target, &pik, false /* log_err_key */)
        .PermitUncheckedError();
    for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
      auto iter = range_tombstone_iters_[i];
      if (iter) {
        iter->Seek(pik.user_key);
        // The while loop is needed as the Seek() call above is only for user
        // key. We could have a range tombstone with end_key covering user_key,
        // but still is smaller than target. This happens when the range
        // tombstone is truncated at iter.largest_.
        while (iter->Valid() &&
               comparator_->Compare(iter->end_key(), pik) <= 0) {
          iter->Next();
        }
        if (range_tombstone_iters_[i]->Valid()) {
          InsertRangeTombstoneToMinHeap(
              i, comparator_->Compare(range_tombstone_iters_[i]->start_key(),
                                      pik) > 0 /* start_key */);
        }
      }
    }
  }

  direction_ = kForward;
  assert(current_ == CurrentForward());
}

// Advance all range tombstones iters, including the one corresponding to
// current_, to the first tombstone with start_key <= current_.key().
void MergingIterator::SwitchToBackward() {
  ClearHeaps();
  InitMaxHeap();
  Slice target = key();
  for (auto& child : children_) {
    if (&child.iter != current_) {
      child.iter.SeekForPrev(target);
      TEST_SYNC_POINT_CALLBACK("MergeIterator::Prev:BeforePrev", &child);
      if (child.iter.Valid() && comparator_->Equal(target, child.iter.key())) {
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
    auto iter = range_tombstone_iters_[i];
    if (iter) {
      iter->SeekForPrev(pik.user_key);
      // Since the SeekForPrev() call above is only for user key,
      // we may end up with some range tombstone with start key having the
      // same user key at current_, but with a smaller sequence number. This
      // makes current_ not at maxHeap_ top for the CurrentReverse() call
      // below. If there is a range tombstone start key with the same user
      // key and the same sequence number as current_.key(), it will be fine as
      // in InsertRangeTombstoneToMaxHeap() we change op_type to be the smallest
      // op_type.
      while (iter->Valid() &&
             comparator_->Compare(iter->start_key(), pik) > 0) {
        iter->Prev();
      }
      if (iter->Valid()) {
        InsertRangeTombstoneToMaxHeap(
            i, comparator_->Compare(range_tombstone_iters_[i]->end_key(),
                                    pik) <= 0 /* end_key */);
      }
    }
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

void MergingIterator::ClearHeaps(bool clear_active) {
  minHeap_.clear();
  if (maxHeap_) {
    maxHeap_->clear();
  }
  if (clear_active) {
    active_.clear();
  }
}

void MergingIterator::InitMaxHeap() {
  if (!maxHeap_) {
    maxHeap_ =
        std::make_unique<MergerMaxIterHeap>(MaxHeapItemComparator(comparator_));
  }
}

// Assume there is a next visible key.
// Pre-condition:
// - Invariants (4)-(10) hold
// - There is some k where k <= children_[i].iter.key() <= LevelNextVisible(i,
// k) for all levels i
//
// Define NextVisible(k) to be the first key >= k from among children_ that
// is not covered by any range tombstone.
// Post-condition:
// - Invariants (2)-(10) hold
// - minHeap_->top()->key() == NextVisible(k)
//
// Loop invariants:
// - Invariant (4)-(10)
// - (*): k <= children_[i].iter.key() <= LevelNextVisible(i, k)
//
// Progress: minHeap_.top()->key() is non-decreasing and strictly increases in
// a finite number of iterations.
// TODO: it is possible to call SeekImpl(k2) after SeekImpl(k1) with
//  k2 < k1 in the same FindNextVisibleKey(). For example, l1 has a range
//  tombstone [2,3) and l2 has a range tombstone [1, 4). Point key 1 from l5
//  triggers SeekImpl(4 /* target */, 5). Then point key 2 from l3 triggers
//  SeekImpl(3 /* target */, 3).
//  Ideally we should only move iterators forward in SeekImpl(), and the
//  progress condition can be made simpler: iterator only moves forward.
//
// Proof sketch:
// Invariant (2) holds when this method returns:
// Ignoring the empty minHeap_ case, there are two cases:
// Case 1: active_ is empty and !minHeap_.top()->IsDeleteRangeSentinelKey().
// By invariants (5) and (9), active_ being empty means if a
// pinned_heap_item_[i] is in minHeap_, it has type DELETE_RANGE_START. Note
// that PopDeleteRangeStart() was called right before the while loop condition,
// so minHeap_.top() is not of type DELETE_RANGE_START. So minHeap_.top() must
// be of type ITERATOR.
// Case 2: SkipNextDeleted() returns false. The method returns false only when
// minHeap_.top().type == ITERATOR.
//
// Invariant (3) holds when this method returns:
// From Invariant (2), minHeap_.top().type == ITERATOR. Suppose it is
// children_[i] for some i. Suppose that children_[i].iter.key() is covered by
// some range tombstone. This means there is a j <= i and a range tombstone from
// level j with start_key() < children_[i].iter.key() < end_key(). We assume
// that in each level, range tombstones cover non-overlapping ranges. By
// Invariant (8), range_tombstone_iters_[j] is at or before this range
// tombstone. Suppose range_tombstone_iters_[j] is before this range tombstone.
// - If range_tombstone_iters_[j]->Valid(), by Invariant (5),
// pinned_heap_item_[j] is in minHeap_, and pinned_heap_item_[j].tombstone_pik
// is either start or end key value of this range tombstone, which is less than
// children_[i].iter.key(). This key must be popped from minHeap_ before
// children_[i] by Invariant (7) and min heap property.
// - If !range_tombstone_iters_[j]->Valid(), then range_tombstone_iters_[j] is
// at an SST file with all range tombstones from that file exhausted. The file
// is in level j and must come before the file containing the first
// range tombstone with start_key() < children_[i].iter.key() < end_key().
// Assume files have non-overlapping ranges, the current file's meta.largest is
// less than children_[i].iter.key(). So the file boundary key, which has value
// meta.largest must have been processed before children_[i].iter.key(). So
// range_tombstone_iters_[j] would not point to this SST file.
// So we can assume range_tombstone_[j] is **at** the first range tombstone
// with start_key() < children_[i].iter.key() < end_key(). By a similar
// reasoning as above, the range tombstone's end_key is in minHeap_. By
// Invariants (6) and (9), j is in active_. From while loop condition,
// SkipNextDeleted() must have returned false for this method to return.
// If j < i, then SeekImpl(range_tombstone_iters_[j']->end_key(), i) was
// called for some j' < i and children_[i] would be at a key >
// children_[i].iter.key().
// If j == i, children_[i]->Next() would have been called.
// So it is impossible for children_[i].iter.key() to be covered by a range
// tombstone.
//
// Invariant (4) holds after each iteration:
// In i-th iteration, SkipNextDeleted() touches children_ by calling SeekImpl()
// or current->iter.Next(). If it calls SeekImpl(), this follows from
// SeekImpl()'s contract. If it calls current->iter.Next(), then
// current->iter.Valid() is checked and BinaryHeap API replace_top() or pop() is
// called to restore Invariant (4).
//
// Invariant (5) holds after each iteration:
// In PopDeleteRangeStart(), it pops the top item from minHeap_ if it has type
// DELETE_RANGE_START. It then calls InsertRangeTombstoneToMinHeap() to insert
// pinned_heap_item_ of type DELETE_RANGE_END from the same level.
// So (5) holds after each call to PopDeleteRangeStart().
// In SkipNextDeleted():
// - If current->type == DELETE_RANGE_END, the heap item is only removed from
// minHeap_ if !range_tombstone_iters_[current->level]->Valid().
// - We assume that range_tombstone_iters_[i]->start_key() always comes before
// file boundary sentinel key when both are in minHeap_. If
// current->iter.IsDeleteRangeSentinel(), only the last range tombstone's end
// key from the same SST file may still be in minHeap_, and is popped if needed.
// When current->iter enters a new SST file, the range tombstone start key is
// inserted through InsertRangeTombstoneToMinHeap() iff
// range_tombstone_iters_[i]->Valid().
// - If current->type == ITERATOR and SeekImpl() is called, the contract for
// SeekImpl() implies Invariant (5).
// - If current->type == ITERATOR and SeekImpl() is not called,
// pinned_heap_items_ and range_tombstone_iters_ are not updated.
//
// Invariant (6) holds after each iteration:
// All pinned_heap_item_ is updated through InsertRangeTombstoneToMinHeap()
// which updates both type and key.
//
// Invariant (7) holds always: only the heap top's value is updated and in that
// case, BinaryHeap APIs pop() or replace_top() is called to restore heap
// property.
//
// Invariant (8) holds after each iteration:
// PopDeleteRangeStart() does not change range tombstone position.
// In SkipNextDeleted():
//   - If DELETE_RANGE_END is popped from minHeap_, it means the range
//   tombstone's end key is <= all other point keys, so it is safe to advance to
//   next range tombstone.
//   - If file boundary is popped (current->iter.IsDeleteRangeSentinelKey()),
//   we assume that file's last range tombstone's
//   end_key <= file boundary key <= all other point keys. So it is safe to
//   move to the first range tombstone in the next SST file.
//   - If children_[i]->Next() is called, then it is fine as it is advancing a
//   point iterator.
//   - If SeekImpl(k, l) is called, it moves all children_[j] for j >= l to some
//   key >= k > children_[l].key(). Since (8) holds at beginning of the
//   iteration, for all i < l, range_tombstone_iters_[i] is at or before the
//   first range tombstone with end_key() > children_[l].key(). So for i < l
//   and j >= l (8) still hold. Since children_[<l] is not updated in
//   SeekImpl(), (8) holds for all i < l and j >= i.
//   For i >= l, (8) follows from SeekImpl()'s contract.
//
// Invariant (9) holds after each iteration:
// - in PopDeleteRangeStart(), active_ is maintained through
// InsertRangeTombstoneToMinHeap(i), which only inserts i to active_
// when setting pinned_heap_item_[i].type to DELETE_RANGE_END.
// - in SkipNextDeleted():
//   - if the top of heap is DELETE_RANGE_END from level i, active_.erase(i) is
//   called and then range_tombstone_iters_[i] becomes !Valid() or
//   pinned_heap_items_[i].type is set to DELETE_RANGE_START.
//   - if the top of heap is a file boundary sentinel key, a heap item of
//   DELETE_RANGE_END from the same level is popped from the heap if present,
//   with the level removed from active_. Then range_tombstone_iters_[i]
//   becomes !Valid() or pinned_heap_items_[i].type is set to
//   DELETE_RANGE_START.
//   - if the top of heap is point key and SeekImpl() is called, then it
//   follows from contract of SeekImpl().
//   - If current->iter.Next() is called, active_ or range_tombstone_iters_
//   are not touched.
//
// Invariant (10) holds after each iteration:
// A level i is inserted into active_ during calls to PopDeleteRangeStart().
// In that case, range_tombstone_iters_[i].start_key() < all point keys
// by heap property and the assumption that point keys and range tombstone keys
// are distinct.
// If SeekImpl(k, l) is called, then there is a range_tombstone_iters_[j]
// where j < l, j is in active_, k = range_tombstone_iters_[j]->end_key(),
// start_key() < children_[l]->key() < k, and children_[l] was at the top
// of minHeap_. Since (10) holds before the iteration, if i < l in active_,
// range_tombstone_iters_[i]->start_key() < k. SeekImpl(k, l) places
// children_[i] for all i >= l at some key >= k. So (10) holds for i < l. For i
// >= l, it follows from the contract of SeekImpl().
//
// Look Invariant (*): for all level i, k <= children_[i] <= LevelNextVisible(i,
// k).
// k <= children_[i] follows from loop `progress` condition.
// Consider when children_[i] is changed for any i. It is through
// children_[i]->Next() or SeekImpl() in SkipNextDeleted(). If
// children_[i]->Next() is called, there is a range tombstone from level i with
// start_key < children_[i]->key() < end_key and with seqno > children_[i]'s
// sequence number. So children_[i].iter.key() is not visible, and it is safe to
// call Next(). If SeekImpl(target, l) is called, by its contract, target <=
// children_[i]->key() <= LevelNextVisible(i, target) for i >= l. children_[<l]
// is not touched. Since children_[l] was at top of minHeap_, we have
// children_[i]->key() >= children_[l]->key() for i >= l. We also know the range
// [children_[l].iter.key(), target) is not visible for all levels >= l. so
// LevelNextVisible(i, target) = LevelNextVisible(i, k) for i >= l.
//
//
// `Progress` holds for each iteration:
// Very sloppy intuition:
// - in PopDeleteRangeStart(): the value of a pinned_heap_item_.tombstone_pik_
// is updated from the start key to the end key from the same range tombstone.
// We assume that start key <= end key for the same range tombstone.
// - in SkipNextDeleted()
//   - If the top of heap is DELETE_RANGE_END, the range tombstone is advanced
//     and the relevant pinned_heap_item_.tombstone_pik is increased or popped
//     from minHeap_.
//   - If the top of heap is a file boundary key, then both point iter and
//     range tombstone iter are advanced to the next file.
//   - If the top of heap is ITERATOR and current->iter.Next() is called, it
//     moves to a larger point key.
//   - If the top of heap is ITERATOR and SeekImpl(k, l) is called, then all
//     iterators from levels >= l are advanced to some key >= k by its contract.
//     And that top of minHeap_ before SeekImpl(k, l) was less than k.
// There are special cases where different heap items have the same key,
// e.g. when two range tombstone end keys share the same value). In
// these cases, iterators are being advanced, so the minimum key should increase
// in a finite number of steps.
inline void MergingIterator::FindNextVisibleKey() {
  PopDeleteRangeStart();
  // PopDeleteRangeStart() implies heap top is not DELETE_RANGE_START
  // active_ being empty implies no DELETE_RANGE_END in heap.
  // So minHeap_->top() must be of type ITERATOR.
  while (
      !minHeap_.empty() &&
      (!active_.empty() || minHeap_.top()->iter.IsDeleteRangeSentinelKey()) &&
      SkipNextDeleted()) {
    PopDeleteRangeStart();
  }
  // Invariant (2)
  assert(minHeap_.empty() || minHeap_.top()->type == HeapItem::Type::ITERATOR);
}

inline void MergingIterator::FindPrevVisibleKey() {
  PopDeleteRangeEnd();
  // PopDeleteRangeEnd() implies heap top is not DELETE_RANGE_END
  // active_ being empty implies no DELETE_RANGE_START in heap.
  // So maxHeap_->top() must be of type ITERATOR.
  while (
      !maxHeap_->empty() &&
      (!active_.empty() || maxHeap_->top()->iter.IsDeleteRangeSentinelKey()) &&
      SkipPrevDeleted()) {
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
    const InternalKeyComparator* comparator, Arena* a, bool prefix_seek_mode,
    const Slice* iterate_upper_bound)
    : first_iter(nullptr), use_merging_iter(false), arena(a) {
  auto mem = arena->AllocateAligned(sizeof(MergingIterator));
  merge_iter = new (mem) MergingIterator(comparator, nullptr, 0, true,
                                         prefix_seek_mode, iterate_upper_bound);
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

void MergeIteratorBuilder::AddPointAndTombstoneIterator(
    InternalIterator* point_iter, TruncatedRangeDelIterator* tombstone_iter,
    TruncatedRangeDelIterator*** tombstone_iter_ptr) {
  // tombstone_iter_ptr != nullptr means point_iter is a LevelIterator.
  bool add_range_tombstone = tombstone_iter ||
                             !merge_iter->range_tombstone_iters_.empty() ||
                             tombstone_iter_ptr;
  if (!use_merging_iter && (add_range_tombstone || first_iter)) {
    use_merging_iter = true;
    if (first_iter) {
      merge_iter->AddIterator(first_iter);
      first_iter = nullptr;
    }
  }
  if (use_merging_iter) {
    merge_iter->AddIterator(point_iter);
    if (add_range_tombstone) {
      // If there was a gap, fill in nullptr as empty range tombstone iterators.
      while (merge_iter->range_tombstone_iters_.size() <
             merge_iter->children_.size() - 1) {
        merge_iter->AddRangeTombstoneIterator(nullptr);
      }
      merge_iter->AddRangeTombstoneIterator(tombstone_iter);
    }

    if (tombstone_iter_ptr) {
      // This is needed instead of setting to &range_tombstone_iters_[i]
      // directly here since the memory address of range_tombstone_iters_[i]
      // might change during vector resizing.
      range_del_iter_ptrs_.emplace_back(
          merge_iter->range_tombstone_iters_.size() - 1, tombstone_iter_ptr);
    }
  } else {
    first_iter = point_iter;
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
    if (db_iter && !merge_iter->range_tombstone_iters_.empty()) {
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
