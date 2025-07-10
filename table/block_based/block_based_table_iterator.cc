//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/block_based_table_iterator.h"

#include <iostream>

namespace ROCKSDB_NAMESPACE {

struct MultiScanPrefetcher {
  // Prefetch([offset, size])
  Status Prefetch(std::vector<FSReadRequest>& read_reqs, const IOOptions& opts,
                  RandomAccessFileReader* reader) {
    (void)read_reqs;
    AlignedBuf aligned_buf;
    IODebugContext dbg;
    bool direct_io = reader->use_direct_io();
    IOStatus s = reader->MultiRead(opts, read_reqs.data(), read_reqs.size(),
                                   direct_io ? &aligned_buf : nullptr, &dbg);
    if (!s.ok()) {
      return s;
    }
    for (auto& req : read_reqs) {
      if (!req.status.ok()) {
        return req.status;
      }
    }

    return Status::OK();
  }

  // PrefetchAsync([offset, size])

  // GetBlockContent(offset, size)
};

void BlockBasedTableIterator::SeekToFirst() { SeekImpl(nullptr, false); }

void BlockBasedTableIterator::Seek(const Slice& target) {
  SeekImpl(&target, true);
}

void BlockBasedTableIterator::SeekSecondPass(const Slice* target) {
  AsyncInitDataBlock(/*is_first_pass=*/false);

  if (target) {
    block_iter_.Seek(*target);
  } else {
    block_iter_.SeekToFirst();
  }
  FindKeyForward();

  CheckOutOfBound();

  if (target) {
    assert(!Valid() || icomp_.Compare(*target, key()) <= 0);
  }
}

void BlockBasedTableIterator::SeekImpl(const Slice* target,
                                       bool async_prefetch) {
  // ----------------
  // Init prefix from PrefixExtractor(target)
  // -----------------
  //
  // TODO(hx235): set `seek_key_prefix_for_readahead_trimming_`
  // even when `target == nullptr` that is when `SeekToFirst()` is called
  // prepared_ means specialized iterator for multi-scan where all data blocks
  // are pre-loaded
  std::cout << "SeekImpl target: " << (target ? target->ToString() : "nullptr")
            << std::endl;
  if (prepared_) {
    // TODO: GC pinned data blocks when !prepared_
    // TODO: do we do internal seek from DBiter?? maybe, tombstone...
    //
    // TODO: sanity check multiscan on not supported iterator options:
    //   - prefix extractor
    //   - iterate_upper_bound??
    // Validate seek key with scan options
    if (next_multi_scan_idx_ >= scan_opts_->size()) {
      // TODO: idx tracking may not work due to tombstone seek from DBIter
      prepared_ = false;
    } else if (!target) {
      // start key must be set for multi-scan
      prepared_ = false;
    } else if (user_comparator_.Compare(
                   ExtractUserKey(*target),
                   (*scan_opts_)[next_multi_scan_idx_].range.start.value()) !=
               0) {
      prepared_ = false;
    } else {
      is_out_of_bound_ = false;
      // we should have the data block
      //
      // Seek index to get it from map
      // cur_data_block_idx_ = starting_block_per_scan_[next_multi_scan_idx_];
      std::tie(cur_scan_start_idx_, cur_scan_end_idx_) =
          block_ranges_per_scan_[next_multi_scan_idx_];
      ++next_multi_scan_idx_;
      // if (cur_data_block_idx_ < 0) {
      if (cur_scan_start_idx_ >= cur_scan_end_idx_) {
        // TODO: check if this is fine
        is_out_of_bound_ = true;
        // std::cout << "SeekImpl served from prepared blocks " <<
        // cur_data_block_idx_ << std::endl;
        std::cout << "SeekImpl served from prepared blocks, found empty scan "
                     "range: start idx "
                  << cur_scan_start_idx_ << " end idx" << cur_scan_end_idx_
                  << std::endl;
        // ResetDataIter();
        // TODO: cur iter should be Invalid
        return;
      }

      // TODO: maintain state block_iter_points_to_real_block_
      // ResetDataIter();
      // First scan or different block, need to reinit block iter
      if (next_multi_scan_idx_ == 1 ||
          cur_data_block_idx_ != cur_scan_start_idx_) {
        assert(next_multi_scan_idx_ == 1 ||
               cur_data_block_idx_ < cur_scan_start_idx_);
        // ResetDataIter
        ResetDataIter();
        table_->NewDataBlockIterator<DataBlockIter>(
            read_options_,  // TODO: the function calls transfer to
                            // data_block_iter.... how to keep the block pinned?
            multiscan_pinned_data_blocks_[cur_scan_start_idx_], &block_iter_,
            Status::OK());
      }
      cur_data_block_idx_ = cur_scan_start_idx_;
      block_iter_points_to_real_block_ = true;

      block_iter_.Seek(*target);
      if (!block_iter_.Valid()) {
        FindBlockForward();
      }
      // if (!block_iter_.Valid()) {
      //   do {
      //     if (!block_iter_.status().ok()) {
      //       return;
      //     }
      //     ResetDataIter();
      //     cur_data_block_idx_++;
      //     if (cur_data_block_idx_ >= multiscan_pinned_data_blocks_.size()) {
      //       // TODO: document that -1 means cur BBTI is invalid, but status
      //       ok cur_data_block_idx_ = -1; return;
      //     }
      //     table_->NewDataBlockIterator<DataBlockIter>(read_options_,
      //       multiscan_pinned_data_blocks_[cur_data_block_idx_], &block_iter_,
      //       Status::OK());
      //     block_iter_.Seek(*target);
      //   } while (!block_iter_.Valid());
      // }
      // CheckOutOfBound();
      std::cout << "SeekImpl served from prepared blocks "
                << cur_data_block_idx_ << std::endl;
      return;
    }
  }

  assert(!prepared_);

  if (target != nullptr && prefix_extractor_ &&
      read_options_.prefix_same_as_start) {
    const Slice& seek_user_key = ExtractUserKey(*target);
    seek_key_prefix_for_readahead_trimming_ =
        prefix_extractor_->InDomain(seek_user_key)
            ? prefix_extractor_->Transform(seek_user_key).ToString()
            : "";
  }

  // ----------------
  // 2nd pass for async read
  bool is_first_pass = !async_read_in_progress_;

  if (!is_first_pass) {
    SeekSecondPass(target);
    return;
  }
  // ------------------

  ResetBlockCacheLookupVar();

  bool autotune_readaheadsize =
      read_options_.auto_readahead_size &&
      (read_options_.iterate_upper_bound || read_options_.prefix_same_as_start);

  if (autotune_readaheadsize &&
      table_->get_rep()->table_options.block_cache.get() &&
      direction_ == IterDirection::kForward) {
    readahead_cache_lookup_ = true;
  }

  // is_out_of_bound_ = false; // Set in ResetBlockCacheLookupVar
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;
  bool filter_checked = false;
  // -------------------------
  // Prefix filter check
  if (target &&
      !CheckPrefixMayMatch(*target, IterDirection::kForward, &filter_checked)) {
    // TODO: ResetDataIter was already called inside CheckPrefixMayMatch
    // resets and clears current data block (or delegate to pinning iter
    // manager)
    ResetDataIter();
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_FILTERED
                                            : NON_LAST_LEVEL_SEEK_FILTERED);
    return;
  }
  if (filter_checked) {
    seek_stat_state_ = kFilterUsed;
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_FILTER_MATCH
                                            : NON_LAST_LEVEL_SEEK_FILTER_MATCH);
  }
  // -------------------------

  bool need_seek_index = true;

  // ------------------
  // Position index block
  //
  //
  //  In case of readahead_cache_lookup_, index_iter_ could change to find the
  //  readahead size in BlockCacheLookupForReadAheadSize so it needs to
  //  reseek.
  if (IsIndexAtCurr() && block_iter_points_to_real_block_ &&
      block_iter_.Valid()) {
    // Reseek.
    prev_block_offset_ = index_iter_->value().handle.offset();

    if (target) {
      // TODO: why not use internal key?
      //  block iter and index iter should all use internal key
      // We can avoid an index seek if:
      // 1. The new seek key is larger than the current key
      // 2. The new seek key is within the upper bound of the block
      // Since we don't necessarily know the internal key for either
      // the current key or the upper bound, we check user keys and
      // exclude the equality case. Considering internal keys can
      // improve for the boundary cases, but it would complicate the
      // code.
      if (user_comparator_.Compare(ExtractUserKey(*target),
                                   block_iter_.user_key()) > 0 &&
          user_comparator_.Compare(ExtractUserKey(*target),
                                   index_iter_->user_key()) < 0) {
        need_seek_index = false;
      }
    }
  }

  if (need_seek_index) {
    if (target) {
      index_iter_->Seek(*target);
    } else {
      index_iter_->SeekToFirst();
    }
    is_index_at_curr_block_ = true;
    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  // ---------------
  // index block is positioned at the right data block handle
  // Now we need to load datablock (if neede), and position it

  // After reseek, index_iter_ point to the right key i.e. target in
  // case of readahead_cache_lookup_. So index_iter_ can be used directly.
  IndexValue v = index_iter_->value();
  const bool same_block = block_iter_points_to_real_block_ &&
                          v.handle.offset() == prev_block_offset_;

  if (!v.first_internal_key.empty() && !same_block &&
      (!target || icomp_.Compare(*target, v.first_internal_key) <= 0) &&
      allow_unprepared_value_) {
    // ----------------------
    // Positioned at the first key in the block
    //
    // Index contains the first key of the block, and it's >= target.
    // We can defer reading the block.
    is_at_first_key_from_index_ = true;
    // ResetDataIter() will invalidate block_iter_. Thus, there is no need to
    // call CheckDataBlockWithinUpperBound() to check for iterate_upper_bound
    // as that will be done later when the data block is actually read.
    ResetDataIter();
  } else {
    // -----------------
    // Load data block
    //
    // Need to use the data block.
    if (!same_block) {
      if (read_options_.async_io && async_prefetch) {
        // Async load the data block
        // => In second pass, we are at the step of having data block, and need
        // to positioning within the data block
        AsyncInitDataBlock(/*is_first_pass=*/true);
        if (async_read_in_progress_) {
          // Status::TryAgain indicates asynchronous request for retrieval of
          // data blocks has been submitted. So it should return at this point
          // and Seek should be called again to retrieve the requested block
          // and execute the remaining code.
          return;
        }
      } else {
        InitDataBlock();
      }
    } else {
      // When the user does a reseek, the iterate_upper_bound might have
      // changed. CheckDataBlockWithinUpperBound() needs to be called
      // explicitly if the reseek ends up in the same data block.
      // If the reseek ends up in a different block, InitDataBlock() will do
      // the iterator upper bound check.
      CheckDataBlockWithinUpperBound();
    }

    // Position within the data block
    if (target) {
      block_iter_.Seek(*target);
    } else {
      block_iter_.SeekToFirst();
    }
    FindKeyForward();
  }

  CheckOutOfBound();

  if (target) {
    assert(!Valid() || icomp_.Compare(*target, key()) <= 0);
  }
}

void BlockBasedTableIterator::SeekForPrev(const Slice& target) {
  direction_ = IterDirection::kBackward;
  ResetBlockCacheLookupVar();
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;
  bool filter_checked = false;
  // For now totally disable prefix seek in auto prefix mode because we don't
  // have logic
  if (!CheckPrefixMayMatch(target, IterDirection::kBackward, &filter_checked)) {
    ResetDataIter();
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_FILTERED
                                            : NON_LAST_LEVEL_SEEK_FILTERED);
    return;
  }
  if (filter_checked) {
    seek_stat_state_ = kFilterUsed;
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_FILTER_MATCH
                                            : NON_LAST_LEVEL_SEEK_FILTER_MATCH);
  }

  SavePrevIndexValue();

  // Call Seek() rather than SeekForPrev() in the index block, because the
  // target data block will likely to contain the position for `target`, the
  // same as Seek(), rather than than before.
  // For example, if we have three data blocks, each containing two keys:
  //   [2, 4]  [6, 8] [10, 12]
  //  (the keys in the index block would be [4, 8, 12])
  // and the user calls SeekForPrev(7), we need to go to the second block,
  // just like if they call Seek(7).
  // The only case where the block is difference is when they seek to a position
  // in the boundary. For example, if they SeekForPrev(5), we should go to the
  // first block, rather than the second. However, we don't have the information
  // to distinguish the two unless we read the second block. In this case, we'll
  // end up with reading two blocks.
  index_iter_->Seek(target);
  is_index_at_curr_block_ = true;

  if (!index_iter_->Valid()) {
    auto seek_status = index_iter_->status();
    // Check for IO error
    if (!seek_status.IsNotFound() && !seek_status.ok()) {
      ResetDataIter();
      return;
    }

    // With prefix index, Seek() returns NotFound if the prefix doesn't exist
    if (seek_status.IsNotFound()) {
      // Any key less than the target is fine for prefix seek
      ResetDataIter();
      return;
    } else {
      index_iter_->SeekToLast();
    }
    // Check for IO error
    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  InitDataBlock();

  block_iter_.SeekForPrev(target);

  FindKeyBackward();
  CheckDataBlockWithinUpperBound();
  assert(!block_iter_.Valid() ||
         icomp_.Compare(target, block_iter_.key()) >= 0);
}

void BlockBasedTableIterator::SeekToLast() {
  direction_ = IterDirection::kBackward;
  ResetBlockCacheLookupVar();
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;

  SavePrevIndexValue();

  index_iter_->SeekToLast();
  is_index_at_curr_block_ = true;

  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }

  InitDataBlock();
  block_iter_.SeekToLast();
  FindKeyBackward();
  CheckDataBlockWithinUpperBound();
}

void BlockBasedTableIterator::Next() {
  assert(Valid());
  if (is_at_first_key_from_index_ && !MaterializeCurrentBlock()) {
    assert(!prepared_);
    return;
  }
  assert(block_iter_points_to_real_block_);
  block_iter_.Next();
  // During next, we only call InitDataBlock()... that still does async
  // prefetching?
  FindKeyForward();
  CheckOutOfBound();
}

bool BlockBasedTableIterator::NextAndGetResult(IterateResult* result) {
  Next();
  bool is_valid = Valid();
  if (is_valid) {
    result->key = key();
    result->bound_check_result = UpperBoundCheckResult();
    result->value_prepared = !is_at_first_key_from_index_;
  }
  return is_valid;
}

void BlockBasedTableIterator::Prev() {
  if (readahead_cache_lookup_ && !IsIndexAtCurr()) {
    // In case of readahead_cache_lookup_, index_iter_ has moved forward. So we
    // need to reseek the index_iter_ to point to current block by using
    // block_iter_'s key.
    if (Valid()) {
      ResetBlockCacheLookupVar();
      direction_ = IterDirection::kBackward;
      Slice last_key = key();

      index_iter_->Seek(last_key);
      is_index_at_curr_block_ = true;

      // Check for IO error.
      if (!index_iter_->Valid()) {
        ResetDataIter();
        return;
      }
    }

    if (!Valid()) {
      ResetDataIter();
      return;
    }
  }

  ResetBlockCacheLookupVar();
  if (is_at_first_key_from_index_) {
    is_at_first_key_from_index_ = false;

    index_iter_->Prev();
    if (!index_iter_->Valid()) {
      return;
    }

    InitDataBlock();
    block_iter_.SeekToLast();
  } else {
    assert(block_iter_points_to_real_block_);
    block_iter_.Prev();
  }

  FindKeyBackward();
}

// Load the block currently pointed to by index_iter_
// Unless we are doing some readahead?
//    - when
void BlockBasedTableIterator::InitDataBlock() {
  BlockHandle data_block_handle;
  bool is_in_cache = false;
  bool use_block_cache_for_lookup = true;

  if (DoesContainBlockHandles()) {  // TODO when is this true?
    data_block_handle = block_handles_->front().handle_;
    is_in_cache = block_handles_->front().is_cache_hit_;
    use_block_cache_for_lookup = false;
  } else {
    // Reading the data block currently pointed to by index iter
    data_block_handle = index_iter_->value().handle;
  }

  if (!block_iter_points_to_real_block_ ||
      data_block_handle.offset() != prev_block_offset_ ||
      // TODO: what does incomplete mean here?
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetDataIter();
    }

    // ----------------------------------------
    // - Cache look up
    // ----------------------------------------
    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;

    // Initialize Data Block From CacheableEntry.
    if (is_in_cache) {
      Status s;
      // Note that invalidate clears the underlying block content when it can
      block_iter_.Invalidate(Status::OK());
      // TODO: passing non-ok status for a new block iter feels weird here
      // TODO: who owns/pins the block?
      // ----------------------------------------
      // - Bulid iterator from already cached block
      // ----------------------------------------
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_,
          // (block_handles_->front().cachable_entry_).As<Block>(),
          block_handles_->front().cachable_entry_, &block_iter_, s);
    } else {
      auto* rep = table_->get_rep();

      std::function<void(bool, uint64_t&, uint64_t&)> readaheadsize_cb =
          nullptr;
      // Set during BBIter::Seek()
      if (readahead_cache_lookup_) {
        readaheadsize_cb = std::bind(
            &BlockBasedTableIterator::BlockCacheLookupForReadAheadSize, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3);
      }

      // ----------------------------------------
      // Prefetching logic
      // Block read logic here

      // Prefetch additional data for range scans (iterators).
      // Implicit auto readahead:
      //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
      // Explicit user requested readahead:
      //   Enabled from the very first IO when ReadOptions.readahead_size is
      //   set.
      block_prefetcher_.PrefetchIfNeeded(
          rep, data_block_handle, read_options_.readahead_size,
          is_for_compaction,
          /*no_sequential_checking=*/false, read_options_, readaheadsize_cb,
          read_options_.async_io);

      Status s;
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, data_block_handle, &block_iter_, BlockType::kData,
          /*get_context=*/nullptr, &lookup_context_,
          block_prefetcher_.prefetch_buffer(),
          /*for_compaction=*/is_for_compaction, /*async_read=*/false, s,
          use_block_cache_for_lookup);
    }
    block_iter_points_to_real_block_ = true;

    CheckDataBlockWithinUpperBound();

    // --------------
    // Seek Stats update
    if (!is_for_compaction &&
        (seek_stat_state_ & kDataBlockReadSinceLastSeek) == 0) {
      RecordTick(table_->GetStatistics(), is_last_level_
                                              ? LAST_LEVEL_SEEK_DATA
                                              : NON_LAST_LEVEL_SEEK_DATA);
      seek_stat_state_ = static_cast<SeekStatState>(
          seek_stat_state_ | kDataBlockReadSinceLastSeek | kReportOnUseful);
    }
  }
}

// Async init the block pointed to by index_iter_
void BlockBasedTableIterator::AsyncInitDataBlock(bool is_first_pass) {
  BlockHandle data_block_handle;
  bool is_for_compaction =
      lookup_context_.caller == TableReaderCaller::kCompaction;
  if (is_first_pass) {
    data_block_handle = index_iter_->value().handle;
    if (!block_iter_points_to_real_block_ ||
        data_block_handle.offset() != prev_block_offset_ ||
        // if previous attempt of reading the block missed cache, try again
        // -------- Where can we get incomplete????
        block_iter_.status().IsIncomplete()) {
      if (block_iter_points_to_real_block_) {
        ResetDataIter();
      }
      auto* rep = table_->get_rep();

      // TODO: read callback usage
      std::function<void(bool, uint64_t&, uint64_t&)> readaheadsize_cb =
          nullptr;
      if (readahead_cache_lookup_) {
        readaheadsize_cb = std::bind(
            &BlockBasedTableIterator::BlockCacheLookupForReadAheadSize, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3);
      }

      // Prefetch additional data for range scans (iterators).
      // Implicit auto readahead:
      //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
      // Explicit user requested readahead:
      //   Enabled from the very first IO when ReadOptions.readahead_size is
      //   set.
      // In case of async_io with Implicit readahead, block_prefetcher_ will
      // always the create the prefetch buffer by setting no_sequential_checking
      // = true.
      // TODO: what is no_sequential_checking
      block_prefetcher_.PrefetchIfNeeded(
          rep, data_block_handle, read_options_.readahead_size,
          is_for_compaction, /*no_sequential_checking=*/read_options_.async_io,
          read_options_, readaheadsize_cb, read_options_.async_io);

      Status s;
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, data_block_handle, &block_iter_, BlockType::kData,
          /*get_context=*/nullptr, &lookup_context_,
          block_prefetcher_.prefetch_buffer(),
          /*for_compaction=*/is_for_compaction, /*async_read=*/true, s,
          /*use_block_cache_for_lookup=*/true);

      if (s.IsTryAgain()) {
        async_read_in_progress_ = true;
        return;
      }
    }
  } else {
    // Second pass will call the Poll to get the data block which has been
    // requested asynchronously.
    bool is_in_cache = false;

    if (DoesContainBlockHandles()) {
      data_block_handle = block_handles_->front().handle_;
      is_in_cache = block_handles_->front().is_cache_hit_;
    } else {
      data_block_handle = index_iter_->value().handle;
    }

    Status s;
    // Initialize Data Block From CacheableEntry.
    if (is_in_cache) {
      block_iter_.Invalidate(Status::OK());
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, (block_handles_->front().cachable_entry_).As<Block>(),
          &block_iter_, s);
    } else {
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, data_block_handle, &block_iter_, BlockType::kData,
          /*get_context=*/nullptr, &lookup_context_,
          block_prefetcher_.prefetch_buffer(),
          /*for_compaction=*/is_for_compaction, /*async_read=*/false, s,
          /*use_block_cache_for_lookup=*/false);
    }
  }
  block_iter_points_to_real_block_ = true;
  CheckDataBlockWithinUpperBound();

  if (!is_for_compaction &&
      (seek_stat_state_ & kDataBlockReadSinceLastSeek) == 0) {
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_DATA
                                            : NON_LAST_LEVEL_SEEK_DATA);
    seek_stat_state_ = static_cast<SeekStatState>(
        seek_stat_state_ | kDataBlockReadSinceLastSeek | kReportOnUseful);
  }
  async_read_in_progress_ = false;
}

bool BlockBasedTableIterator::MaterializeCurrentBlock() {
  assert(is_at_first_key_from_index_);
  assert(!block_iter_points_to_real_block_);
  assert(index_iter_->Valid());

  is_at_first_key_from_index_ = false;
  InitDataBlock();
  assert(block_iter_points_to_real_block_);

  if (!block_iter_.status().ok()) {
    return false;
  }

  block_iter_.SeekToFirst();

  // MaterializeCurrentBlock is called when block is actually read by
  // calling InitDataBlock. is_at_first_key_from_index_ will be false for block
  // handles placed in blockhandle. So index_ will be pointing to current block.
  // After InitDataBlock, index_iter_ can point to different block if
  // BlockCacheLookupForReadAheadSize is called.
  Slice first_internal_key;
  if (DoesContainBlockHandles()) {
    first_internal_key = block_handles_->front().first_internal_key_;
  } else {
    first_internal_key = index_iter_->value().first_internal_key;
  }

  if (!block_iter_.Valid() ||
      icomp_.Compare(block_iter_.key(), first_internal_key) != 0) {
    block_iter_.Invalidate(Status::Corruption(
        "first key in index doesn't match first key in block"));
    return false;
  }
  return true;
}

void BlockBasedTableIterator::FindKeyForward() {
  // This method's code is kept short to make it likely to be inlined.
  assert(!is_out_of_bound_);
  assert(block_iter_points_to_real_block_);

  if (!block_iter_.Valid()) {
    // This is the only call site of FindBlockForward(), but it's extracted into
    // a separate method to keep FindKeyForward() short and likely to be
    // inlined. When transitioning to a different block, we call
    // FindBlockForward(), which is much longer and is probably not inlined.
    FindBlockForward();
  } else {
    // This is the fast path that avoids a function call.
  }
}

void BlockBasedTableIterator::FindBlockForward() {
  std::cout << "FindBlockForward prepared: " << prepared_ << std::endl;
  if (prepared_) {
    do {
      if (!block_iter_.status().ok()) {
        return;
      }
      // if (static_cast<size_t>(cur_data_block_idx_) >=
      // multiscan_pinned_data_blocks_.size()) {
      if (cur_data_block_idx_ + 1 >= cur_scan_end_idx_) {
        // TODO: document that -1 means cur BBTI is invalid, but status ok
        is_out_of_bound_ = true;
        std::cout << "FindBlockForward served from prepared blocks, "
                     "out-of-bound cur idx "
                  << cur_data_block_idx_ << " cur end idx " << cur_scan_end_idx_
                  << std::endl;
        return;
      }
      ResetDataIter();
      ++cur_data_block_idx_;
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, multiscan_pinned_data_blocks_[cur_data_block_idx_],
          &block_iter_, Status::OK());
      block_iter_.SeekToFirst();
      block_iter_points_to_real_block_ = true;
    } while (!block_iter_.Valid());
    std::cout << "FindBlockForward served from prepared blocks "
              << cur_data_block_idx_ << std::endl;
    return;
  }
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  do {
    if (!block_iter_.status().ok()) {
      return;
    }
    // Whether next data block is out of upper bound, if there is one.
    //  index_iter_ can point to different block in case of
    //  readahead_cache_lookup_. readahead_cache_lookup_ will be handle the
    //  upper_bound check.
    bool next_block_is_out_of_bound =
        IsIndexAtCurr() && read_options_.iterate_upper_bound != nullptr &&
        block_iter_points_to_real_block_ &&
        block_upper_bound_check_ == BlockUpperBound::kUpperBoundInCurBlock;

    assert(!next_block_is_out_of_bound ||
           user_comparator_.CompareWithoutTimestamp(
               *read_options_.iterate_upper_bound, /*a_has_ts=*/false,
               index_iter_->user_key(), /*b_has_ts=*/true) <= 0);

    ResetDataIter();

    if (DoesContainBlockHandles()) {
      // Advance and point to that next Block handle to make that block handle
      // current.
      block_handles_->pop_front();
    }

    if (!DoesContainBlockHandles()) {
      // For readahead_cache_lookup_ enabled scenario -
      // 1. In case of Seek, block_handle will be empty and it should be follow
      //    as usual doing index_iter_->Next().
      // 2. If block_handles is empty and index is not at current because of
      //    lookup (during Next), it should skip doing index_iter_->Next(), as
      //    it's already pointing to next block;
      // 3. Last block could be out of bound and it won't iterate over that
      // during BlockCacheLookup. We need to set for that block here.
      if (IsIndexAtCurr() || is_index_out_of_bound_) {
        index_iter_->Next();
        if (is_index_out_of_bound_) {
          next_block_is_out_of_bound = is_index_out_of_bound_;
          is_index_out_of_bound_ = false;
        }
      } else {
        // Skip Next as index_iter_ already points to correct index when it
        // iterates in BlockCacheLookupForReadAheadSize.
        is_index_at_curr_block_ = true;
      }

      if (next_block_is_out_of_bound) {
        // The next block is out of bound. No need to read it.
        TEST_SYNC_POINT_CALLBACK("BlockBasedTableIterator:out_of_bound",
                                 nullptr);
        // We need to make sure this is not the last data block before setting
        // is_out_of_bound_, since the index key for the last data block can be
        // larger than smallest key of the next file on the same level.
        if (index_iter_->Valid()) {
          is_out_of_bound_ = true;
        }
        return;
      }

      if (!index_iter_->Valid()) {
        return;
      }
      IndexValue v = index_iter_->value();

      if (!v.first_internal_key.empty() && allow_unprepared_value_) {
        // Index contains the first key of the block. Defer reading the block.
        is_at_first_key_from_index_ = true;
        return;
      }
    }
    InitDataBlock();
    block_iter_.SeekToFirst();
  } while (!block_iter_.Valid());
}

void BlockBasedTableIterator::FindKeyBackward() {
  while (!block_iter_.Valid()) {
    if (!block_iter_.status().ok()) {
      return;
    }

    ResetDataIter();
    index_iter_->Prev();

    if (index_iter_->Valid()) {
      InitDataBlock();
      block_iter_.SeekToLast();
    } else {
      return;
    }
  }

  // We could have check lower bound here too, but we opt not to do it for
  // code simplicity.
}

void BlockBasedTableIterator::CheckOutOfBound() {
  if (read_options_.iterate_upper_bound != nullptr &&
      block_upper_bound_check_ != BlockUpperBound::kUpperBoundBeyondCurBlock &&
      Valid()) {
    is_out_of_bound_ =
        user_comparator_.CompareWithoutTimestamp(
            *read_options_.iterate_upper_bound, /*a_has_ts=*/false, user_key(),
            /*b_has_ts=*/true) <= 0;
  }
}

void BlockBasedTableIterator::CheckDataBlockWithinUpperBound() {
  if (IsIndexAtCurr() && read_options_.iterate_upper_bound != nullptr &&
      block_iter_points_to_real_block_) {
    block_upper_bound_check_ = (user_comparator_.CompareWithoutTimestamp(
                                    *read_options_.iterate_upper_bound,
                                    /*a_has_ts=*/false, index_iter_->user_key(),
                                    /*b_has_ts=*/true) > 0)
                                   ? BlockUpperBound::kUpperBoundBeyondCurBlock
                                   : BlockUpperBound::kUpperBoundInCurBlock;
  }
}

void BlockBasedTableIterator::InitializeStartAndEndOffsets(
    bool read_curr_block, bool& found_first_miss_block,
    uint64_t& start_updated_offset, uint64_t& end_updated_offset,
    size_t& prev_handles_size) {
  assert(block_handles_ != nullptr);
  prev_handles_size = block_handles_->size();
  size_t footer = table_->get_rep()->footer.GetBlockTrailerSize();

  // It initialize start and end offset to begin which is covered by following
  // scenarios
  if (read_curr_block) {
    if (!DoesContainBlockHandles()) {
      // Scenario 1 : read_curr_block (callback made on miss block which caller
      //              was reading) and it has no existing handles in queue. i.e.
      //              index_iter_ is pointing to block that is being read by
      //              caller.
      //
      // Add current block here as it doesn't need any lookup.
      BlockHandleInfo block_handle_info;
      block_handle_info.handle_ = index_iter_->value().handle;
      block_handle_info.SetFirstInternalKey(
          index_iter_->value().first_internal_key);

      end_updated_offset = block_handle_info.handle_.offset() + footer +
                           block_handle_info.handle_.size();
      block_handles_->emplace_back(std::move(block_handle_info));

      index_iter_->Next();
      is_index_at_curr_block_ = false;
      found_first_miss_block = true;
    } else {
      // Scenario 2 : read_curr_block (callback made on miss block which caller
      //              was reading) but the queue already has some handles.
      //
      // It can be due to reading error in second buffer in FilePrefetchBuffer.
      // BlockHandles already added to the queue but there was error in fetching
      // those data blocks. So in this call they need to be read again.
      found_first_miss_block = true;
      // Initialize prev_handles_size to 0 as all those handles need to be read
      // again.
      prev_handles_size = 0;
      start_updated_offset = block_handles_->front().handle_.offset();
      end_updated_offset = block_handles_->back().handle_.offset() + footer +
                           block_handles_->back().handle_.size();
    }
  } else {
    // Scenario 3 : read_curr_block is false (callback made to do additional
    //              prefetching in buffers) and the queue already has some
    //              handles from first buffer.
    if (DoesContainBlockHandles()) {
      start_updated_offset = block_handles_->back().handle_.offset() + footer +
                             block_handles_->back().handle_.size();
      end_updated_offset = start_updated_offset;
    } else {
      // Scenario 4 : read_curr_block is false (callback made to do additional
      //              prefetching in buffers) but the queue has no handle
      //              from first buffer.
      //
      // It can be when Reseek is from block cache (which doesn't clear the
      // buffers in FilePrefetchBuffer but clears block handles from queue) and
      // reseek also lies within the buffer. So Next will get data from
      // exisiting buffers untill this callback is made to prefetch additional
      // data. All handles need to be added to the queue starting from
      // index_iter_.
      assert(index_iter_->Valid());
      start_updated_offset = index_iter_->value().handle.offset();
      end_updated_offset = start_updated_offset;
    }
  }
}

// BlockCacheLookupForReadAheadSize API lookups in the block cache and tries to
// reduce the start and end offset passed.
//
// Implementation -
// This function looks into the block cache for the blocks between start_offset
// and end_offset and add all the handles in the queue.
// It then iterates from the end to find first miss block and update the end
// offset to that block.
// It also iterates from the start and find first miss block and update the
// start offset to that block.
//
// Arguments -
// start_offset    : Offset from which the caller wants to read.
// end_offset      : End offset till which the caller wants to read.
// read_curr_block : True if this call was due to miss in the cache and
//                   caller wants to read that block.
//                   False if current call is to prefetch additional data in
//                   extra buffers.
void BlockBasedTableIterator::BlockCacheLookupForReadAheadSize(
    bool read_curr_block, uint64_t& start_offset, uint64_t& end_offset) {
  uint64_t start_updated_offset = start_offset;

  // readahead_cache_lookup_ can be set false, if after Seek and Next
  // there is SeekForPrev or any other backward operation.
  if (!readahead_cache_lookup_) {
    return;
  }

  size_t footer = table_->get_rep()->footer.GetBlockTrailerSize();
  if (read_curr_block && !DoesContainBlockHandles() &&
      IsNextBlockOutOfReadaheadBound()) {
    end_offset = index_iter_->value().handle.offset() + footer +
                 index_iter_->value().handle.size();
    return;
  }

  uint64_t end_updated_offset = start_updated_offset;
  bool found_first_miss_block = false;
  size_t prev_handles_size;

  // Initialize start and end offsets based on exisiting handles in the queue
  // and read_curr_block argument passed.
  if (block_handles_ == nullptr) {
    block_handles_.reset(new std::deque<BlockHandleInfo>());
  }
  InitializeStartAndEndOffsets(read_curr_block, found_first_miss_block,
                               start_updated_offset, end_updated_offset,
                               prev_handles_size);

  while (index_iter_->Valid() && !is_index_out_of_bound_) {
    BlockHandle block_handle = index_iter_->value().handle;

    // Adding this data block exceeds end offset. So this data
    // block won't be added.
    // There can be a case where passed end offset is smaller than
    // block_handle.size() + footer because of readahead_size truncated to
    // upper_bound. So we prefer to read the block rather than skip it to avoid
    // sync read calls in case of async_io.
    if (start_updated_offset != end_updated_offset &&
        (end_updated_offset + block_handle.size() + footer > end_offset)) {
      break;
    }

    // For current data block, do the lookup in the cache. Lookup should pin the
    // data block in cache.
    BlockHandleInfo block_handle_info;
    block_handle_info.handle_ = index_iter_->value().handle;
    block_handle_info.SetFirstInternalKey(
        index_iter_->value().first_internal_key);
    end_updated_offset += footer + block_handle_info.handle_.size();

    Status s = table_->LookupAndPinBlocksInCache<Block_kData>(
        read_options_, block_handle,
        &(block_handle_info.cachable_entry_).As<Block_kData>());
    if (!s.ok()) {
#ifndef NDEBUG
      // To allow fault injection verification to pass since non-okay status in
      // `BlockCacheLookupForReadAheadSize()` won't fail the read but to have
      // less or no readahead
      IGNORE_STATUS_IF_ERROR(s);
#endif
      break;
    }

    block_handle_info.is_cache_hit_ =
        (block_handle_info.cachable_entry_.GetValue() ||
         block_handle_info.cachable_entry_.GetCacheHandle());

    // If this is the first miss block, update start offset to this block.
    if (!found_first_miss_block && !block_handle_info.is_cache_hit_) {
      found_first_miss_block = true;
      start_updated_offset = block_handle_info.handle_.offset();
    }

    // Add the handle to the queue.
    block_handles_->emplace_back(std::move(block_handle_info));

    // Can't figure out for current block if current block
    // is out of bound. But for next block we can find that.
    // If curr block's index key >= iterate_upper_bound, it
    // means all the keys in next block or above are out of
    // bound.
    if (IsNextBlockOutOfReadaheadBound()) {
      is_index_out_of_bound_ = true;
      break;
    }
    index_iter_->Next();
    is_index_at_curr_block_ = false;
  }

#ifndef NDEBUG
  // To allow fault injection verification to pass since non-okay status in
  // `BlockCacheLookupForReadAheadSize()` won't fail the read but to have less
  // or no readahead
  if (!index_iter_->status().ok()) {
    IGNORE_STATUS_IF_ERROR(index_iter_->status());
  }
#endif

  if (found_first_miss_block) {
    // Iterate cache hit block handles from the end till a Miss is there, to
    // truncate and update the end offset till that Miss.
    auto it = block_handles_->rbegin();
    auto it_end =
        block_handles_->rbegin() + (block_handles_->size() - prev_handles_size);

    while (it != it_end && (*it).is_cache_hit_ &&
           start_updated_offset != (*it).handle_.offset()) {
      it++;
    }
    end_updated_offset = (*it).handle_.offset() + footer + (*it).handle_.size();
  } else {
    // Nothing to read. Can be because of IOError in index_iter_->Next() or
    // reached upper_bound.
    end_updated_offset = start_updated_offset;
  }

  end_offset = end_updated_offset;
  start_offset = start_updated_offset;
  ResetPreviousBlockOffset();
}

/*
  1. Collect all block handles needed for scan_opts
  2. Collapse block handles into IO requests
    - Check block cache and pin blocks and remove the ones that are pinned
  3. Fire IO requests
    - perhaps async
  4. When user Seek starts, ensure taht IO requests are done
*/
// TODO: prefix_same_as_start?
// We will collapse IOs into kIOSize chunks
const size_t kIOSize = 16 * 1024;  // 16KB

// During scanning, when iterator moves past the end of a scan range, it may
// return invalid or the next key in the block.
void BlockBasedTableIterator::Prepare(
    const std::vector<ScanOptions>* scan_opts) {
  assert(prepared_ == false);
  printf("[Prepare] Called with %zu scan options\n",
         scan_opts ? scan_opts->size() : 0);
  if (scan_opts == nullptr || scan_opts->empty()) {
    return;
  }
  const bool has_limit = scan_opts->front().range.limit.has_value();
  Slice start_key = scan_opts->front().range.start.value();

  {
    // Validate scan ranges to be increasing and with limit
    for (size_t i = 0; i < scan_opts->size(); ++i) {
      printf("[Prepare] Processing scan range %zu: start='%s', limit='%s'\n", i,
             scan_opts->at(i).range.start.value().ToString(true).c_str(),
             scan_opts->at(i).range.limit.has_value()
                 ? scan_opts->at(i).range.limit.value().ToString(true).c_str()
                 : "none");

      const auto& scan_opt = (*scan_opts)[i];
      if (!scan_opt.range.start.has_value()) {
        printf(
            "[Prepare] Skipping Prepare since scan range %zu: no start key\n",
            i);
        return;
      }

      if (i > 0 && !scan_opt.range.limit.has_value() == has_limit) {
        printf(
            "[Prepare] Skipping Prepare since scan range %zu: limit mismatch\n",
            i);
        return;
      }

      if (i > 0 && user_comparator_.Compare(scan_opt.range.start.value(),
                                            start_key) <= 0) {
        printf(
            "[Prepare] Skipping Prepare since scan range %zu: start key is not "
            "increasing\n",
            i);
        return;
      }
    }
  }

  // Collect all blocks that overlap with this range
  std::vector<BlockHandle> blocks_to_prepare;
  // TODO: check !has_limit special case
  if (!has_limit) {
    printf("[Prepare] Collecting blocks for range: start='%s', no limit\n",
           start_key.ToString(true).c_str());
    blocks_to_prepare = GetBlockHandlesForRange(scan_opts->front().range);
    // TODO: need to look over scan ranges to init this
    if (blocks_to_prepare.empty()) {
      starting_block_per_scan_.emplace_back(-1);
    } else {
      starting_block_per_scan_.emplace_back(0);
    }
    block_ranges_per_scan_.emplace_back(0, blocks_to_prepare.size());
  } else {
    for (size_t i = 0; i < scan_opts->size(); ++i) {
      printf("[Prepare] Collecting blocks for range: start='%s', limit='%s'\n",
             scan_opts->at(i).range.start.value().ToString(true).c_str(),
             scan_opts->at(i).range.limit.value().ToString(true).c_str());
      std::vector<BlockHandle> blocks =
          GetBlockHandlesForRange(scan_opts->at(i).range);
      // merge blocks into blocks_to_prepare, only for blocks that start further
      // than the last block in blocks_to_prepare
      //
      // only need to check the last block since scan start keys are increasing
      // starting_block_per_scan_.emplace_back(blocks_to_prepare.empty()?
      // BlockHandle::NullBlockHandle() : blocks_to_prepare.front());
      auto num_blocks = blocks.size();
      if (blocks.empty()) {
        starting_block_per_scan_.emplace_back(-1);
      } else {
        if (blocks_to_prepare.empty()) {
          starting_block_per_scan_.emplace_back(0);
          blocks_to_prepare = std::move(blocks);
        } else {
          // TODO: assert increasing sorted order
          auto last_block = blocks_to_prepare.back();
          // auto last_block_offset = last_block.offset() + last_block.size();
          bool overlap = last_block == blocks.front();
          if (overlap) {
            starting_block_per_scan_.emplace_back(blocks_to_prepare.size() - 1);
          } else {
            starting_block_per_scan_.emplace_back(blocks_to_prepare.size());
          }

          for (size_t j = (overlap ? 1 : 0); j < blocks.size(); j++) {
            blocks_to_prepare.push_back(blocks[j]);
          }
          // for (auto& block : blocks) {
          //   if (block.offset() > last_block_offset) {
          //     blocks_to_prepare.push_back(block);
          //   }
          // }
        }
      }
      block_ranges_per_scan_.emplace_back(blocks_to_prepare.size() - num_blocks,
                                          blocks_to_prepare.size());
    }
  }

  // Print all blocks to prepare and starting blocks per scan
  std::cout << "Blocks to prepare (" << blocks_to_prepare.size()
            << " blocks):" << std::endl;
  for (const auto& block : blocks_to_prepare) {
    std::cout << "  Offset: " << block.offset() << ", Size: " << block.size()
              << std::endl;
  }

  std::cout << "Starting blocks per scan (" << starting_block_per_scan_.size()
            << " scans):" << std::endl;
  for (size_t i = 0; i < starting_block_per_scan_.size(); i++) {
    std::cout << "  Scan " << i << " offset: " << starting_block_per_scan_[i]
              << std::endl;
  }

  // Print block ranges per scan
  std::cout << "Block ranges per scan (" << block_ranges_per_scan_.size()
            << " ranges):" << std::endl;
  for (size_t i = 0; i < block_ranges_per_scan_.size(); i++) {
    std::cout << "  Scan " << i << " range: ["
              << std::get<0>(block_ranges_per_scan_[i]) << ", "
              << std::get<1>(block_ranges_per_scan_[i]) << ")" << std::endl;
  }

  // ------------------------------------------------
  // blocks_to_prepare has all the blocks that need to be read
  // pin cached entries and filter them out
  // for (auto& block : blocks_to_prepare) {
  //   // TODO: filter by blocks in block cache and pin them
  //   blocks_to_read.push_back(block);
  // }
  // Print all blocks to read

  // index into multiscan_pinned_data_blocks_ -> block handle
  std::vector<std::tuple<size_t, BlockHandle>> blocks_to_read;
  Status s;
  for (const auto& data_block_handle : blocks_to_prepare) {
    std::cout << "Block to read - Offset: " << data_block_handle.offset()
              << ", Size: " << data_block_handle.size() << std::endl;
    multiscan_pinned_data_blocks_.emplace_back();
    // Test step: pin all blocks in block cache
    // TODO: status check
    // TODO: gc
    // CachableEntry<Block> b;
    // TODO: should use MaybeLoadAndPinBlock and check for result block
    s = table_->LookupAndPinBlocksInCache<Block_kData>(
        read_options_, data_block_handle,
        &multiscan_pinned_data_blocks_.back().As<Block_kData>());

    // s = table_->MaybeReadAndPinBlocksInCache<Block_kData>(read_options_,
    // data_block_handle,
    // &(multiscan_pinned_data_blocks_.back().As<Block_kData>()));
    if (!s.ok()) {
      // TODO: !ok should be logged or as iterator status
      std::cout << "Prepare abort, LookupAndPinBlocksInCache failed with "
                << s.ToString() << std::endl;
      multiscan_pinned_data_blocks_.clear();
      // prepared_data_blocks_.clear();
      return;
    }
    if (!multiscan_pinned_data_blocks_.back().GetValue()) {
      // std::cout << "Prepare abort, block read from FS/block cache returned
      // null" << std::endl;
      std::cout << "Prepare found a block that is not in block cache: "
                << data_block_handle.offset() << ", "
                << data_block_handle.size() << std::endl;
      blocks_to_read.emplace_back(multiscan_pinned_data_blocks_.size() - 1,
                                  data_block_handle);
    }
    // prepared_data_blocks_[data_block_handle.offset()] = std::move(b);
  }

  // ------------------------------------------------
  // Print all blocks to read
  std::cout << "Blocks to read (" << blocks_to_read.size()
            << " blocks):" << std::endl;
  for (const auto& block : blocks_to_read) {
    std::cout << "  Index: " << std::get<0>(block)
              << ", Offset: " << std::get<1>(block).offset()
              << ", Size: " << std::get<1>(block).size() << std::endl;
  }

  // ------------------------------------------------
  // Read blocks
  if (!blocks_to_read.empty()) {
    // Collapse IOs
    // Each vector correspond to a read request
    std::vector<std::vector<std::tuple<size_t, BlockHandle>>>
        collapsed_blocks_to_read;

    const size_t kCoalesceThreshold = 16 * 1024;
    collapsed_blocks_to_read.resize(1);
    auto& current_group = collapsed_blocks_to_read[0];

    for (const auto& block : blocks_to_read) {
      if (current_group.empty()) {
        current_group.push_back(block);
      } else {
        const auto& last_block = current_group.back();
        uint64_t last_end =
            std::get<1>(last_block).offset() +
            BlockBasedTable::BlockSizeWithTrailer(std::get<1>(last_block));
        uint64_t current_start = std::get<1>(block).offset();

        if (current_start > last_end + kCoalesceThreshold) {
          // new IO group
          collapsed_blocks_to_read.emplace_back();
          current_group = collapsed_blocks_to_read.back();
        }
        current_group.emplace_back(block);
      }
    }

    MultiScanPrefetcher multi_scan_prefetcher;
    IOOptions io_opts;
    s = table_->get_rep()->file->PrepareIOOptions(read_options_, io_opts);
    if (!s.ok()) {
      std::cout << "Prepare abort, PrepareIOOptions failed with "
                << s.ToString() << std::endl;
      multiscan_pinned_data_blocks_.clear();
      return;
    }

    // TODO: collapse IOs into kIOSize chunks
    std::vector<FSReadRequest> read_reqs;
    // read_reqs.reserve(blocks_to_read.size());
    read_reqs.reserve(collapsed_blocks_to_read.size());
    size_t total_len = 0;
    // for (const auto& block : blocks_to_read) {
    for (const auto& blocks : collapsed_blocks_to_read) {
      auto start_offset = std::get<1>(blocks[0]).offset();
      auto end_offset = std::get<1>(blocks.back()).offset() + BlockBasedTable::BlockSizeWithTrailer(std::get<1>(blocks.back()));
      auto t = std::get<1>(blocks[0]).offset();
      for (const auto& block : blocks) {
        assert(std::get<1>(block).offset() >= t);
        t = std::get<1>(block).offset();
      }

      FSReadRequest read_req;
      // read_req.offset = std::get<1>(block).offset();
      read_req.offset = start_offset;
      // read_req.len = std::get<1>(block).size() +
      //                table_->get_rep()->footer.GetBlockTrailerSize();
      assert(end_offset > start_offset);
      read_req.len = end_offset - start_offset;
      total_len += read_req.len;

      read_reqs.emplace_back(std::move(read_req));
    }

    std::unique_ptr<char[]> buf;

    bool direct_io = table_->get_rep()->file->use_direct_io();
    if (direct_io) {
      for (auto& read_req : read_reqs) {
        read_req.scratch = nullptr;
      }
    } else {
      // TODO: fs buffer?
      buf.reset(new char[total_len]);
      size_t offset = 0;
      for (auto& read_req : read_reqs) {
        read_req.scratch = buf.get() + offset;
        offset += read_req.len;
      }
    }

    // TODO: rate_limiter
    s = multi_scan_prefetcher.Prefetch(read_reqs, io_opts,
                                       table_->get_rep()->file.get());
    if (!s.ok()) {
      std::cout << "Prepare abort, MultiScan Prefetch failed with "
                << s.ToString() << std::endl;
      multiscan_pinned_data_blocks_.clear();
      return;
    }

    // init blocks with contents from read_reqs
    MemoryAllocator* memory_allocator =
        table_->get_rep()->table_options.block_cache->memory_allocator();
    // for (size_t i = 0; i < blocks_to_read.size(); i++) {
    for (size_t i = 0; i < collapsed_blocks_to_read.size(); i++) {
      auto& blocks = collapsed_blocks_to_read[i];
      // auto& block = blocks_to_read[i];
      for (const auto& block : blocks) {
        auto block_size_with_trailer = BlockBasedTable::BlockSizeWithTrailer(std::get<1>(block));
        auto& read_req = read_reqs[i];
        std::cout << "From read_read " << i << " with offset " << read_req.offset << " size " << read_req.len << std::endl;

        auto block_offset_in_buffer = std::get<1>(block).offset() - read_req.offset;

        std::cout << "init block handle  " << std::get<1>(block).offset() << ", "
                  << std::get<1>(block).size() << " block offset in buffer is " << block_offset_in_buffer << std::endl;


        std::cout << "Block compression type: "
                  << (int)BlockBasedTable::GetBlockCompressionType(
                        read_req.result.data() + block_offset_in_buffer, std::get<1>(block).size())
                  << std::endl;
        // TODO: figure out where is trailer size included/excluded
        CacheAllocationPtr data = AllocateBlock(block_size_with_trailer, memory_allocator);
            // AllocateBlock(read_req.result.size(), memory_allocator);
        memcpy(data.get(), read_req.result.data() + block_offset_in_buffer, block_size_with_trailer);
        BlockContents tmp_contents(std::move(data), std::get<1>(block).size());

      #ifndef NDEBUG
        tmp_contents.has_trailer =
            table_->get_rep()->footer.GetBlockTrailerSize() > 0;
      #endif

        s = table_->MaybeReadAndPinBlocksInCache<Block_kData>(
            read_options_, std::get<1>(block),
            &(multiscan_pinned_data_blocks_[std::get<0>(block)]
                  .As<Block_kData>()),
            &tmp_contents);
        if (!s.ok()) {
          std::cout << "Prepare abort, MaybeReadAndPinBlocksInCache failed when "
                      "reloading read blocks into cache with "
                    << s.ToString() << std::endl;
          multiscan_pinned_data_blocks_.clear();
          return;
        }
      }
    }
  }

  std::cout << "Prepare successful " << s.ToString() << std::endl;
  // All blocks are pinned
  scan_opts_ = scan_opts;
  next_multi_scan_idx_ = 0;
  cur_data_block_idx_ = 0;
  // This is important for status()
  is_index_at_curr_block_ = false;
  prepared_ = true;
  // TODO: update read path to get blocks from this vector.

  // We want to optimize the number of file read we call, and for each call we
  // restrict the IO size to multiples of kIOSize.

  // ------------------------------------------------
  // blocks_to_read has all the blocks that need to be read
  // Try to coalesce them into fewer IOs
  // const size_t trailer_size =
  // table_->get_rep()->footer.GetBlockTrailerSize(); struct IOReq{
  //   uint64_t offset;
  //   uint64_t size;
  // };
  // std::vector<IOReq> io_reqs;
  // for (auto& block : blocks_to_read) {
  //   uint64_t offset = block.offset();
  //   uint64_t size = block.size() + trailer_size;
  //   std::cout << "Processing block with offset: " << offset << ", size: " <<
  //   size << std::endl; if (io_reqs.empty()) {
  //     io_reqs.push_back({offset, size});
  //     std::cout << "Added new IOReq with offset: " << offset << ", size: " <<
  //     size << std::endl;
  //   } else {
  //     auto& last_io_req = io_reqs.back();
  //     if (offset == last_io_req.offset + last_io_req.size) {
  //       last_io_req.size += size;
  //       std::cout << "Merged with last IOReq, new size: " << last_io_req.size
  //       << std::endl;
  //     } else {
  //       io_reqs.push_back({offset, size});
  //       std::cout << "Added new IOReq with offset: " << offset << ", size: "
  //       << size << std::endl;
  //     }
  //   }
  // }

  // // Print final io_reqs
  // for (const auto& io_req : io_reqs) {
  //   std::cout << "IOReq - Offset: " << io_req.offset << ", Size: " <<
  //   io_req.size << std::endl;
  // }

  // ------------------------------------------------
  // TODO: further coalesce by considering kIOSize

  // ------------------------------------------------
  // TODO: fire IOs async
  // assert(read_options_.async_io);

  // ------------------------------------------------
  // Update states_ for iterator to use
  // or a callback function that loads and pins blocks into block cache
}

std::vector<BlockHandle> BlockBasedTableIterator::GetBlockHandlesForRange(
    const RangeOpt& range) {
  printf("[GetBlockHandlesForRange] Called with start='%s', limit='%s'\n",
         range.start.value().ToString(true).c_str(),
         range.limit.has_value() ? range.limit.value().ToString(true).c_str()
                                 : "none");

  std::vector<BlockHandle> blocks_for_range;
  // TODO: are we sure index is already here?
  // Find start block
  printf("Seeking to start of range: %s\n",
         range.start.value().ToString(true).c_str());

  // TODO: does user key work...?
  InternalKey start_key(range.start.value(), kMaxSequenceNumber,
                        kValueTypeForSeek);
  // index_iter_->Seek(range.start.value());
  index_iter_->Seek(start_key.Encode());
  // TODO: error handling
  assert(index_iter_->status().ok());
  if (!index_iter_->Valid()) {
    printf("Index iterator is not valid after seek.\n");
    return blocks_for_range;
  }

  while (
      index_iter_->Valid() &&
      (!range.limit.has_value() ||
       user_comparator_.Compare(index_iter_->user_key(), *range.limit) <= 0)) {
    printf("Adding block handle at offset: %lu, size: %lu, key: %s\n",
           static_cast<unsigned long>(index_iter_->value().handle.offset()),
           static_cast<unsigned long>(index_iter_->value().handle.size()),
           index_iter_->user_key().ToString(true).c_str());
    blocks_for_range.push_back(index_iter_->value().handle);
    index_iter_->Next();
  }
  // Stop until limit: index->key > limit
  // Include the current index block since it can still contain keys <= limit
  if (index_iter_->Valid()) {
    printf("Including last block handle at offset: %lu, size: %lu, key: %s\n",
           static_cast<unsigned long>(index_iter_->value().handle.offset()),
           static_cast<unsigned long>(index_iter_->value().handle.size()),
           index_iter_->user_key().ToString(true).c_str());
    blocks_for_range.push_back(index_iter_->value().handle);
  }

  return blocks_for_range;
}

// Add result to vector
void BlockBasedTableIterator::CollectBlocksForRange(const RangeOpt&,
                                                    std::vector<BlockHandle>&) {
  // // Save current index iterator position

  // // Seek to the start of the range
  // if (range.start.has_value()) {
  //   index_iter_->Seek(*range.start);
  // } else {
  //   index_iter_->SeekToFirst();
  // }

  // if (!index_iter_->Valid()) {
  //   // TODO: return error status
  //   assert(index_iter_->status().ok());
  //   return;
  // }

  // // Remember start offset and end_offset (end of last block)
  // uint64_t start_offset = index_iter_->value().handle.offset();
  // uint64_t end_offset = start_offset;

  // // Collect all blocks that overlap with this range
  // while (index_iter_->Valid()) {
  //   IndexValue index_value = index_iter_->value();
  //   blocks_to_prepare.push_back(index_value.handle);

  //   // Check if we've gone beyond the range limit

  // }

  // // Collect all blocks that overlap with this range
  // while (index_iter_->Valid()) {
  //   IndexValue index_value = index_iter_->value();

  //   // Check if we've gone beyond the range limit
  //   if (range.limit.has_value() &&
  //       user_comparator_.Compare(ExtractUserKey(index_value.first_internal_key),
  //                               *range.limit) >= 0) {
  //     break;
  //   }

  //   // Add this block to our collection
  //   BlockHandleInfo block_info;
  //   block_info.handle_ = index_value.handle;
  //   block_info.SetFirstInternalKey(index_value.first_internal_key);
  //   blocks_to_prepare.push_back(std::move(block_info));

  //   index_iter_->Next();
  // }

  // // Restore index iterator position
  // if (was_index_valid) {
  //   index_iter_->Seek(saved_key);
  // } else {
  //   index_iter_->SeekToFirst();
  //   if (index_iter_->Valid()) {
  //     index_iter_->Prev();
  //   }
  // }
}

void BlockBasedTableIterator::CoalesceAndPrefetchBlocks(
    const std::vector<BlockHandleInfo>& blocks) {
  if (blocks.empty()) {
    return;
  }

  const size_t kCoalesceThreshold = 16 * 1024;  // 16KB threshold for coalescing
  const size_t kFooterSize = table_->get_rep()->footer.GetBlockTrailerSize();

  // Group blocks for coalesced reads
  std::vector<std::vector<size_t>> coalesced_groups;
  std::vector<size_t> current_group;

  for (size_t i = 0; i < blocks.size(); ++i) {
    if (current_group.empty()) {
      current_group.push_back(i);
    } else {
      // Check if this block can be coalesced with the current group
      const auto& prev_block = blocks[current_group.back()];
      const auto& curr_block = blocks[i];

      uint64_t prev_end =
          prev_block.handle_.offset() + prev_block.handle_.size() + kFooterSize;
      uint64_t curr_start = curr_block.handle_.offset();
      uint64_t gap = curr_start - prev_end;

      if (gap <= kCoalesceThreshold) {
        current_group.push_back(i);
      } else {
        // Start a new group
        coalesced_groups.push_back(std::move(current_group));
        current_group.clear();
        current_group.push_back(i);
      }
    }
  }

  if (!current_group.empty()) {
    coalesced_groups.push_back(std::move(current_group));
  }

  // Process each coalesced group
  for (const auto& group : coalesced_groups) {
    ProcessCoalescedGroup(blocks, group);
  }
}

void BlockBasedTableIterator::ProcessCoalescedGroup(
    const std::vector<BlockHandleInfo>& blocks,
    const std::vector<size_t>& group_indices) {
  (void)blocks;
  (void)group_indices;
  printf("ProcessCoalescedGroup\n");

  // // Check cache for each block in the group
  // std::vector<bool> cache_hits(group_indices.size(), false);
  // std::vector<size_t> blocks_to_read;

  // for (size_t i = 0; i < group_indices.size(); ++i) {
  //   size_t block_idx = group_indices[i];
  //   const auto& block_info = blocks[block_idx];

  //   // Create a mutable copy for cache lookup
  //   BlockHandleInfo mutable_block_info = block_info;

  //   Status s = table_->LookupAndPinBlocksInCache<Block_kData>(
  //       read_options_, block_info.handle_,
  //       &(mutable_block_info.cachable_entry_).As<Block_kData>());

  //   if (s.ok() && (mutable_block_info.cachable_entry_.GetValue() ||
  //                  mutable_block_info.cachable_entry_.GetCacheHandle())) {
  //     // Block is in cache
  //     cache_hits[i] = true;
  //     mutable_block_info.is_cache_hit_ = true;

  //     // Add to our prepared blocks
  //     block_handles_->push_back(std::move(mutable_block_info));
  //   } else {
  //     // Block needs to be read from storage
  //     blocks_to_read.push_back(block_idx);
  //   }
  // }

  // // If we have blocks to read, issue coalesced read
  // if (!blocks_to_read.empty()) {
  //   IssueCoalescedRead(blocks, blocks_to_read);
  // }
}

void BlockBasedTableIterator::IssueCoalescedRead(
    const std::vector<BlockHandleInfo>& blocks,
    const std::vector<size_t>& blocks_to_read) {
  (void)blocks;

  printf("IssueCoalescedRead\n");
  if (blocks_to_read.empty()) {
    return;
  }

  // // For now, read blocks individually
  // // TODO: Implement true coalesced IO using async reads
  // for (size_t block_idx : blocks_to_read) {
  //   const auto& block_info = blocks[block_idx];

  //   // Create a mutable copy
  //   BlockHandleInfo mutable_block_info = block_info;
  //   mutable_block_info.is_cache_hit_ = false;

  //   // For now, we'll mark these blocks as needing to be read later
  //   // The actual reading will happen during normal iteration
  //   block_handles_->push_back(std::move(mutable_block_info));
  // }
}

}  // namespace ROCKSDB_NAMESPACE
