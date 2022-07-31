//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "db/version_set.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class DBRangeDelTest : public DBTestBase {
 public:
  DBRangeDelTest() : DBTestBase("db_range_del_test", /*env_do_fsync=*/false) {}

  std::string GetNumericStr(int key) {
    uint64_t uint64_key = static_cast<uint64_t>(key);
    std::string str;
    str.resize(8);
    memcpy(&str[0], static_cast<void*>(&uint64_key), 8);
    return str;
  }
};

// PlainTableFactory, WriteBatchWithIndex, and NumTableFilesAtLevel() are not
// supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(DBRangeDelTest, NonBlockBasedTableNotSupported) {
  // TODO: figure out why MmapReads trips the iterator pinning assertion in
  // RangeDelAggregator. Ideally it would be supported; otherwise it should at
  // least be explicitly unsupported.
  for (auto config : {kPlainTableAllBytesPrefix, /* kWalDirAndMmapReads */}) {
    option_config_ = config;
    DestroyAndReopen(CurrentOptions());
    ASSERT_TRUE(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "dr1", "dr1")
                    .IsNotSupported());
  }
}

TEST_F(DBRangeDelTest, WriteBatchWithIndexNotSupported) {
  WriteBatchWithIndex indexedBatch{};
  ASSERT_TRUE(indexedBatch.DeleteRange(db_->DefaultColumnFamily(), "dr1", "dr1")
                  .IsNotSupported());
  ASSERT_TRUE(indexedBatch.DeleteRange("dr1", "dr1").IsNotSupported());
}

TEST_F(DBRangeDelTest, EndSameAsStartCoversNothing) {
  ASSERT_OK(db_->Put(WriteOptions(), "b", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "b"));
  ASSERT_EQ("val", Get("b"));
}

TEST_F(DBRangeDelTest, EndComesBeforeStartInvalidArgument) {
  ASSERT_OK(db_->Put(WriteOptions(), "b", "val"));
  ASSERT_TRUE(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "a")
          .IsInvalidArgument());
  ASSERT_EQ("val", Get("b"));
}

TEST_F(DBRangeDelTest, FlushOutputHasOnlyRangeTombstones) {
  do {
    DestroyAndReopen(CurrentOptions());
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               "dr1", "dr2"));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_EQ(1, NumTableFilesAtLevel(0));
  } while (ChangeOptions(kRangeDelSkipConfigs));
}

TEST_F(DBRangeDelTest, DictionaryCompressionWithOnlyRangeTombstones) {
  Options opts = CurrentOptions();
  opts.compression_opts.max_dict_bytes = 16384;
  Reopen(opts);
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr1",
                             "dr2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
}

TEST_F(DBRangeDelTest, CompactionOutputHasOnlyRangeTombstone) {
  do {
    Options opts = CurrentOptions();
    opts.disable_auto_compactions = true;
    opts.statistics = CreateDBStatistics();
    DestroyAndReopen(opts);

    // snapshot protects range tombstone from dropping due to becoming obsolete.
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
    ASSERT_OK(db_->Flush(FlushOptions()));

    ASSERT_EQ(1, NumTableFilesAtLevel(0));
    ASSERT_EQ(0, NumTableFilesAtLevel(1));
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                          true /* disallow_trivial_move */));
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_EQ(1, NumTableFilesAtLevel(1));
    ASSERT_EQ(0, TestGetTickerCount(opts, COMPACTION_RANGE_DEL_DROP_OBSOLETE));
    db_->ReleaseSnapshot(snapshot);
    // Skip cuckoo memtables, which do not support snapshots. Skip non-leveled
    // compactions as the above assertions about the number of files in a level
    // do not hold true.
  } while (ChangeOptions(kRangeDelSkipConfigs | kSkipUniversalCompaction |
                         kSkipFIFOCompaction));
}

TEST_F(DBRangeDelTest, CompactionOutputFilesExactlyFilled) {
  // regression test for exactly filled compaction output files. Previously
  // another file would be generated containing all range deletions, which
  // could invalidate the non-overlapping file boundary invariant.
  const int kNumPerFile = 4, kNumFiles = 2, kFileBytes = 9 << 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  options.num_levels = 2;
  options.target_file_size_base = kFileBytes;
  BlockBasedTableOptions table_options;
  table_options.block_size_deviation = 50;  // each block holds two keys
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));

  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 12K (4 values, each 3K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
      if (j == 0 && i > 0) {
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      }
    }
  }
  // put extra key to trigger final flush
  ASSERT_OK(Put("", ""));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, MaxCompactionBytesCutsOutputFiles) {
  // Ensures range deletion spanning multiple compaction output files that are
  // cut by max_compaction_bytes will have non-overlapping key-ranges.
  // https://github.com/facebook/rocksdb/issues/1778
  const int kNumFiles = 2, kNumPerFile = 1 << 8, kBytesPerVal = 1 << 12;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.disable_auto_compactions = true;
  opts.level0_file_num_compaction_trigger = kNumFiles;
  opts.max_compaction_bytes = kNumPerFile * kBytesPerVal;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  // Want max_compaction_bytes to trigger the end of compaction output file, not
  // target_file_size_base, so make the latter much bigger
  //  opts.target_file_size_base = 100 * opts.max_compaction_bytes;
  opts.target_file_size_base = 1;
  DestroyAndReopen(opts);

  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  Random rnd(301);

  ASSERT_OK(Put(GetNumericStr(0), rnd.RandomString(kBytesPerVal)));
  ASSERT_OK(
      Put(GetNumericStr(kNumPerFile - 1), rnd.RandomString(kBytesPerVal)));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(GetNumericStr(kNumPerFile), rnd.RandomString(kBytesPerVal)));
  ASSERT_OK(
      Put(GetNumericStr(kNumPerFile * 2 - 1), rnd.RandomString(kBytesPerVal)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(NumTableFilesAtLevel(2), 2);

  ASSERT_OK(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"target_file_size_base",
                        std::to_string(100 * opts.max_compaction_bytes)}}));

  // It spans the whole key-range, thus will be included in all output files
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             GetNumericStr(0),
                             GetNumericStr(kNumFiles * kNumPerFile - 1)));

  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 1MB (256 values, each 4K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(kBytesPerVal));
      ASSERT_OK(Put(GetNumericStr(kNumPerFile * i + j), values[j]));
    }
    // extra entry to trigger SpecialSkipListFactory's flush
    ASSERT_OK(Put(GetNumericStr(kNumPerFile), ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
  }

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr,
                                        /*column_family=*/nullptr,
                                        /*disallow_trivial_move=*/true));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GE(NumTableFilesAtLevel(1), 2);
  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);

  for (size_t i = 0; i + 1 < files[1].size(); ++i) {
    ASSERT_TRUE(InternalKeyComparator(opts.comparator)
                    .Compare(files[1][i].largest, files[1][i + 1].smallest) <
                0);
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, SentinelsOmittedFromOutputFile) {
  // Regression test for bug where sentinel range deletions (i.e., ones with
  // sequence number of zero) were included in output files.
  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  // gaps between ranges creates sentinels in our internal representation
  std::vector<std::pair<std::string, std::string>> range_dels = {{"a", "b"}, {"c", "d"}, {"e", "f"}};
  for (const auto& range_del : range_dels) {
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               range_del.first, range_del.second));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
  ASSERT_GT(files[0][0].fd.smallest_seqno, 0);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, FlushRangeDelsSameStartKey) {
  ASSERT_OK(db_->Put(WriteOptions(), "b1", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "c"));
  ASSERT_OK(db_->Put(WriteOptions(), "b2", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "b"));
  // first iteration verifies query correctness in memtable, second verifies
  // query correctness for a single SST file
  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
      ASSERT_EQ(1, NumTableFilesAtLevel(0));
    }
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "b1", &value).IsNotFound());
    ASSERT_OK(db_->Get(ReadOptions(), "b2", &value));
  }
}

TEST_F(DBRangeDelTest, CompactRangeDelsSameStartKey) {
  ASSERT_OK(db_->Put(WriteOptions(), "unused",
                     "val"));  // prevents empty after compaction
  ASSERT_OK(db_->Put(WriteOptions(), "b1", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "c"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(3, NumTableFilesAtLevel(0));

  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                            true /* disallow_trivial_move */));
      ASSERT_EQ(0, NumTableFilesAtLevel(0));
      ASSERT_EQ(1, NumTableFilesAtLevel(1));
    }
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "b1", &value).IsNotFound());
  }
}
#endif  // ROCKSDB_LITE

TEST_F(DBRangeDelTest, FlushRemovesCoveredKeys) {
  const int kNum = 300, kRangeBegin = 50, kRangeEnd = 250;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  DestroyAndReopen(opts);

  // Write a third before snapshot, a third between snapshot and tombstone, and
  // a third after the tombstone. Keys older than snapshot or newer than the
  // tombstone should be preserved.
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 3) {
      snapshot = db_->GetSnapshot();
    } else if (i == 2 * kNum / 3) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 GetNumericStr(kRangeBegin),
                                 GetNumericStr(kRangeEnd)));
    }
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(i), "val"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (int i = 0; i < kNum; ++i) {
    ReadOptions read_opts;
    read_opts.ignore_range_deletions = true;
    std::string value;
    if (i < kRangeBegin || i > kRangeEnd || i < kNum / 3 || i >= 2 * kNum / 3) {
      ASSERT_OK(db_->Get(read_opts, GetNumericStr(i), &value));
    } else {
      ASSERT_TRUE(db_->Get(read_opts, GetNumericStr(i), &value).IsNotFound());
    }
  }
  db_->ReleaseSnapshot(snapshot);
}

// NumTableFilesAtLevel() is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(DBRangeDelTest, CompactionRemovesCoveredKeys) {
  const int kNumPerFile = 100, kNumFiles = 4;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.disable_auto_compactions = true;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  opts.num_levels = 2;
  opts.statistics = CreateDBStatistics();
  DestroyAndReopen(opts);

  for (int i = 0; i < kNumFiles; ++i) {
    if (i > 0) {
      // range tombstone covers first half of the previous file
      ASSERT_OK(db_->DeleteRange(
          WriteOptions(), db_->DefaultColumnFamily(),
          GetNumericStr((i - 1) * kNumPerFile),
          GetNumericStr((i - 1) * kNumPerFile + kNumPerFile / 2)));
    }
    // Make sure a given key appears in each file so compaction won't be able to
    // use trivial move, which would happen if the ranges were non-overlapping.
    // Also, we need an extra element since flush is only triggered when the
    // number of keys is one greater than SpecialSkipListFactory's limit.
    // We choose a key outside the key-range used by the test to avoid conflict.
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(kNumPerFile * kNumFiles),
                       "val"));

    for (int j = 0; j < kNumPerFile; ++j) {
      ASSERT_OK(
          db_->Put(WriteOptions(), GetNumericStr(i * kNumPerFile + j), "val"));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ((kNumFiles - 1) * kNumPerFile / 2,
            TestGetTickerCount(opts, COMPACTION_KEY_DROP_RANGE_DEL));

  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kNumPerFile; ++j) {
      ReadOptions read_opts;
      read_opts.ignore_range_deletions = true;
      std::string value;
      if (i == kNumFiles - 1 || j >= kNumPerFile / 2) {
        ASSERT_OK(
            db_->Get(read_opts, GetNumericStr(i * kNumPerFile + j), &value));
      } else {
        ASSERT_TRUE(
            db_->Get(read_opts, GetNumericStr(i * kNumPerFile + j), &value)
                .IsNotFound());
      }
    }
  }
}

TEST_F(DBRangeDelTest, ValidLevelSubcompactionBoundaries) {
  const int kNumPerFile = 100, kNumFiles = 4, kFileBytes = 100 << 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.max_bytes_for_level_base = 2 * kFileBytes;
  options.max_subcompactions = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  options.num_levels = 3;
  options.target_file_size_base = kFileBytes;
  options.target_file_size_multiplier = 1;
  options.max_compaction_bytes = 1500;
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    for (int j = 0; j < kNumFiles; ++j) {
      if (i > 0) {
        // delete [95,105) in two files, [295,305) in next two
        int mid = (j + (1 - j % 2)) * kNumPerFile;
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   Key(mid - 5), Key(mid + 5)));
      }
      std::vector<std::string> values;
      // Write 100KB (100 values, each 1K)
      for (int k = 0; k < kNumPerFile; k++) {
        values.push_back(rnd.RandomString(990));
        ASSERT_OK(Put(Key(j * kNumPerFile + k), values[k]));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put("", ""));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      if (j < kNumFiles - 1) {
        // background compaction may happen early for kNumFiles'th file
        ASSERT_EQ(NumTableFilesAtLevel(0), j + 1);
      }
      if (j == options.level0_file_num_compaction_trigger - 1) {
        // When i == 1, compaction will output some files to L1, at which point
        // L1 is not bottommost so range deletions cannot be compacted away. The
        // new L1 files must be generated with non-overlapping key ranges even
        // though multiple subcompactions see the same ranges deleted, else an
        // assertion will fail.
        //
        // Only enable auto-compactions when we're ready; otherwise, the
        // oversized L0 (relative to base_level) causes the compaction to run
        // earlier.
        ASSERT_OK(db_->EnableAutoCompaction({db_->DefaultColumnFamily()}));
        ASSERT_OK(dbfull()->TEST_WaitForCompact());
        ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                  {{"disable_auto_compactions", "true"}}));
        ASSERT_EQ(NumTableFilesAtLevel(0), 0);
        ASSERT_GT(NumTableFilesAtLevel(1), 0);
        ASSERT_GT(NumTableFilesAtLevel(2), 0);
      }
    }
  }
}

TEST_F(DBRangeDelTest, ValidUniversalSubcompactionBoundaries) {
  const int kNumPerFile = 100, kFilesPerLevel = 4, kNumLevels = 4;
  Options options = CurrentOptions();
  options.compaction_options_universal.min_merge_width = kFilesPerLevel;
  options.compaction_options_universal.max_merge_width = kFilesPerLevel;
  options.compaction_options_universal.size_ratio = 10;
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = kFilesPerLevel;
  options.max_subcompactions = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  options.num_levels = kNumLevels;
  options.target_file_size_base = kNumPerFile << 10;
  options.target_file_size_multiplier = 1;
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumLevels - 1; ++i) {
    for (int j = 0; j < kFilesPerLevel; ++j) {
      if (i == kNumLevels - 2) {
        // insert range deletions [95,105) in two files, [295,305) in next two
        // to prepare L1 for later manual compaction.
        int mid = (j + (1 - j % 2)) * kNumPerFile;
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   Key(mid - 5), Key(mid + 5)));
      }
      std::vector<std::string> values;
      // Write 100KB (100 values, each 1K)
      for (int k = 0; k < kNumPerFile; k++) {
        values.push_back(rnd.RandomString(990));
        ASSERT_OK(Put(Key(j * kNumPerFile + k), values[k]));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put("", ""));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      if (j < kFilesPerLevel - 1) {
        // background compaction may happen early for kFilesPerLevel'th file
        ASSERT_EQ(NumTableFilesAtLevel(0), j + 1);
      }
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    ASSERT_GT(NumTableFilesAtLevel(kNumLevels - 1 - i), kFilesPerLevel - 1);
  }
  // Now L1-L3 are full, when we compact L1->L2 we should see (1) subcompactions
  // happen since input level > 0; (2) range deletions are not dropped since
  // output level is not bottommost. If no file boundary assertion fails, that
  // probably means universal compaction + subcompaction + range deletion are
  // compatible.
  ASSERT_OK(dbfull()->RunManualCompaction(
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd(),
      1 /* input_level */, 2 /* output_level */, CompactRangeOptions(),
      nullptr /* begin */, nullptr /* end */, true /* exclusive */,
      true /* disallow_trivial_move */,
      std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
      "" /*trim_ts*/));
}
#endif  // ROCKSDB_LITE

TEST_F(DBRangeDelTest, CompactionRemovesCoveredMergeOperands) {
  const int kNumPerFile = 3, kNumFiles = 3;
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(2 * kNumPerFile));
  opts.merge_operator = MergeOperators::CreateUInt64AddOperator();
  opts.num_levels = 2;
  Reopen(opts);

  // Iterates kNumFiles * kNumPerFile + 1 times since flushing the last file
  // requires an extra entry.
  for (int i = 0; i <= kNumFiles * kNumPerFile; ++i) {
    if (i % kNumPerFile == 0 && i / kNumPerFile == kNumFiles - 1) {
      // Delete merge operands from all but the last file
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "key", "key_"));
    }
    std::string val;
    PutFixed64(&val, i);
    ASSERT_OK(db_->Merge(WriteOptions(), "key", val));
    // we need to prevent trivial move using Puts so compaction will actually
    // process the merge operands.
    ASSERT_OK(db_->Put(WriteOptions(), "prevent_trivial_move", ""));
    if (i > 0 && i % kNumPerFile == 0) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
  }

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  std::string expected, actual;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 45);  // 1+2+...+9
  ASSERT_EQ(expected, actual);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  expected.clear();
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  uint64_t tmp;
  Slice tmp2(actual);
  GetFixed64(&tmp2, &tmp);
  PutFixed64(&expected, 30);  // 6+7+8+9 (earlier operands covered by tombstone)
  ASSERT_EQ(expected, actual);
}

TEST_F(DBRangeDelTest, PutDeleteRangeMergeFlush) {
  // Test the sequence of operations: (1) Put, (2) DeleteRange, (3) Merge, (4)
  // Flush. The `CompactionIterator` previously had a bug where we forgot to
  // check for covering range tombstones when processing the (1) Put, causing
  // it to reappear after the flush.
  Options opts = CurrentOptions();
  opts.merge_operator = MergeOperators::CreateUInt64AddOperator();
  Reopen(opts);

  std::string val;
  PutFixed64(&val, 1);
  ASSERT_OK(db_->Put(WriteOptions(), "key", val));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             "key", "key_"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key", val));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions read_opts;
  std::string expected, actual;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 1);
  ASSERT_EQ(expected, actual);
}

// NumTableFilesAtLevel() is not supported in ROCKSDB_LITE
#ifndef ROCKSDB_LITE
TEST_F(DBRangeDelTest, ObsoleteTombstoneCleanup) {
  // During compaction to bottommost level, verify range tombstones older than
  // the oldest snapshot are removed, while others are preserved.
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.num_levels = 2;
  opts.statistics = CreateDBStatistics();
  Reopen(opts);

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr1",
                             "dr10"));  // obsolete after compaction
  ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr2",
                             "dr20"));  // protected by snapshot
  ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, TestGetTickerCount(opts, COMPACTION_RANGE_DEL_DROP_OBSOLETE));

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, TableEvictedDuringScan) {
  // The RangeDelAggregator holds pointers into range deletion blocks created by
  // table readers. This test ensures the aggregator can still access those
  // blocks even if it outlives the table readers that created them.
  //
  // DBIter always keeps readers open for L0 files. So, in order to test
  // aggregator outliving reader, we need to have deletions in L1 files, which
  // are opened/closed on-demand during the scan. This is accomplished by
  // setting kNumRanges > level0_stop_writes_trigger, which prevents deletions
  // from all lingering in L0 (there is at most one range deletion per L0 file).
  //
  // The first L1 file will contain a range deletion since its begin key is 0.
  // SeekToFirst() references that table's reader and adds its range tombstone
  // to the aggregator. Upon advancing beyond that table's key-range via Next(),
  // the table reader will be unreferenced by the iterator. Since we manually
  // call Evict() on all readers before the full scan, this unreference causes
  // the reader's refcount to drop to zero and thus be destroyed.
  //
  // When it is destroyed, we do not remove its range deletions from the
  // aggregator. So, subsequent calls to Next() must be able to use these
  // deletions to decide whether a key is covered. This will work as long as
  // the aggregator properly references the range deletion block.
  const int kNum = 25, kRangeBegin = 0, kRangeEnd = 7, kNumRanges = 5;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.level0_file_num_compaction_trigger = 4;
  opts.level0_stop_writes_trigger = 4;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  opts.num_levels = 2;
  BlockBasedTableOptions bbto;
  bbto.cache_index_and_filter_blocks = true;
  bbto.block_cache = NewLRUCache(8 << 20);
  opts.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(opts);

  // Hold a snapshot so range deletions can't become obsolete during compaction
  // to bottommost level (i.e., L1).
  const Snapshot* snapshot = db_->GetSnapshot();
  for (int i = 0; i < kNum; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(i), "val"));
    if (i > 0) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    if (i >= kNum / 2 && i < kNum / 2 + kNumRanges) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 GetNumericStr(kRangeBegin),
                                 GetNumericStr(kRangeEnd)));
    }
  }
  // Must be > 1 so the first L1 file can be closed before scan finishes
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(NumTableFilesAtLevel(1), 1);
  std::vector<uint64_t> file_numbers = ListTableFiles(env_, dbname_);

  ReadOptions read_opts;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());
  int expected = kRangeEnd;
  iter->SeekToFirst();
  for (auto file_number : file_numbers) {
    // This puts table caches in the state of being externally referenced only
    // so they are destroyed immediately upon iterator unreferencing.
    TableCache::Evict(dbfull()->TEST_table_cache(), file_number);
  }
  for (; iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    ++expected;
    // Keep clearing block cache's LRU so range deletion block can be freed as
    // soon as its refcount drops to zero.
    bbto.block_cache->EraseUnRefEntries();
  }
  ASSERT_EQ(kNum, expected);
  delete iter;
  db_->ReleaseSnapshot(snapshot);

  // Also test proper cache handling in GetRangeTombstoneIterator,
  // via TablesRangeTombstoneSummary. (This once triggered memory leak
  // report with ASAN.)
  opts.max_open_files = 1;
  Reopen(opts);

  std::string str;
  ASSERT_OK(dbfull()->TablesRangeTombstoneSummary(db_->DefaultColumnFamily(),
                                                  100, &str));
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromMutableMemtable) {
  do {
    DestroyAndReopen(CurrentOptions());
    ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

    ReadOptions read_opts;
    std::string value;
    ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
  } while (ChangeOptions(kRangeDelSkipConfigs));
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromImmutableMemtable) {
  do {
    Options opts = CurrentOptions();
    opts.max_write_buffer_number = 3;
    opts.min_write_buffer_number_to_merge = 2;
    // SpecialSkipListFactory lets us specify maximum number of elements the
    // memtable can hold. It switches the active memtable to immutable (flush is
    // prevented by the above options) upon inserting an element that would
    // overflow the memtable.
    opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
    DestroyAndReopen(opts);

    ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
    ASSERT_OK(db_->Put(WriteOptions(), "blah", "val"));

    ReadOptions read_opts;
    std::string value;
    ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
  } while (ChangeOptions(kRangeDelSkipConfigs));
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromSst) {
  do {
    DestroyAndReopen(CurrentOptions());
    ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
    // snapshot prevents key from being deleted during flush
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
    ASSERT_OK(db_->Flush(FlushOptions()));

    ReadOptions read_opts;
    std::string value;
    ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
    db_->ReleaseSnapshot(snapshot);
  } while (ChangeOptions(kRangeDelSkipConfigs));
}

TEST_F(DBRangeDelTest, GetCoveredMergeOperandFromMemtable) {
  const int kNumMergeOps = 10;
  Options opts = CurrentOptions();
  opts.merge_operator = MergeOperators::CreateUInt64AddOperator();
  Reopen(opts);

  for (int i = 0; i < kNumMergeOps; ++i) {
    std::string val;
    PutFixed64(&val, i);
    ASSERT_OK(db_->Merge(WriteOptions(), "key", val));
    if (i == kNumMergeOps / 2) {
      // deletes [0, 5]
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "key", "key_"));
    }
  }

  ReadOptions read_opts;
  std::string expected, actual;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 30);  // 6+7+8+9
  ASSERT_EQ(expected, actual);

  expected.clear();
  read_opts.ignore_range_deletions = true;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 45);  // 0+1+2+...+9
  ASSERT_EQ(expected, actual);
}

TEST_F(DBRangeDelTest, GetIgnoresRangeDeletions) {
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 4;
  opts.min_write_buffer_number_to_merge = 3;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  Reopen(opts);

  ASSERT_OK(db_->Put(WriteOptions(), "sst_key", "val"));
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "imm_key", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Put(WriteOptions(), "mem_key", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  for (std::string key : {"sst_key", "imm_key", "mem_key"}) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, key, &value));
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, IteratorRemovesCoveredKeys) {
  const int kNum = 200, kRangeBegin = 50, kRangeEnd = 150, kNumPerFile = 25;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  DestroyAndReopen(opts);

  // Write half of the keys before the tombstone and half after the tombstone.
  // Only covered keys (i.e., within the range and older than the tombstone)
  // should be deleted.
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 2) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 GetNumericStr(kRangeBegin),
                                 GetNumericStr(kRangeEnd)));
    }
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(i), "val"));
  }
  ReadOptions read_opts;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());

  int expected = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    if (expected == kRangeBegin - 1) {
      expected = kNum / 2;
    } else {
      ++expected;
    }
  }
  ASSERT_EQ(kNum, expected);
  delete iter;
}

TEST_F(DBRangeDelTest, IteratorOverUserSnapshot) {
  const int kNum = 200, kRangeBegin = 50, kRangeEnd = 150, kNumPerFile = 25;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  DestroyAndReopen(opts);

  const Snapshot* snapshot = nullptr;
  // Put a snapshot before the range tombstone, verify an iterator using that
  // snapshot sees all inserted keys.
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 2) {
      snapshot = db_->GetSnapshot();
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 GetNumericStr(kRangeBegin),
                                 GetNumericStr(kRangeEnd)));
    }
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(i), "val"));
  }
  ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());

  int expected = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    ++expected;
  }
  ASSERT_EQ(kNum / 2, expected);
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, IteratorIgnoresRangeDeletions) {
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 4;
  opts.min_write_buffer_number_to_merge = 3;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  Reopen(opts);

  ASSERT_OK(db_->Put(WriteOptions(), "sst_key", "val"));
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "imm_key", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Put(WriteOptions(), "mem_key", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());
  int i = 0;
  std::string expected[] = {"imm_key", "mem_key", "sst_key"};
  for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++i) {
    std::string key;
    ASSERT_EQ(expected[i], iter->key());
  }
  ASSERT_EQ(3, i);
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

#ifndef ROCKSDB_UBSAN_RUN
TEST_F(DBRangeDelTest, TailingIteratorRangeTombstoneUnsupported) {
  ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  // iterations check unsupported in memtable, l0, and then l1
  for (int i = 0; i < 3; ++i) {
    ReadOptions read_opts;
    read_opts.tailing = true;
    auto* iter = db_->NewIterator(read_opts);
    if (i == 2) {
      // For L1+, iterators over files are created on-demand, so need seek
      iter->SeekToFirst();
    }
    ASSERT_TRUE(iter->status().IsNotSupported());

    delete iter;
    if (i == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else if (i == 1) {
      MoveFilesToLevel(1);
    }
  }
  db_->ReleaseSnapshot(snapshot);
}
#endif  // !ROCKSDB_UBSAN_RUN

TEST_F(DBRangeDelTest, SubcompactionHasEmptyDedicatedRangeDelFile) {
  const int kNumFiles = 2, kNumKeysPerFile = 4;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.max_subcompactions = 2;
  options.num_levels = 2;
  options.target_file_size_base = 4096;
  Reopen(options);

  // need a L1 file for subcompaction to be triggered
  ASSERT_OK(
      db_->Put(WriteOptions(), db_->DefaultColumnFamily(), Key(0), "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);

  // put enough keys to fill up the first subcompaction, and later range-delete
  // them so that the first subcompaction outputs no key-values. In that case
  // it'll consider making an SST file dedicated to range deletions.
  for (int i = 0; i < kNumKeysPerFile; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), Key(i),
                       std::string(1024, 'a')));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(kNumKeysPerFile)));

  // the above range tombstone can be dropped, so that one alone won't cause a
  // dedicated file to be opened. We can make one protected by snapshot that
  // must be considered. Make its range outside the first subcompaction's range
  // to exercise the tricky part of the code.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             Key(kNumKeysPerFile + 1),
                             Key(kNumKeysPerFile + 2)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  ASSERT_OK(db_->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, MemtableBloomFilter) {
  // regression test for #2743. the range delete tombstones in memtable should
  // be added even when Get() skips searching due to its prefix bloom filter
  const int kMemtableSize = 1 << 20;              // 1MB
  const int kMemtablePrefixFilterSize = 1 << 13;  // 8KB
  const int kNumKeys = 1000;
  const int kPrefixLen = 8;
  Options options = CurrentOptions();
  options.memtable_prefix_bloom_size_ratio =
      static_cast<double>(kMemtablePrefixFilterSize) / kMemtableSize;
  options.prefix_extractor.reset(
      ROCKSDB_NAMESPACE::NewFixedPrefixTransform(kPrefixLen));
  options.write_buffer_size = kMemtableSize;
  Reopen(options);

  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "val"));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(kNumKeys)));
  for (int i = 0; i < kNumKeys; ++i) {
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
  }
}

TEST_F(DBRangeDelTest, CompactionTreatsSplitInputLevelDeletionAtomically) {
  // This test originally verified that compaction treated files containing a
  // split range deletion in the input level as an atomic unit. I.e.,
  // compacting any input-level file(s) containing a portion of the range
  // deletion causes all other input-level files containing portions of that
  // same range deletion to be included in the compaction. Range deletion
  // tombstones are now truncated to sstable boundaries which removed the need
  // for that behavior (which could lead to excessively large
  // compactions).
  const int kNumFilesPerLevel = 4, kValueBytes = 4 << 10;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = kNumFilesPerLevel;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(2 /* num_entries_flush */));
  options.target_file_size_base = kValueBytes;
  options.max_compaction_bytes = 1500;
  // i == 0: CompactFiles
  // i == 1: CompactRange
  // i == 2: automatic compaction
  for (int i = 0; i < 3; ++i) {
    DestroyAndReopen(options);

    ASSERT_OK(Put(Key(0), ""));
    ASSERT_OK(db_->Flush(FlushOptions()));
    MoveFilesToLevel(2);
    ASSERT_EQ(1, NumTableFilesAtLevel(2));

    // snapshot protects range tombstone from dropping due to becoming obsolete.
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(0), Key(2 * kNumFilesPerLevel)));

    Random rnd(301);
    std::string value = rnd.RandomString(kValueBytes);
    for (int j = 0; j < kNumFilesPerLevel; ++j) {
      // give files overlapping key-ranges to prevent trivial move
      ASSERT_OK(Put(Key(j), value));
      ASSERT_OK(Put(Key(2 * kNumFilesPerLevel - 1 - j), value));
      if (j > 0) {
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
        ASSERT_EQ(j, NumTableFilesAtLevel(0));
      }
    }
    // put extra key to trigger final flush
    ASSERT_OK(Put("", ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_EQ(kNumFilesPerLevel, NumTableFilesAtLevel(1));

    ColumnFamilyMetaData meta;
    db_->GetColumnFamilyMetaData(&meta);
    if (i == 0) {
      ASSERT_OK(db_->CompactFiles(
          CompactionOptions(), {meta.levels[1].files[0].name}, 2 /* level */));
      ASSERT_EQ(0, NumTableFilesAtLevel(1));
    } else if (i == 1) {
      auto begin_str = Key(0), end_str = Key(1);
      Slice begin = begin_str, end = end_str;
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &begin, &end));
      ASSERT_EQ(3, NumTableFilesAtLevel(1));
    } else if (i == 2) {
      ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                {{"max_bytes_for_level_base", "10000"}}));
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ(1, NumTableFilesAtLevel(1));
    }
    ASSERT_GT(NumTableFilesAtLevel(2), 0);

    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(DBRangeDelTest, RangeTombstoneEndKeyAsSstableUpperBound) {
  // Test the handling of the range-tombstone end-key as the
  // upper-bound for an sstable.

  const int kNumFilesPerLevel = 2, kValueBytes = 4 << 10;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = kNumFilesPerLevel;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(2 /* num_entries_flush */));
  options.target_file_size_base = kValueBytes;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);

  // Create an initial sstable at L2:
  //   [key000000#1,1, key000000#1,1]
  ASSERT_OK(Put(Key(0), ""));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // A snapshot protects the range tombstone from dropping due to
  // becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(2 * kNumFilesPerLevel)));

  // Create 2 additional sstables in L0. Note that the first sstable
  // contains the range tombstone.
  //   [key000000#3,1, key000004#72057594037927935,15]
  //   [key000001#5,1, key000002#6,1]
  Random rnd(301);
  std::string value = rnd.RandomString(kValueBytes);
  for (int j = 0; j < kNumFilesPerLevel; ++j) {
    // Give files overlapping key-ranges to prevent a trivial move when we
    // compact from L0 to L1.
    ASSERT_OK(Put(Key(j), value));
    ASSERT_OK(Put(Key(2 * kNumFilesPerLevel - 1 - j), value));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_EQ(j + 1, NumTableFilesAtLevel(0));
  }
  // Compact the 2 L0 sstables to L1, resulting in the following LSM. There
  // are 2 sstables generated in L1 due to the target_file_size_base setting.
  //   L1:
  //     [key000000#3,1, key000002#72057594037927935,15]
  //     [key000002#6,1, key000004#72057594037927935,15]
  //   L2:
  //     [key000000#1,1, key000000#1,1]
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  {
    // Compact the second sstable in L1:
    //   L1:
    //     [key000000#3,1, key000002#72057594037927935,15]
    //   L2:
    //     [key000000#1,1, key000000#1,1]
    //     [key000002#6,1, key000004#72057594037927935,15]
    //
    // At the same time, verify the compaction does not cause the key at the
    // endpoint (key000002#6,1) to disappear.
    ASSERT_EQ(value, Get(Key(2)));
    auto begin_str = Key(3);
    const ROCKSDB_NAMESPACE::Slice begin = begin_str;
    ASSERT_OK(dbfull()->TEST_CompactRange(1, &begin, nullptr));
    ASSERT_EQ(1, NumTableFilesAtLevel(1));
    ASSERT_EQ(2, NumTableFilesAtLevel(2));
    ASSERT_EQ(value, Get(Key(2)));
  }

  {
    // Compact the first sstable in L1. This should be copacetic, but
    // was previously resulting in overlapping sstables in L2 due to
    // mishandling of the range tombstone end-key when used as the
    // largest key for an sstable. The resulting LSM structure should
    // be:
    //
    //   L2:
    //     [key000000#1,1, key000001#72057594037927935,15]
    //     [key000001#5,1, key000002#72057594037927935,15]
    //     [key000002#6,1, key000004#72057594037927935,15]
    auto begin_str = Key(0);
    const ROCKSDB_NAMESPACE::Slice begin = begin_str;
    ASSERT_OK(dbfull()->TEST_CompactRange(1, &begin, &begin));
    ASSERT_EQ(0, NumTableFilesAtLevel(1));
    ASSERT_EQ(3, NumTableFilesAtLevel(2));
  }

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, UnorderedTombstones) {
  // Regression test for #2752. Range delete tombstones between
  // different snapshot stripes are not stored in order, so the first
  // tombstone of each snapshot stripe should be checked as a smallest
  // candidate.
  Options options = CurrentOptions();
  DestroyAndReopen(options);

  auto cf = db_->DefaultColumnFamily();

  ASSERT_OK(db_->Put(WriteOptions(), cf, "a", "a"));
  ASSERT_OK(db_->Flush(FlushOptions(), cf));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  ASSERT_OK(db_->DeleteRange(WriteOptions(), cf, "b", "c"));
  // Hold a snapshot to separate these two delete ranges.
  auto snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), cf, "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions(), cf));
  db_->ReleaseSnapshot(snapshot);

  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(cf, &files);
  ASSERT_EQ(1, files[0].size());
  ASSERT_EQ("a", files[0][0].smallest.user_key());
  ASSERT_EQ("c", files[0][0].largest.user_key());

  std::string v;
  auto s = db_->Get(ReadOptions(), "a", &v);
  ASSERT_TRUE(s.IsNotFound());
}

class MockMergeOperator : public MergeOperator {
  // Mock non-associative operator. Non-associativity is expressed by lack of
  // implementation for any `PartialMerge*` functions.
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    assert(merge_out != nullptr);
    merge_out->new_value = merge_in.operand_list.back().ToString();
    return true;
  }

  const char* Name() const override { return "MockMergeOperator"; }
};

TEST_F(DBRangeDelTest, KeyAtOverlappingEndpointReappears) {
  // This test uses a non-associative merge operator since that is a convenient
  // way to get compaction to write out files with overlapping user-keys at the
  // endpoints. Note, however, overlapping endpoints can also occur with other
  // value types (Put, etc.), assuming the right snapshots are present.
  const int kFileBytes = 1 << 20;
  const int kValueBytes = 1 << 10;
  const int kNumFiles = 4;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.merge_operator.reset(new MockMergeOperator());
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  // Push dummy data to L3 so that our actual test files on L0-L2
  // will not be considered "bottommost" level, otherwise compaction
  // may prevent us from creating overlapping user keys
  // as on the bottommost layer MergeHelper
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "dummy"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(3);

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kFileBytes / kValueBytes; ++j) {
      auto value = rnd.RandomString(kValueBytes);
      ASSERT_OK(db_->Merge(WriteOptions(), "key", value));
    }
    if (i == kNumFiles - 1) {
      // Take snapshot to prevent covered merge operands from being dropped by
      // compaction.
      snapshot = db_->GetSnapshot();
      // The DeleteRange is the last write so all merge operands are covered.
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "key", "key_"));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());

  ASSERT_OK(dbfull()->TEST_CompactRange(
      0 /* level */, nullptr /* begin */, nullptr /* end */,
      nullptr /* column_family */, true /* disallow_trivial_move */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  // Now we have multiple files at L1 all containing a single user key, thus
  // guaranteeing overlap in the file endpoints.
  ASSERT_GT(NumTableFilesAtLevel(1), 1);

  // Verify no merge operands reappeared after the compaction.
  ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());

  // Compact and verify again. It's worthwhile because now the files have
  // tighter endpoints, so we can verify that doesn't mess anything up.
  ASSERT_OK(dbfull()->TEST_CompactRange(
      1 /* level */, nullptr /* begin */, nullptr /* end */,
      nullptr /* column_family */, true /* disallow_trivial_move */));
  ASSERT_GT(NumTableFilesAtLevel(2), 1);
  ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, UntruncatedTombstoneDoesNotDeleteNewerKey) {
  // Verify a key newer than a range tombstone cannot be deleted by being
  // compacted to the bottom level (and thus having its seqnum zeroed) before
  // the range tombstone. This used to happen when range tombstones were
  // untruncated on reads such that they extended past their file boundaries.
  //
  // Test summary:
  //
  // - L1 is bottommost.
  // - A couple snapshots are strategically taken to prevent seqnums from being
  //   zeroed, range tombstone from being dropped, merge operands from being
  //   dropped, and merge operands from being combined.
  // - Left half of files in L1 all have same user key, ensuring their file
  //   boundaries overlap. In the past this would cause range tombstones to be
  //   untruncated.
  // - Right half of L1 files all have different keys, ensuring no overlap.
  // - A range tombstone spans all L1 keys, so it is stored in every L1 file.
  // - Keys in the right side of the key-range are overwritten. These are
  //   compacted down to L1 after releasing snapshots such that their seqnums
  //   will be zeroed.
  // - A full range scan is performed. If the tombstone in the left L1 files
  //   were untruncated, it would now cover keys newer than it (but with zeroed
  //   seqnums) in the right L1 files.
  const int kFileBytes = 1 << 20;
  const int kValueBytes = 1 << 10;
  const int kNumFiles = 4;
  const int kMaxKey = kNumFiles* kFileBytes / kValueBytes;
  const int kKeysOverwritten = 10;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.merge_operator.reset(new MockMergeOperator());
  options.num_levels = 2;
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  Random rnd(301);
  // - snapshots[0] prevents merge operands from being combined during
  //   compaction.
  // - snapshots[1] prevents merge operands from being dropped due to the
  //   covering range tombstone.
  const Snapshot* snapshots[] = {nullptr, nullptr};
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kFileBytes / kValueBytes; ++j) {
      auto value = rnd.RandomString(kValueBytes);
      std::string key;
      if (i < kNumFiles / 2) {
        key = Key(0);
      } else {
        key = Key(1 + i * kFileBytes / kValueBytes + j);
      }
      ASSERT_OK(db_->Merge(WriteOptions(), key, value));
    }
    if (i == 0) {
      snapshots[0] = db_->GetSnapshot();
    }
    if (i == kNumFiles - 1) {
      snapshots[1] = db_->GetSnapshot();
      // The DeleteRange is the last write so all merge operands are covered.
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 Key(0), Key(kMaxKey + 1)));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));

  auto get_key_count = [this]() -> int {
    auto* iter = db_->NewIterator(ReadOptions());
    assert(iter->status().ok());
    iter->SeekToFirst();
    int keys_found = 0;
    for (; iter->Valid(); iter->Next()) {
      ++keys_found;
    }
    delete iter;
    return keys_found;
  };

  // All keys should be covered
  ASSERT_EQ(0, get_key_count());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr /* begin_key */,
                              nullptr /* end_key */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  // Roughly the left half of L1 files should have overlapping boundary keys,
  // while the right half should not.
  ASSERT_GE(NumTableFilesAtLevel(1), kNumFiles);

  // Now overwrite a few keys that are in L1 files that definitely don't have
  // overlapping boundary keys.
  for (int i = kMaxKey; i > kMaxKey - kKeysOverwritten; --i) {
    auto value = rnd.RandomString(kValueBytes);
    ASSERT_OK(db_->Merge(WriteOptions(), Key(i), value));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // The overwritten keys are in L0 now, so clearly aren't covered by the range
  // tombstone in L1.
  ASSERT_EQ(kKeysOverwritten, get_key_count());

  // Release snapshots so seqnums can be zeroed when L0->L1 happens.
  db_->ReleaseSnapshot(snapshots[0]);
  db_->ReleaseSnapshot(snapshots[1]);

  auto begin_key_storage = Key(kMaxKey - kKeysOverwritten + 1);
  auto end_key_storage = Key(kMaxKey);
  Slice begin_key(begin_key_storage);
  Slice end_key(end_key_storage);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &begin_key, &end_key));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GE(NumTableFilesAtLevel(1), kNumFiles);

  ASSERT_EQ(kKeysOverwritten, get_key_count());
}

TEST_F(DBRangeDelTest, DeletedMergeOperandReappearsIterPrev) {
  // Exposes a bug where we were using
  // `RangeDelPositioningMode::kBackwardTraversal` while scanning merge operands
  // in the forward direction. Confusingly, this case happened during
  // `DBIter::Prev`. It could cause assertion failure, or reappearing keys.
  const int kFileBytes = 1 << 20;
  const int kValueBytes = 1 << 10;
  // Need multiple keys so we can get results when calling `Prev()` after
  // `SeekToLast()`.
  const int kNumKeys = 3;
  const int kNumFiles = 4;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.merge_operator.reset(new MockMergeOperator());
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kFileBytes / kValueBytes; ++j) {
      auto value = rnd.RandomString(kValueBytes);
      ASSERT_OK(db_->Merge(WriteOptions(), Key(j % kNumKeys), value));
      if (i == 0 && j == kNumKeys) {
        // Take snapshot to prevent covered merge operands from being dropped or
        // merged by compaction.
        snapshot = db_->GetSnapshot();
        // Do a DeleteRange near the beginning so only the oldest merge operand
        // for each key is covered. This ensures the sequence of events:
        //
        // - `DBIter::Prev()` is called
        // - After several same versions of the same user key are encountered,
        //   it decides to seek using `DBIter::FindValueForCurrentKeyUsingSeek`.
        // - Binary searches to the newest version of the key, which is in the
        //   leftmost file containing the user key.
        // - Scans forwards to collect all merge operands. Eventually reaches
        //   the rightmost file containing the oldest merge operand, which
        //   should be covered by the `DeleteRange`. If `RangeDelAggregator`
        //   were not properly using `kForwardTraversal` here, that operand
        //   would reappear.
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   Key(0), Key(kNumKeys + 1)));
      }
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr /* begin_key */,
                              nullptr /* end_key */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(1), 1);

  auto* iter = db_->NewIterator(ReadOptions());
  ASSERT_OK(iter->status());
  iter->SeekToLast();
  int keys_found = 0;
  for (; iter->Valid(); iter->Prev()) {
    ++keys_found;
  }
  delete iter;
  ASSERT_EQ(kNumKeys, keys_found);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, SnapshotPreventsDroppedKeys) {
  const int kFileBytes = 1 << 20;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  const Snapshot* snapshot = db_->GetSnapshot();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(10)));

  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(Key(0), iter->key());

  iter->Next();
  ASSERT_FALSE(iter->Valid());

  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, SnapshotPreventsDroppedKeysInImmMemTables) {
  const int kFileBytes = 1 << 20;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  // block flush thread -> pin immtables in memory
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"SnapshotPreventsDroppedKeysInImmMemTables:AfterNewIterator",
       "DBImpl::BGWorkFlush"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(0), "a"));
  std::unique_ptr<const Snapshot, std::function<void(const Snapshot*)>>
      snapshot(db_->GetSnapshot(),
               [this](const Snapshot* s) { db_->ReleaseSnapshot(s); });

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(10)));

  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  ReadOptions read_opts;
  read_opts.snapshot = snapshot.get();
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
  ASSERT_OK(iter->status());

  TEST_SYNC_POINT("SnapshotPreventsDroppedKeysInImmMemTables:AfterNewIterator");

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(Key(0), iter->key());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
}

TEST_F(DBRangeDelTest, RangeTombstoneWrittenToMinimalSsts) {
  // Adapted from
  // https://github.com/cockroachdb/cockroach/blob/de8b3ea603dd1592d9dc26443c2cc92c356fbc2f/pkg/storage/engine/rocksdb_test.go#L1267-L1398.
  // Regression test for issue where range tombstone was written to more files
  // than necessary when it began exactly at the begin key in the next
  // compaction output file.
  const int kFileBytes = 1 << 20;
  const int kValueBytes = 4 << 10;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  // Have a bit of slack in the size limits but we enforce them more strictly
  // when manually flushing/compacting.
  options.max_compaction_bytes = 2 * kFileBytes;
  options.target_file_size_base = 2 * kFileBytes;
  options.write_buffer_size = 2 * kFileBytes;
  Reopen(options);

  Random rnd(301);
  for (char first_char : {'a', 'b', 'c'}) {
    for (int i = 0; i < kFileBytes / kValueBytes; ++i) {
      std::string key(1, first_char);
      key.append(Key(i));
      std::string value = rnd.RandomString(kValueBytes);
      ASSERT_OK(Put(key, value));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
    MoveFilesToLevel(2);
  }
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(3, NumTableFilesAtLevel(2));

  // Populate the memtable lightly while spanning the whole key-space. The
  // setting of `max_compaction_bytes` will cause the L0->L1 to output multiple
  // files to prevent a large L1->L2 compaction later.
  ASSERT_OK(Put("a", "val"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             "c" + Key(1), "d"));
  // Our compaction output file cutting logic currently only considers point
  // keys. So, in order for the range tombstone to have a chance at landing at
  // the start of a new file, we need a point key at the range tombstone's
  // start.
  // TODO(ajkr): remove this `Put` after file cutting accounts for range
  // tombstones (#3977).
  ASSERT_OK(Put("c" + Key(1), "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Ensure manual L0->L1 compaction cuts the outputs before the range tombstone
  // and the range tombstone is only placed in the second SST.
  std::string begin_key_storage("c" + Key(1));
  Slice begin_key(begin_key_storage);
  std::string end_key_storage("d");
  Slice end_key(end_key_storage);
  ASSERT_OK(dbfull()->TEST_CompactRange(
      0 /* level */, &begin_key /* begin */, &end_key /* end */,
      nullptr /* column_family */, true /* disallow_trivial_move */));
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  std::vector<LiveFileMetaData> all_metadata;
  std::vector<LiveFileMetaData> l1_metadata;
  db_->GetLiveFilesMetaData(&all_metadata);
  for (const auto& metadata : all_metadata) {
    if (metadata.level == 1) {
      l1_metadata.push_back(metadata);
    }
  }
  std::sort(l1_metadata.begin(), l1_metadata.end(),
            [&](const LiveFileMetaData& a, const LiveFileMetaData& b) {
              return options.comparator->Compare(a.smallestkey, b.smallestkey) <
                     0;
            });
  ASSERT_EQ("a", l1_metadata[0].smallestkey);
  ASSERT_EQ("a", l1_metadata[0].largestkey);
  ASSERT_EQ("c" + Key(1), l1_metadata[1].smallestkey);
  ASSERT_EQ("d", l1_metadata[1].largestkey);

  TablePropertiesCollection all_table_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&all_table_props));
  int64_t num_range_deletions = 0;
  for (const auto& name_and_table_props : all_table_props) {
    const auto& name = name_and_table_props.first;
    const auto& table_props = name_and_table_props.second;
    // The range tombstone should only be output to the second L1 SST.
    if (name.size() >= l1_metadata[1].name.size() &&
        name.substr(name.size() - l1_metadata[1].name.size()).compare(l1_metadata[1].name) == 0) {
      ASSERT_EQ(1, table_props->num_range_deletions);
      ++num_range_deletions;
    } else {
      ASSERT_EQ(0, table_props->num_range_deletions);
    }
  }
  ASSERT_EQ(1, num_range_deletions);
}

TEST_F(DBRangeDelTest, OverlappedTombstones) {
  const int kNumPerFile = 4, kNumFiles = 2;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = 9 * 1024;
  options.max_compaction_bytes = 9 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 12K (4 values, each 3K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(2);
  ASSERT_EQ(2, NumTableFilesAtLevel(2));

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(1),
                             Key((kNumFiles)*kNumPerFile + 1)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));

  // The tombstone range is not broken up into multiple SSTs which may incur a
  // large compaction with L2.
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  std::vector<std::vector<FileMetaData>> files;
  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
}

TEST_F(DBRangeDelTest, OverlappedKeys) {
  const int kNumPerFile = 4, kNumFiles = 2;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = 9 * 1024;
  options.max_compaction_bytes = 9 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 12K (4 values, each 3K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(2);
  ASSERT_EQ(2, NumTableFilesAtLevel(2));

  for (int i = 1; i < kNumFiles * kNumPerFile + 1; i++) {
    ASSERT_OK(Put(Key(i), "0x123"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // The key range is broken up into three SSTs to avoid a future big compaction
  // with the grandparent
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(3, NumTableFilesAtLevel(1));

  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(
      3, NumTableFilesAtLevel(
             2));  // L1->L2 compaction size is limited to max_compaction_bytes
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
}

TEST_F(DBRangeDelTest, IteratorRefresh) {
  // Refreshing an iterator after a range tombstone is added should cause the
  // deleted range of keys to disappear.
  for (bool sv_changed : {false, true}) {
    ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
    ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));

    auto* iter = db_->NewIterator(ReadOptions());
    ASSERT_OK(iter->status());

    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               "key2", "key3"));

    if (sv_changed) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }

    ASSERT_OK(iter->Refresh());
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    ASSERT_EQ("key1", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());

    delete iter;
  }
}

void VerifyIteratorReachesEnd(InternalIterator* iter) {
  ASSERT_TRUE(!iter->Valid() && iter->status().ok());
}

void VerifyIteratorReachesEnd(Iterator* iter) {
  ASSERT_TRUE(!iter->Valid() && iter->status().ok());
}

TEST_F(DBRangeDelTest, IteratorReseek) {
  // Range tombstone triggers reseek in merging iterator.
  // Test set up:
  //    one memtable: range tombstone [0, 1)
  //    one immutable memtable: range tombstone [1, 2)
  //    one L0 file with range tombstone [2, 3)
  //    one L1 file with range tombstone [3, 4)
  // Seek(a) should trigger cascading reseeks at all levels
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  // L1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(4)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  // L0
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(3)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  // Immutable memtable
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(1),
                             Key(2)));
  ASSERT_OK(static_cast_with_check<DBImpl>(db_)->TEST_SwitchMemtable());
  std::string value;
  ASSERT_TRUE(dbfull()->GetProperty(db_->DefaultColumnFamily(),
                                    "rocksdb.num-immutable-mem-table", &value));
  ASSERT_EQ(1, std::stoi(value));
  // live memtable
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));
  // this memtable is still active
  ASSERT_TRUE(dbfull()->GetProperty(db_->DefaultColumnFamily(),
                                    "rocksdb.num-immutable-mem-table", &value));
  ASSERT_EQ(1, std::stoi(value));

  auto iter = db_->NewIterator(ReadOptions());
  get_perf_context()->Reset();
  iter->Seek(Key(0));
  // Reseeked immutable memtable, L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 3);
  VerifyIteratorReachesEnd(iter);
  get_perf_context()->Reset();
  iter->SeekForPrev(Key(1));
  // Reseeked L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);
  VerifyIteratorReachesEnd(iter);
  get_perf_context()->Reset();
  iter->SeekToFirst();
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 0);
  VerifyIteratorReachesEnd(iter);
  iter->SeekToLast();
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 0);
  VerifyIteratorReachesEnd(iter);
  delete iter;
}

TEST_F(DBRangeDelTest, ReseekDuringNextAndPrev) {
  // Range tombstone triggers reseek during Next() in merging iterator.
  // Test set up:
  //    memtable has: [0, 1) [2, 3)
  //    L0 has: 2
  //    L1 has: 1, 2, 3
  // Seek(0) will reseek 1 for all L0 and L1.
  // Then Next() will try to reseek 3 for L0 and L1.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  // L1
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L0
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // Memtable
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(3)));

  auto iter = db_->NewIterator(ReadOptions());
  auto iter_test_forward = [&] {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(1));

    get_perf_context()->Reset();
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(3));
    // Reseeked L0 and L1
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);

    // Next to Prev
    get_perf_context()->Reset();
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(1));
    // Reseeked L0 and L1
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);

    // Prev to Next
    get_perf_context()->Reset();
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(3));
    // Reseeked L0 and L1
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);

    iter->Next();
    VerifyIteratorReachesEnd(iter);
  };

  get_perf_context()->Reset();
  iter->Seek(Key(0));
  // Reseeked L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);
  iter_test_forward();
  iter->Seek(Key(1));
  // Reseeked L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);
  iter_test_forward();

  get_perf_context()->Reset();
  iter->SeekForPrev(Key(2));
  // Reseeked L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);
  iter_test_forward();
  iter->SeekForPrev(Key(1));
  // Reseeked L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);
  iter_test_forward();

  get_perf_context()->Reset();
  iter->SeekToFirst();
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 0);
  iter_test_forward();

  iter->SeekToLast();
  iter->Prev();
  iter_test_forward();
  delete iter;
}

TEST_F(DBRangeDelTest, TombstoneFromCurrentLevel) {
  // Range tombstone triggers reseek when covering key from the same level.
  // in merging iterator. Test set up:
  //    memtable has: [0, 1)
  //    L0 has: [2, 3), 2
  //    L1 has: 1, 2, 3
  // Seek(0) will reseek at 1 for L0 and L1.
  // Then Next() reseek at 3
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  // L1
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L0
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(3)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // Memtable
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));

  auto iter = db_->NewIterator(ReadOptions());
  get_perf_context()->Reset();
  iter->Seek(Key(0));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(1));
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);

  get_perf_context()->Reset();
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(3));
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);

  delete iter;
}

TEST_F(DBRangeDelTest, TombstoneAcrossFileBoundary) {
  // Verify that a range tombstone across file boundary covers keys from older
  // levels. Test set up:
  //    L1_0: 1, 3, [2, 6)   L1_1: 5, 7, [2, 6) (this is from compaction with
  //    L1_0) L2 has: 4
  // Seek(1) and Next() should move the L1 level iterator to
  // L1_1. Check if 4 is returned after Next().
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 2 * 1024;
  options.max_compaction_bytes = 2 * 1024;

  DestroyAndReopen(options);

  Random rnd(301);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(7), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), rnd.RandomString(1 << 10)));
  // Prevent keys being compacted away
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(6)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  get_perf_context()->Reset();
  iter->Seek(Key(1));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(1));
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(7));
  // 1 reseek into L2 when key 4 in L2 is covered by [2, 6) from L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);

  delete iter;
  db_->ReleaseSnapshot(snapshot);
  ;
}

TEST_F(DBRangeDelTest, NonOverlappingTombstonAtBoundary) {
  // Verify that a range tombstone across file boundary covers keys from older
  // levels. Test set up:
  //    L1_0: 1, 3, [4, 7)         L1_1: 6, 8, [4, 7)
  //    L2: 5
  // [4, 7) from L1_0 should cover 5 is sentinel works
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 2 * 1024;
  DestroyAndReopen(options);

  Random rnd(301);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(6), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(8), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), rnd.RandomString(4 << 10)));
  // Prevent keys being compacted away
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(7)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(3));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(3));
  get_perf_context()->Reset();
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(8));
  // 1 reseek into L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);
  for (auto& k : {4, 5, 6}) {
    get_perf_context()->Reset();
    iter->Seek(Key(k));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(8));
    // 1 reseek into L1
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);
  }
  delete iter;
}

TEST_F(DBRangeDelTest, OlderLevelHasNewerData) {
  // L1_0: 1, 3, [2, 7)   L1_1: 5, 6 at a newer sequence number than [2, 7)
  // Compact L1_1 to L2. Seek(3) should not skip 6.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;
  DestroyAndReopen(options);

  Random rnd(301);
  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), rnd.RandomString(4 << 10)));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(7)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(6), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  auto key = Key(6);
  Slice begin(key);
  EXPECT_OK(dbfull()->TEST_CompactRange(1, &begin, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(3));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(5));
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(6));
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, LevelBoundaryDefinedByTombstone) {
  // L1 has: 1, 2, [4, 5)
  // L2 has: 4
  // Seek(3), which is over all points keys in L1, check whether
  // sentinel key from L1 works in this case.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const Snapshot* snapshot = db_->GetSnapshot();
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(5)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(3));
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());

  get_perf_context()->Reset();
  iter->SeekForPrev(Key(5));
  // This is not reseeked in SeekForPrevImpl, but could be optimized to.
  //  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(2));
  db_->ReleaseSnapshot(snapshot);
  delete iter;
}

TEST_F(DBRangeDelTest, TombstoneOnlyFile) {
  // L1_0: 1, 2, L1_1: [3, 5)
  // L2: 3
  // Seek(2) then Next() which advance L1 iterator into L1_1.
  // If sentinel works with tombstone only file, it should cover the key in L2.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  Random rnd(301);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(5)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(2));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(2));
  iter->Next();
  VerifyIteratorReachesEnd(iter);
  iter->SeekForPrev(Key(4));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(2));
  iter->Next();
  VerifyIteratorReachesEnd(iter);
  delete iter;
}

void VerifyIteratorKey(InternalIterator* iter,
                       std::vector<std::string> expected_keys,
                       bool forward = true) {
  for (auto& key : expected_keys) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->user_key(), key);
    if (forward) {
      iter->Next();
    } else {
      iter->Prev();
    }
  }
}

TEST_F(DBRangeDelTest, TombstoneOnlyLevel) {
  // L1 [3, 5)
  // L2 has: 3, 4
  // Any kind of iterator seek should skip 4.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), "bar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(5)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  get_perf_context()->Reset();
  uint64_t expected_reseek = 0;
  for (auto i = 0; i < 7; ++i) {
    iter->Seek(Key(i));
    VerifyIteratorReachesEnd(iter);
    if (i < 5) {
      ++expected_reseek;
    }
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count,
              expected_reseek);
    iter->SeekForPrev(Key(i));
    VerifyIteratorReachesEnd(iter);
    if (i > 2) {
      ++expected_reseek;
    }
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count,
              expected_reseek);
    iter->SeekToFirst();
    VerifyIteratorReachesEnd(iter);
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count,
              ++expected_reseek);
    iter->SeekToLast();
    VerifyIteratorReachesEnd(iter);
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count,
              ++expected_reseek);
  }

  ColumnFamilyData* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd();
  SuperVersion* sv = cfd->GetSuperVersion();
  Arena arena;
  ReadOptions read_options;
  MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(), &arena,
                                          false /* prefix seek */);
  InternalIterator* level_iter = sv->current->TEST_GetLevelIterator(
      read_options, &merge_iter_builder, 1 /* level */,
      nullptr /* range_del_aggregator */, true);
  auto k = Key(3);
  IterKey target;
  target.SetInternalKey(k, kMaxSequenceNumber, kValueTypeForSeek);
  level_iter->Seek(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(5)});
  VerifyIteratorReachesEnd(level_iter);

  k = Key(5);
  target.SetInternalKey(k, 0, kValueTypeForSeekForPrev);
  level_iter->SeekForPrev(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(3)}, false);
  VerifyIteratorReachesEnd(iter);

  level_iter->SeekToFirst();
  VerifyIteratorKey(level_iter, {Key(5)});
  VerifyIteratorReachesEnd(iter);

  level_iter->SeekToLast();
  VerifyIteratorKey(level_iter, {Key(3)}, false);
  VerifyIteratorReachesEnd(iter);

  delete iter;
  delete level_iter;
}

TEST_F(DBRangeDelTest, TombstoneOnlyWithVisibleKey) {
  // L1: [3, 5)
  // L2: 2, 4, 5
  // 2 and 5 should be visible
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), "bar"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), "foobar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // l1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(5)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  auto iter_test_backward = [&] {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(5));
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(2));
    iter->Prev();
    VerifyIteratorReachesEnd(iter);
  };
  auto iter_test_forward = [&] {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(2));
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(5));
    iter->Next();
    VerifyIteratorReachesEnd(iter);
  };
  iter->Seek(Key(4));
  iter_test_backward();
  iter->SeekForPrev(Key(4));
  iter->Next();
  iter_test_backward();

  iter->Seek(Key(4));
  iter->Prev();
  iter_test_forward();
  iter->SeekForPrev(Key(4));
  iter_test_forward();

  iter->SeekToFirst();
  iter_test_forward();
  iter->SeekToLast();
  iter_test_backward();

  delete iter;
}

// Right sentinel tested in many test cases above
TEST_F(DBRangeDelTest, LeftSentinelKeyTest) {
  // L1_0: 0, 1    L1_1: [2, 3), 5
  // L2: 2
  // SeekForPrev(4) then Next() should give 1 due to sentinel key keeping
  // [2, 3) alive.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;
  options.max_compaction_bytes = 1024;

  DestroyAndReopen(options);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_0
  Random rnd(301);
  ASSERT_OK(db_->Put(WriteOptions(), Key(0), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), "bar"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(3)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  iter->SeekForPrev(Key(4));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(1));
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(0));
  iter->Prev();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());
  delete iter;
}

TEST_F(DBRangeDelTest, LeftSentinelKeyTestWithNewerKey) {
  // L1_0: 1, 2 and newer than L1_1,    L1_1: [2, 4), 5
  // L2: 3
  // SeekForPrev(4) then Next() should give 2 and then 1.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;
  options.max_compaction_bytes = 1024;

  DestroyAndReopen(options);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), "bar"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(4)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L1_0
  Random rnd(301);
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), rnd.RandomString(4 << 10)));
  auto seq = dbfull()->TEST_GetLastVisibleSequence();
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  Arena arena;
  InternalKeyComparator icmp(options.comparator);
  ReadOptions read_options;
  ScopedArenaIterator iter;
  iter.set(
      dbfull()->NewInternalIterator(read_options, &arena, kMaxSequenceNumber));

  auto k = Key(4);
  IterKey target;
  target.SetInternalKey(k, 0 /* sequence_number */, kValueTypeForSeekForPrev);
  iter->SeekForPrev(target.GetInternalKey());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->user_key(), Key(2));
  SequenceNumber actual_seq;
  ValueType type;
  UnPackSequenceAndType(ExtractInternalKeyFooter(iter->key()), &actual_seq,
                        &type);
  ASSERT_EQ(seq, actual_seq);
  // might as well check type
  ASSERT_EQ(type, kTypeValue);

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->user_key(), Key(1));
  iter->Prev();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(DBRangeDelTest, SentinelKeyCommonCaseTest) {
  // L1 has 3 files
  // L1_0: 1, 2     L1_1: [3, 4) 5, 6, [7, 8)     L1_2: 9
  // Check iterator operations on LevelIterator and DB Iter
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  Random rnd(301);
  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L1_1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(4)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(6), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(7),
                             Key(8)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  // L1_2
  ASSERT_OK(db_->Put(WriteOptions(), Key(9), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(3, NumTableFilesAtLevel(1));

  ColumnFamilyData* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd();
  SuperVersion* sv = cfd->GetSuperVersion();
  Arena arena;
  ReadOptions read_options;
  MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(), &arena,
                                          false /* prefix seek */);
  InternalIterator* level_iter = sv->current->TEST_GetLevelIterator(
      read_options, &merge_iter_builder, 1 /* level */,
      nullptr /* range_del_aggregator */, true);
  auto k = Key(7);
  IterKey target;
  target.SetInternalKey(k, kMaxSequenceNumber, kValueTypeForSeek);
  level_iter->Seek(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(8), Key(9)});
  ASSERT_TRUE(!level_iter->Valid() && level_iter->status().ok());

  k = Key(6);
  target.SetInternalKey(k, kMaxSequenceNumber, kValueTypeForSeek);
  level_iter->Seek(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(6), Key(8), Key(9)});

  k = Key(4);
  target.SetInternalKey(k, 0, kValueTypeForSeekForPrev);
  level_iter->SeekForPrev(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(3), Key(2), Key(1)}, false);

  k = Key(5);
  target.SetInternalKey(k, 0, kValueTypeForSeekForPrev);
  level_iter->SeekForPrev(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(5), Key(3), Key(2), Key(1)}, false);

  level_iter->SeekToFirst();
  VerifyIteratorKey(level_iter,
                    {Key(1), Key(2), Key(5), Key(6), Key(8), Key(9)});

  level_iter->SeekToLast();
  VerifyIteratorKey(level_iter,
                    {Key(9), Key(6), Key(5), Key(3), Key(2), Key(1)}, false);

  delete level_iter;
}

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
