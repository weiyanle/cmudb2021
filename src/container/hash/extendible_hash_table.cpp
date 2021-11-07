//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager->NewPage(&directory_page_id_, nullptr)->GetData());
  directory_page->SetPageId(directory_page_id_);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  buffer_pool_manager->NewPage(&bucket_page_id, nullptr);
  directory_page->SetBucketPageId(0, bucket_page_id);
  buffer_pool_manager->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager->UnpinPage(bucket_page_id, true, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(directory_page_id_, nullptr)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(
      buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchPageByKey(KeyType key) {
  auto ans = FetchBucketPage(KeyToPageId(key, FetchDirectoryPage()));
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);  // directory page doesn't need anymore
  return ans;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto bpg = FetchPageByKey(key);
  bool ans = bpg->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(KeyToPageId(key, FetchDirectoryPage()), false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto bpg = FetchPageByKey(key);
  bool ans = bpg->Insert(key, value, comparator_);
  if (!ans && bpg->IsFull()) {
    SplitInsert(transaction, key, value);
  }
  buffer_pool_manager_->UnpinPage(KeyToPageId(key, FetchDirectoryPage()), true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.WUnlock();
  return ans;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto bpg = FetchPageByKey(key);
  auto dpg = FetchDirectoryPage();
  auto kti = KeyToDirectoryIndex(key, dpg);
  if (dpg->GetGlobalDepth() == dpg->GetLocalDepth(kti)) {
    if (1 << dpg->GetGlobalDepth() != DIRECTORY_ARRAY_SIZE) {
      dpg->IncrGlobalDepth();
    } else {
      return false;  // DIRECTORY is full
    }
  }
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&bucket_page_id, nullptr)->GetData());
  size_t image_index = dpg->GetSplitImageIndex(kti);
  FetchDirectoryPage()->SetBucketPageId(image_index, bucket_page_id);

  // add pointers to image page
  size_t common_bits = kti % (1 << dpg->GetLocalDepth(kti));
  size_t ld = dpg->GetLocalDepth(kti);
  for (size_t i = common_bits; i < DIRECTORY_ARRAY_SIZE; i += (1 << ld)) {
    if (((i >> ld) & 1) != ((kti >> ld) & 1)) {
      dpg->SetBucketPageId(i, bucket_page_id);
    }
  }

  dpg->IncrLocalDepth(kti);
  dpg->IncrLocalDepth(image_index);
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (bpg->IsOccupied(i)) {
      if (KeyToDirectoryIndex(bpg->KeyAt(i), dpg) == image_index) {
        bucket_page->SetPair(bpg->KeyAt(i), bpg->ValueAt(i), i);
        bpg->DeleteAt(i);
      }
    }
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(KeyToPageId(key, dpg), true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  bool ans = Insert(transaction, key, value);
  return ans;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto bpg = FetchPageByKey(key);
  bool ans = bpg->Remove(key, value, comparator_);
  if (ans && bpg->IsEmpty()) {
    Merge(transaction, key, value);
  }
  buffer_pool_manager_->UnpinPage(KeyToPageId(key, FetchDirectoryPage()), true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.WUnlock();
  return ans;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dpg = FetchDirectoryPage();
  auto kti = KeyToDirectoryIndex(key, dpg);
  size_t image_index = dpg->GetSplitImageIndex(kti);
  if (dpg->GetLocalDepth(kti) == 0 || dpg->GetLocalDepth(kti) != dpg->GetLocalDepth(image_index)) {
    return;
  }

  dpg->DecrLocalDepth(image_index);

  // transfer pointers to image page
  size_t common_bits = image_index % (1 << dpg->GetLocalDepth(image_index));
  size_t ld = dpg->GetLocalDepth(image_index);
  for (size_t i = common_bits; i < DIRECTORY_ARRAY_SIZE; i += (1 << ld)) {
    if (((i >> ld) & 1) == ((kti >> ld) & 1)) {
      dpg->SetBucketPageId(i, dpg->GetBucketPageId(image_index));
    }
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->DeletePage(KeyToPageId(key, dpg), nullptr);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
