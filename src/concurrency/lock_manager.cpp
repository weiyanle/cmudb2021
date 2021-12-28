//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  LockPreCheck(txn, rid, 0);
  LockRequestQueue *lrq = &lock_table_[rid];
  lrq->latch_.lock();
  lrq->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  txn->GetSharedLockSet()->emplace(rid);
  KillYoungerRequests(lrq, txn, LockMode::EXCLUSIVE);
  lrq->latch_.unlock();
  std::unique_lock<std::mutex> lck(latch_);
  while (OlderExists(lrq, txn->GetTransactionId(), LockMode::EXCLUSIVE) &&
         txn->GetState() != TransactionState::ABORTED) {
    lrq->cv_.wait(lck);
  }
  auto lr_iter = GetRequestInQueue(lrq, txn->GetTransactionId());
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lrq->latch_.lock();
  lr_iter->granted_ = true;
  lrq->latch_.unlock();
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  LockPreCheck(txn, rid, 1);
  LockRequestQueue *lrq = &lock_table_[rid];
  lrq->latch_.lock();
  lrq->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  KillYoungerRequests(lrq, txn, LockMode::SHARED);
  lrq->latch_.unlock();
  std::unique_lock<std::mutex> lck(latch_);
  while (OlderExists(lrq, txn->GetTransactionId(), LockMode::SHARED) && txn->GetState() != TransactionState::ABORTED) {
    lrq->cv_.wait(lck);
  }
  auto lr_iter = GetRequestInQueue(lrq, txn->GetTransactionId());
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lrq->latch_.lock();
  lr_iter->granted_ = true;
  lrq->latch_.unlock();
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  LockPreCheck(txn, rid, 2);
  LockRequestQueue *lrq = &lock_table_[rid];
  if (lrq->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  auto lr_iter = GetRequestInQueue(lrq, txn->GetTransactionId());
  if (lr_iter == lrq->request_queue_.end() || lr_iter->lock_mode_ == LockMode::EXCLUSIVE || !lr_iter->granted_) {
    return false;
  }
  lrq->latch_.lock();
  lrq->upgrading_ = txn->GetTransactionId();
  lr_iter->lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  KillYoungerRequests(lrq, txn, LockMode::SHARED);
  lrq->latch_.unlock();
  std::unique_lock<std::mutex> lck(latch_);
  while (OlderExists(lrq, txn->GetTransactionId(), LockMode::SHARED) && txn->GetState() != TransactionState::ABORTED) {
    lrq->cv_.wait(lck);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lrq->upgrading_ = INVALID_TXN_ID;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetExclusiveLockSet()->erase(rid);
  txn->GetSharedLockSet()->erase(rid);
  LockRequestQueue *lrq = &lock_table_[rid];
  lrq->latch_.lock();
  auto lr_iter = GetRequestInQueue(lrq, txn->GetTransactionId());
  if (lr_iter == lrq->request_queue_.end()) {
    lrq->latch_.unlock();
    return false;
  }
  lrq->request_queue_.erase(lr_iter);
  lrq->latch_.unlock();
  lrq->cv_.notify_all();
  return true;
}

bool LockManager::LockPreCheck(Transaction *txn, const RID &rid, int lock_mode) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (lock_mode == 0 && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetState() == TransactionState::ABORTED || (lock_mode == 2 && lock_table_.find(rid) == lock_table_.end())) {
    return false;
  }
  if (lock_mode == 0 || lock_mode == 1) {
    latch_.lock();  // insert into map is multi-thread unsafe
    if (lock_table_.find(rid) == lock_table_.end()) {
      lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    }
    latch_.unlock();
  }
  return true;
}
std::list<LockManager::LockRequest>::iterator LockManager::GetRequestInQueue(LockManager::LockRequestQueue *lrq,
                                                                             const txn_id_t &id) {
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); ++iter) {
    if (iter->txn_id_ == id) {
      return iter;
    }
  }
  return lrq->request_queue_.end();
}

bool LockManager::OlderExists(LockManager::LockRequestQueue *lrq, const txn_id_t &id, LockMode lower_bound) {
  return std::any_of(lrq->request_queue_.begin(), lrq->request_queue_.end(), [&](const LockRequest &r) {
    return r.txn_id_ < id && (lower_bound == LockMode::SHARED || r.lock_mode_ == LockMode::EXCLUSIVE);
  });
}

void LockManager::KillYoungerRequests(LockManager::LockRequestQueue *lrq, Transaction *txn, LockMode lower_bound) {
  for (auto &r : lrq->request_queue_) {
    if ((lower_bound == LockMode::SHARED || r.lock_mode_ == LockMode::EXCLUSIVE) &&
        r.txn_id_ > txn->GetTransactionId() &&
        TransactionManager::GetTransaction(r.txn_id_)->GetState() != TransactionState::ABORTED) {
      TransactionManager::GetTransaction(r.txn_id_)->SetState(TransactionState::ABORTED);
      r.granted_ = false;
      lrq->cv_.notify_all();
    }
  }
}

}  // namespace bustub
