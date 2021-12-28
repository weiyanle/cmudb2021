//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple;
  RID old_rid;
  // To avoid pushing tuple to result set, update all tuples in one Next()
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    Tuple updated_tuple = GenerateUpdatedTuple(old_tuple);
    Transaction *txn = GetExecutorContext()->GetTransaction();
    if (!txn->IsExclusiveLocked(old_rid)) {
      if (txn->IsSharedLocked(old_rid)) {
        GetExecutorContext()->GetLockManager()->LockUpgrade(txn, old_rid);
      } else {
        GetExecutorContext()->GetLockManager()->LockExclusive(txn, old_rid);
      }
    }
    table_info_->table_->UpdateTuple(updated_tuple, old_rid, GetExecutorContext()->GetTransaction());
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      GetExecutorContext()->GetLockManager()->Unlock(txn, old_rid);
    }
    for (IndexInfo *index : indexes_) {
      std::vector<Value> old_key_index_vals{};
      for (auto const &i : index->index_->GetKeyAttrs()) {
        old_key_index_vals.emplace_back(old_tuple.GetValue(child_executor_->GetOutputSchema(), i));
      }
      index->index_->DeleteEntry(Tuple(old_key_index_vals, index->index_->GetKeySchema()), old_rid,
                                 GetExecutorContext()->GetTransaction());
      std::vector<Value> updated_key_index_vals{};
      for (auto const &i : index->index_->GetKeyAttrs()) {
        updated_key_index_vals.emplace_back(updated_tuple.GetValue(child_executor_->GetOutputSchema(), i));
      }
      index->index_->InsertEntry(Tuple(updated_key_index_vals, index->index_->GetKeySchema()), old_rid,
                                 GetExecutorContext()->GetTransaction());
      // ATTENTION! Can't pass memory test if add following codes.
      GetExecutorContext()->GetTransaction()->GetIndexWriteSet()->emplace_back(
          old_rid, table_info_->oid_, WType::UPDATE, Tuple(updated_key_index_vals, index->index_->GetKeySchema()),
          index->index_oid_, GetExecutorContext()->GetCatalog());
      GetExecutorContext()->GetTransaction()->GetIndexWriteSet()->back().old_tuple_ =
          Tuple(old_key_index_vals, index->index_->GetKeySchema());

    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
