//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  RID out_rid{};
  Tuple tuple_insert;
  if (plan_->IsRawInsert()) {
    for (const auto &raw_value : plan_->RawValues()) {
      tuple_insert = Tuple(raw_value, &table_info_->schema_);
      if (table_info_->table_->InsertTuple(tuple_insert, &out_rid, GetExecutorContext()->GetTransaction())) {
        for (IndexInfo *index : indexes_) {
          std::vector<Value> key_index_vals{};
          for (auto const &i : index->index_->GetKeyAttrs()) {
            key_index_vals.emplace_back(tuple_insert.GetValue(&table_info_->schema_, i));
          }
          index->index_->InsertEntry(Tuple(key_index_vals, index->index_->GetKeySchema()), out_rid,
                                     GetExecutorContext()->GetTransaction());
        }
      }
    }
  } else {
    while (child_executor_->Next(&tuple_insert, &out_rid)) {
      if (table_info_->table_->InsertTuple(tuple_insert, &out_rid, GetExecutorContext()->GetTransaction())) {
        for (IndexInfo *index : indexes_) {
          std::vector<Value> key_index_vals{};
          for (auto const &i : index->index_->GetKeyAttrs()) {
            key_index_vals.emplace_back(tuple_insert.GetValue(child_executor_->GetOutputSchema(), i));
          }
          index->index_->InsertEntry(Tuple(key_index_vals, index->index_->GetKeySchema()), out_rid,
                                     GetExecutorContext()->GetTransaction());
        }
      }
    }
  }
  return false;
}

}  // namespace bustub
