//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple;
  RID old_rid;
  // To avoid pushing tuple to result set, delete all tuples in one Next()
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    table_info_->table_->MarkDelete(old_rid, GetExecutorContext()->GetTransaction());
    for (IndexInfo *index : indexes_) {
      std::vector<Value> key_index_vals{};
      for (auto const &i : index->index_->GetKeyAttrs()) {
        key_index_vals.emplace_back(old_tuple.GetValue(child_executor_->GetOutputSchema(), i));
      }
      index->index_->DeleteEntry(Tuple(key_index_vals, index->index_->GetKeySchema()), old_rid,
                                 GetExecutorContext()->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
