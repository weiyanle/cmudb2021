//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = table_info_->table_->Begin(GetExecutorContext()->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != table_info_->table_->End() && plan_->GetPredicate() != nullptr &&
         !plan_->GetPredicate()->Evaluate(&*iter_, &table_info_->schema_).GetAs<bool>()) {
    ++iter_;
  }
  if (iter_ != table_info_->table_->End()) {
    std::vector<Value> values;
    for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); ++i) {
      values.emplace_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(&*iter_, &table_info_->schema_));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    *rid = iter_->GetRid();
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
